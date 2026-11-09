//! SQL sessions on the SQLite adapter: SQLite's own transaction semantics on the primary's
//! writer (ADR-0037, point 12).
//!
//! Each statement is prepared under the user confinement, and classified by SQLite as it does:
//! [`Statement::readonly`] says whether it may write, and the authorizer's transaction and
//! savepoint actions say whether it controls a transaction. Then:
//!
//! - outside a transaction, a read-only statement runs on the read connection, so it never waits
//!   for the writer, and a statement that writes runs in a transaction of its own, `BEGIN
//!   IMMEDIATE … COMMIT`, exactly as an `Execute` of one statement does;
//! - `BEGIN` opens a transaction on the writer that later calls continue, `COMMIT` or `END`
//!   commits it, and `ROLLBACK` rolls it back; a `SAVEPOINT` outside a transaction is refused;
//! - inside a transaction, every statement runs on the writer.
//!
//! A transaction's first write records the staged sink marks and starts the change recorder's
//! transaction, as an `Execute` does before its statements, and its commit records the
//! recorder's commit row first. A statement that changes the reserved namespace, or defines a
//! trigger or view that names it, rolls the whole transaction back.
//!
//! A read-only session runs everything on the read connection, and refuses, without running it,
//! any statement that isn't read-only or controls a transaction.

use std::ffi::c_int;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use rusqlite::fallible_iterator::FallibleIterator as _;
use rusqlite::types::ValueRef;
use rusqlite::{Batch, Connection, Statement, ffi};

use super::changes::Recorder;
use super::marks::Marks;
use super::reserved::Reserved;
use super::{
    Confinement, Interruptible, Param, Refusal, SqliteDatabase, UserStatement, classify,
    encode_position, interrupted, read_wal_file, refuse_programs_on_reserved_names, value_from_sql,
};
use crate::engine::{
    Autocommit, BatchStep, CANCELLED, CallOutcome, CallResult, DEADLINE_EXCEEDED, Description,
    EngineError, Interrupt, Interruption, MAX_RESULT_BYTES, PERMISSION_DENIED, RESULT_TOO_LARGE,
    ResultColumn, SAVEPOINT_OUTSIDE_TRANSACTION, SessionAccess, SessionCall, SessionStatement,
    SqlSessions, StatementError, StatementOutcome, StepOutcome, UNSENDABLE_VALUE, Value, WantRows,
    WriteIntent, holds,
};

/// What a statement does to a transaction, as the authorizer reports it while SQLite prepares
/// the statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Control {
    /// `BEGIN`.
    Begin,
    /// `COMMIT` or `END`.
    Commit,
    /// `ROLLBACK`.
    Rollback,
    /// `SAVEPOINT`.
    Savepoint,
    /// `RELEASE`.
    Release,
    /// `ROLLBACK TO`.
    RollbackTo,
}

/// A prepared statement's classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Class {
    readonly: bool,
    control: Option<Control>,
    /// Whether it defines a trigger or a view, whose body is checked after it runs.
    defines_program: bool,
}

impl Class {
    /// Whether the statement may write: not read-only, and not a transaction's control.
    fn writes(self) -> bool {
        !self.readonly && self.control.is_none()
    }

    /// Whether a read-only session may run it.
    fn reads_only(self) -> bool {
        self.readonly && self.control.is_none()
    }
}

/// A transaction a session opened on the writer.
#[derive(Debug, Default)]
pub(super) struct OpenTransaction {
    /// Whether it has written: the staged marks were recorded, the recorder's transaction
    /// started, and the reserved schema remembered.
    wrote: bool,
}

/// What is left of the most rows one call's results may hold.
#[derive(Debug)]
struct Budget(usize);

impl Budget {
    /// Takes `value`'s size from the budget; fails once it is spent.
    fn take(&mut self, value: &Value) -> Result<(), StatementError> {
        let size = match value {
            Value::Null => 1,
            Value::Integer(_) | Value::Real(_) => 8,
            Value::Text(text) => text.len(),
            Value::Blob(blob) => blob.len(),
        };
        self.0 = self.0.checked_sub(size).ok_or_else(|| {
            StatementError::new(
                RESULT_TOO_LARGE,
                format!(
                    "the call's rows exceed {MAX_RESULT_BYTES} bytes; read them in smaller pieces"
                ),
            )
        })?;
        Ok(())
    }
}

/// A statement's outcome, or the failure of storage that ends the call.
type Ran<T> = Result<Result<T, StatementError>, EngineError>;

impl SqlSessions for SqliteDatabase {
    fn intent(&mut self, call: &SessionCall) -> Result<WriteIntent, EngineError> {
        let texts: Vec<&str> = match call {
            SessionCall::Execute(statement) => vec![statement.sql.as_str()],
            SessionCall::Batch(steps) => steps
                .iter()
                .map(|step| step.statement.sql.as_str())
                .collect(),
            SessionCall::Sequence(sql) => return sequence_intent(&self.writer, &self.writing, sql),
            SessionCall::Describe(_) | SessionCall::GetAutocommit => {
                return Ok(WriteIntent::ReadOnly);
            }
        };
        for sql in texts {
            match prepare_class(&self.writer, &self.writing, sql) {
                Ok((_, class)) if class.reads_only() => {}
                Ok(_) => return Ok(WriteIntent::MayWrite),
                Err(Err(storage)) => return Err(storage),
                // A statement that doesn't prepare now may once an earlier one has run.
                Err(Ok(_)) => return Ok(WriteIntent::MayWrite),
            }
        }
        Ok(WriteIntent::ReadOnly)
    }

    fn run(
        &mut self,
        call: &SessionCall,
        access: SessionAccess,
        interrupt: &Interrupt,
    ) -> Result<CallResult, EngineError> {
        let mut budget = Budget(MAX_RESULT_BYTES);
        let outcome = match self.run_call(call, access, interrupt, &mut budget) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.split().1.roll_back();
                return Err(error);
            }
        };
        Ok(CallResult {
            outcome,
            autocommit: self.access_autocommit(access),
        })
    }

    fn autocommit(&self) -> Autocommit {
        if self.writer.is_autocommit() {
            Autocommit::On
        } else {
            Autocommit::InTransaction
        }
    }

    fn roll_back(&mut self) -> Result<(), EngineError> {
        self.split().1.roll_back();
        if self.writer.is_autocommit() {
            Ok(())
        } else {
            Err(EngineError::Storage(
                "the session's transaction could not be rolled back".into(),
            ))
        }
    }
}

impl SqliteDatabase {
    /// The session state a call with `access` sees: a read-only session is never in a
    /// transaction, whatever another session holds on the writer.
    fn access_autocommit(&self, access: SessionAccess) -> Autocommit {
        match access {
            SessionAccess::ReadOnly => Autocommit::On,
            SessionAccess::ReadWrite => self.autocommit(),
        }
    }

    fn run_call(
        &mut self,
        call: &SessionCall,
        access: SessionAccess,
        interrupt: &Interrupt,
        budget: &mut Budget,
    ) -> Result<CallOutcome, EngineError> {
        Ok(match call {
            SessionCall::Execute(statement) => {
                CallOutcome::Statement(self.run_statement(statement, access, interrupt, budget)?)
            }
            SessionCall::Batch(steps) => {
                CallOutcome::Batch(self.run_batch(steps, access, interrupt, budget)?)
            }
            SessionCall::Sequence(sql) => {
                CallOutcome::Sequence(self.run_sequence(sql, access, interrupt)?)
            }
            SessionCall::Describe(sql) => CallOutcome::Described(self.describe(sql, access)?),
            SessionCall::GetAutocommit => CallOutcome::Autocommit,
        })
    }

    fn run_batch(
        &mut self,
        steps: &[BatchStep],
        access: SessionAccess,
        interrupt: &Interrupt,
        budget: &mut Budget,
    ) -> Result<Vec<StepOutcome>, EngineError> {
        let mut outcomes = Vec::with_capacity(steps.len());
        for step in steps {
            let autocommit = self.access_autocommit(access);
            let runs = step
                .condition
                .as_ref()
                .is_none_or(|condition| holds(condition, &outcomes, autocommit));
            let outcome = if runs {
                match self.run_statement(&step.statement, access, interrupt, budget)? {
                    Ok(outcome) => StepOutcome::Ok(outcome),
                    Err(error) => StepOutcome::Error(error),
                }
            } else {
                StepOutcome::Skipped
            };
            outcomes.push(outcome);
        }
        Ok(outcomes)
    }

    fn run_statement(
        &mut self,
        statement: &SessionStatement,
        access: SessionAccess,
        interrupt: &Interrupt,
        budget: &mut Budget,
    ) -> Ran<StatementOutcome> {
        if let Some(error) = stopped(interrupt) {
            return Ok(Err(error));
        }
        let (reader, mut writer) = self.split();
        if access == SessionAccess::ReadOnly {
            return read(&reader, statement, Gate::ReadOnly, interrupt, budget);
        }
        let (conn, writing) = (writer.conn, writer.confinement);
        let (mut prepared, class) = match prepare_class(conn, writing, &statement.sql) {
            Ok(prepared) => prepared,
            Err(failed) => return failed.map(Err),
        };
        let in_transaction = !conn.is_autocommit();
        match class.control {
            None if class.readonly && !in_transaction => {
                drop(prepared);
                read(&reader, statement, Gate::Open, interrupt, budget)
            }
            Some(Control::Savepoint) if !in_transaction => Ok(Err(savepoint_refusal())),
            _ => writer.run_prepared(&mut prepared, class, Some(statement), interrupt, budget),
        }
    }

    fn run_sequence(&mut self, sql: &str, access: SessionAccess, interrupt: &Interrupt) -> Ran<()> {
        // A sequence's rows are dropped, so they take nothing from a budget.
        let mut budget = Budget(usize::MAX);
        let (reader, mut writer) = self.split();
        let (conn, confinement) = match access {
            SessionAccess::ReadOnly => (reader.conn, reader.confinement),
            SessionAccess::ReadWrite => (writer.conn, writer.confinement),
        };
        let mut batch = Batch::new(conn, sql);
        loop {
            if let Some(error) = stopped(interrupt) {
                return Ok(Err(error));
            }
            let next = {
                let _user = UserStatement::enter(confinement);
                batch.next()
            };
            let mut prepared = match next {
                Ok(Some(prepared)) => prepared,
                Ok(None) => return Ok(Ok(())),
                Err(error) => return statement_error(error, confinement, interrupt).map(Err),
            };
            let class = confinement.class_of(&prepared);
            let ran = match access {
                SessionAccess::ReadOnly if !class.reads_only() => Ok(Err(read_only_refusal())),
                SessionAccess::ReadOnly => step(
                    conn,
                    confinement,
                    &mut prepared,
                    &SessionStatement::default(),
                    interrupt,
                    &mut budget,
                ),
                SessionAccess::ReadWrite
                    if class.control == Some(Control::Savepoint) && conn.is_autocommit() =>
                {
                    Ok(Err(savepoint_refusal()))
                }
                SessionAccess::ReadWrite => {
                    writer.run_prepared(&mut prepared, class, None, interrupt, &mut budget)
                }
            };
            if let Err(error) = ran? {
                return Ok(Err(error));
            }
        }
    }

    fn describe(&mut self, sql: &str, access: SessionAccess) -> Ran<Description> {
        let (reader, writer) = self.split();
        let (conn, confinement) = match access {
            SessionAccess::ReadOnly => (reader.conn, reader.confinement),
            SessionAccess::ReadWrite => (writer.conn, writer.confinement),
        };
        let _user = UserStatement::enter(confinement);
        let prepared = match conn.prepare(sql) {
            Ok(prepared) => prepared,
            Err(error) => return statement_error(error, confinement, &Interrupt::new()).map(Err),
        };
        let params = (1..=prepared.parameter_count())
            .map(|index| prepared.parameter_name(index).map(str::to_owned))
            .collect();
        Ok(Ok(Description {
            params,
            columns: columns(&prepared),
            is_explain: prepared.is_explain() > 0,
            is_readonly: prepared.readonly(),
        }))
    }

    /// The reader, and the writer with everything a session's writes change, borrowed apart.
    fn split(&mut self) -> (ReaderSide<'_>, WriterSide<'_>) {
        let wal = self.wal_path();
        let reader = ReaderSide {
            conn: &self.reader,
            confinement: &self.reading,
        };
        let writer = WriterSide {
            path: &self.path,
            wal,
            conn: &self.writer,
            confinement: &self.writing,
            recorder: &mut self.recorder,
            marks: &mut self.marks,
            reserved: &mut self.reserved,
            end: &mut self.end,
            checkpointed: self.checkpointed,
            transaction: &mut self.transaction,
        };
        (reader, writer)
    }
}

impl Confinement {
    /// Takes what the statement just prepared does to a transaction, if anything.
    pub(super) fn take_control(&self) -> Option<Control> {
        self.control.lock().expect("control lock poisoned").take()
    }

    /// The classification of `prepared`, which was just prepared under this confinement.
    fn class_of(&self, prepared: &Statement<'_>) -> Class {
        Class {
            readonly: prepared.readonly(),
            control: self.take_control(),
            defines_program: self.defines_program.swap(false, Ordering::Relaxed),
        }
    }
}

/// Prepares `sql` on `conn` under `confinement`, and classifies it. A failure is a statement
/// error (`Ok`) or a storage failure (`Err`).
fn prepare_class<'c>(
    conn: &'c Connection,
    confinement: &Confinement,
    sql: &str,
) -> Result<(Statement<'c>, Class), Result<StatementError, EngineError>> {
    let _user = UserStatement::enter(confinement);
    match conn.prepare(sql) {
        Ok(prepared) => {
            let class = confinement.class_of(&prepared);
            Ok((prepared, class))
        }
        Err(error) => Err(statement_error(error, confinement, &Interrupt::new())),
    }
}

/// Whether a sequence may write: whether every statement in it prepares now, is read-only, and
/// controls no transaction.
fn sequence_intent(
    conn: &Connection,
    confinement: &Confinement,
    sql: &str,
) -> Result<WriteIntent, EngineError> {
    let _user = UserStatement::enter(confinement);
    let mut batch = Batch::new(conn, sql);
    loop {
        match batch.next() {
            Ok(Some(prepared)) => {
                if !confinement.class_of(&prepared).reads_only() {
                    return Ok(WriteIntent::MayWrite);
                }
            }
            Ok(None) => return Ok(WriteIntent::ReadOnly),
            Err(error) => {
                return match statement_error(error, confinement, &Interrupt::new()) {
                    Ok(_) => Ok(WriteIntent::MayWrite),
                    Err(storage) => Err(storage),
                };
            }
        }
    }
}

/// Whether a statement on the read connection must be read-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    /// A read-only session: a statement that isn't read-only, or controls a transaction, is
    /// refused without running.
    ReadOnly,
    /// The statement is known to be read-only already.
    Open,
}

/// Runs `statement` on the read connection, against the latest commit.
fn read(
    reader: &ReaderSide<'_>,
    statement: &SessionStatement,
    gate: Gate,
    interrupt: &Interrupt,
    budget: &mut Budget,
) -> Ran<StatementOutcome> {
    let (conn, confinement) = (reader.conn, reader.confinement);
    let (mut prepared, class) = match prepare_class(conn, confinement, &statement.sql) {
        Ok(prepared) => prepared,
        Err(failed) => return failed.map(Err),
    };
    if gate == Gate::ReadOnly && !class.reads_only() {
        return Ok(Err(read_only_refusal()));
    }
    step(
        conn,
        confinement,
        &mut prepared,
        statement,
        interrupt,
        budget,
    )
}

/// The error for a `SAVEPOINT` outside a transaction.
fn savepoint_refusal() -> StatementError {
    StatementError::new(
        SAVEPOINT_OUTSIDE_TRANSACTION,
        "start a transaction with BEGIN before SAVEPOINT",
    )
}

/// The error a read-only session gets for a statement that needs `write`.
fn read_only_refusal() -> StatementError {
    StatementError::new(
        PERMISSION_DENIED,
        "the session can only read: this statement needs write on the database",
    )
}

/// The error for a call whose interrupt is raised, if it is.
fn stopped(interrupt: &Interrupt) -> Option<StatementError> {
    interrupt.raised().map(interruption_error)
}

fn interruption_error(why: Interruption) -> StatementError {
    let code = match why {
        Interruption::Deadline => DEADLINE_EXCEEDED,
        _ => CANCELLED,
    };
    StatementError::new(code, EngineError::Interrupted(why).to_string())
}

/// Binds `statement`'s arguments to `prepared`, then steps it to the end under `confinement`
/// until `interrupt` stops it, keeping its rows if it asked for them.
fn step(
    conn: &Connection,
    confinement: &Confinement,
    prepared: &mut Statement<'_>,
    statement: &SessionStatement,
    interrupt: &Interrupt,
    budget: &mut Budget,
) -> Ran<StatementOutcome> {
    if let Err(error) = bind(prepared, statement) {
        return Ok(Err(error));
    }
    let columns = columns(prepared);
    let width = columns.len();
    let total_before = conn.total_changes();
    let mut rows = Vec::new();
    {
        // Confined while it steps too: SQLite prepares statements of its own while stepping some,
        // and reprepares after a schema change.
        let _user = UserStatement::enter(confinement);
        let _interruptible = Interruptible::watch(confinement, interrupt);
        let mut stepping = prepared.raw_query();
        loop {
            let row = match stepping.next() {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(error) => return statement_error(error, confinement, interrupt).map(Err),
            };
            if statement.want_rows == WantRows::NoRows {
                continue;
            }
            let mut values = Vec::with_capacity(width);
            for index in 0..width {
                let value = match row.get_ref(index) {
                    Ok(value) => value,
                    Err(error) => return statement_error(error, confinement, interrupt).map(Err),
                };
                let value = match sendable(value) {
                    Ok(value) => value,
                    Err(error) => return Ok(Err(error)),
                };
                if let Err(error) = budget.take(&value) {
                    return Ok(Err(error));
                }
                values.push(value);
            }
            rows.push(values);
        }
    }
    // `changes` counts the statement's own rows only, not a trigger's, and keeps the last count
    // until a later statement changes rows: a statement that changed nothing counts none.
    let changed = conn.total_changes() != total_before;
    let affected_row_count = if changed { conn.changes() } else { 0 };
    let last_insert_rowid = changed.then(|| conn.last_insert_rowid());
    Ok(Ok(StatementOutcome {
        columns,
        rows,
        affected_row_count,
        last_insert_rowid,
    }))
}

/// A value read from a row, unless it can't be sent.
fn sendable(value: ValueRef<'_>) -> Result<Value, StatementError> {
    match value {
        ValueRef::Real(real) if !real.is_finite() => Err(StatementError::new(
            UNSENDABLE_VALUE,
            "a REAL value isn't finite and can't be sent",
        )),
        value => Ok(value_from_sql(value)),
    }
}

/// The result columns of `prepared`.
fn columns(prepared: &Statement<'_>) -> Vec<ResultColumn> {
    prepared
        .columns()
        .map(|column| ResultColumn {
            name: Some(column.name().to_owned()),
            decltype: column.decl_type().map(str::to_owned),
        })
        .collect()
}

/// Binds `statement`'s positional and named arguments to `prepared`.
fn bind(prepared: &mut Statement<'_>, statement: &SessionStatement) -> Result<(), StatementError> {
    let count = prepared.parameter_count();
    if statement.args.len() > count {
        return Err(StatementError::new(
            "SQLITE_RANGE",
            format!(
                "the statement takes {count} arguments, and {} were given",
                statement.args.len()
            ),
        ));
    }
    for (index, value) in statement.args.iter().enumerate() {
        prepared
            .raw_bind_parameter(index + 1, Param(value))
            .map_err(|error| bind_error(&error))?;
    }
    for (name, value) in &statement.named_args {
        let Some(index) = parameter_index(prepared, name) else {
            return Err(StatementError::new(
                "SQLITE_RANGE",
                format!("the statement has no parameter named {name}"),
            ));
        };
        prepared
            .raw_bind_parameter(index, Param(value))
            .map_err(|error| bind_error(&error))?;
    }
    Ok(())
}

/// The index of the parameter `name` names: with its prefix, or, without one, with the first of
/// `:`, `@`, and `$` that matches (Hrana's rule).
fn parameter_index(prepared: &Statement<'_>, name: &str) -> Option<usize> {
    let lookup = |name: &str| prepared.parameter_index(name).ok().flatten();
    if name.starts_with([':', '@', '$', '?']) {
        return lookup(name);
    }
    [':', '@', '$']
        .into_iter()
        .find_map(|prefix| lookup(&format!("{prefix}{name}")))
}

fn bind_error(error: &rusqlite::Error) -> StatementError {
    StatementError::new(code_name(error), error.to_string())
}

/// Sorts `error`, from preparing or running a user statement under `confinement`, into a
/// statement error with SQLite's code, or a storage failure.
fn statement_error(
    error: rusqlite::Error,
    confinement: &Confinement,
    interrupt: &Interrupt,
) -> Result<StatementError, EngineError> {
    if let Some(EngineError::Interrupted(why)) = interrupted(&error, interrupt) {
        return Ok(interruption_error(why));
    }
    let code = code_name(&error);
    match confinement.classify(error) {
        EngineError::Statement { message } => Ok(StatementError::new(code, message)),
        other => Err(other),
    }
}

/// An error from one of the adapter's own statements around a session's write, as a statement
/// error or a storage failure.
fn own_error(error: rusqlite::Error) -> Result<StatementError, EngineError> {
    let code = code_name(&error);
    match classify(error) {
        EngineError::Statement { message } => Ok(StatementError::new(code, message)),
        other => Err(other),
    }
}

/// SQLite's name for `error`'s extended result code, or its primary code's name.
fn code_name(error: &rusqlite::Error) -> &'static str {
    let Some(sqlite) = error.sqlite_error() else {
        return match error {
            rusqlite::Error::MultipleStatement => "SQLITE_ERROR",
            _ => "SQLITE_MISUSE",
        };
    };
    extended_name(sqlite.extended_code)
        .or_else(|| extended_name(sqlite.extended_code & 0xff))
        .unwrap_or("SQLITE_ERROR")
}

/// The name of an extended or primary SQLite result code, for the codes a statement can fail
/// with.
fn extended_name(code: c_int) -> Option<&'static str> {
    Some(match code {
        ffi::SQLITE_ERROR => "SQLITE_ERROR",
        ffi::SQLITE_INTERNAL => "SQLITE_INTERNAL",
        ffi::SQLITE_PERM => "SQLITE_PERM",
        ffi::SQLITE_ABORT => "SQLITE_ABORT",
        ffi::SQLITE_BUSY => "SQLITE_BUSY",
        ffi::SQLITE_LOCKED => "SQLITE_LOCKED",
        ffi::SQLITE_NOMEM => "SQLITE_NOMEM",
        ffi::SQLITE_READONLY => "SQLITE_READONLY",
        ffi::SQLITE_INTERRUPT => "SQLITE_INTERRUPT",
        ffi::SQLITE_IOERR => "SQLITE_IOERR",
        ffi::SQLITE_CORRUPT => "SQLITE_CORRUPT",
        ffi::SQLITE_FULL => "SQLITE_FULL",
        ffi::SQLITE_CANTOPEN => "SQLITE_CANTOPEN",
        ffi::SQLITE_SCHEMA => "SQLITE_SCHEMA",
        ffi::SQLITE_TOOBIG => "SQLITE_TOOBIG",
        ffi::SQLITE_CONSTRAINT => "SQLITE_CONSTRAINT",
        ffi::SQLITE_MISMATCH => "SQLITE_MISMATCH",
        ffi::SQLITE_MISUSE => "SQLITE_MISUSE",
        ffi::SQLITE_AUTH => "SQLITE_AUTH",
        ffi::SQLITE_RANGE => "SQLITE_RANGE",
        ffi::SQLITE_CONSTRAINT_CHECK => "SQLITE_CONSTRAINT_CHECK",
        ffi::SQLITE_CONSTRAINT_COMMITHOOK => "SQLITE_CONSTRAINT_COMMITHOOK",
        ffi::SQLITE_CONSTRAINT_FOREIGNKEY => "SQLITE_CONSTRAINT_FOREIGNKEY",
        ffi::SQLITE_CONSTRAINT_FUNCTION => "SQLITE_CONSTRAINT_FUNCTION",
        ffi::SQLITE_CONSTRAINT_NOTNULL => "SQLITE_CONSTRAINT_NOTNULL",
        ffi::SQLITE_CONSTRAINT_PRIMARYKEY => "SQLITE_CONSTRAINT_PRIMARYKEY",
        ffi::SQLITE_CONSTRAINT_TRIGGER => "SQLITE_CONSTRAINT_TRIGGER",
        ffi::SQLITE_CONSTRAINT_UNIQUE => "SQLITE_CONSTRAINT_UNIQUE",
        ffi::SQLITE_CONSTRAINT_VTAB => "SQLITE_CONSTRAINT_VTAB",
        ffi::SQLITE_CONSTRAINT_ROWID => "SQLITE_CONSTRAINT_ROWID",
        _ => return None,
    })
}

/// The read connection and its confinement.
struct ReaderSide<'d> {
    conn: &'d Connection,
    confinement: &'d Confinement,
}

/// The writer and the state a session's writes change, borrowed apart from the reader.
struct WriterSide<'d> {
    path: &'d Path,
    wal: PathBuf,
    conn: &'d Connection,
    confinement: &'d Confinement,
    recorder: &'d mut Option<Recorder>,
    marks: &'d mut Marks,
    reserved: &'d mut Reserved,
    end: &'d mut Option<fleetring_wal::Position>,
    checkpointed: Option<fleetring_wal::Position>,
    transaction: &'d mut Option<OpenTransaction>,
}

impl WriterSide<'_> {
    /// Runs `prepared`, a statement prepared on the writer and classified as `class`, with the
    /// arguments and wishes of `statement` if it has them.
    fn run_prepared(
        &mut self,
        prepared: &mut Statement<'_>,
        class: Class,
        statement: Option<&SessionStatement>,
        interrupt: &Interrupt,
        budget: &mut Budget,
    ) -> Ran<StatementOutcome> {
        let in_transaction = !self.conn.is_autocommit();
        if !in_transaction && self.transaction.is_some() {
            // SQLite ended the transaction by itself, and nothing has seen it since.
            self.ended();
        }
        match (in_transaction, class.control) {
            (false, None) if class.writes() => {
                return self.write_alone(prepared, class, statement, interrupt, budget);
            }
            (true, Some(Control::Commit)) => return self.commit(prepared, statement, budget),
            _ => {}
        }
        if in_transaction && class.writes() {
            match self.begin_writing() {
                Ok(()) => {}
                Err(EngineError::Statement { message }) => {
                    self.roll_back();
                    return Ok(Err(rolled_back(StatementError::new(
                        "SQLITE_AUTH",
                        message,
                    ))));
                }
                Err(storage) => return Err(storage),
            }
        }
        let no_arguments = SessionStatement::default();
        let arguments = statement.unwrap_or(&no_arguments);
        let stepped = step(
            self.conn,
            self.confinement,
            prepared,
            arguments,
            interrupt,
            budget,
        )?;
        let now_in_transaction = !self.conn.is_autocommit();
        let outcome = match stepped {
            Ok(outcome) => outcome,
            Err(error) => {
                if in_transaction && !now_in_transaction {
                    // SQLite rolled the whole transaction back, as it does after some errors.
                    self.ended();
                }
                return Ok(Err(error));
            }
        };
        if class.writes() {
            if let Some(refusal) = self.refusal_after(class)? {
                self.roll_back();
                return Ok(Err(rolled_back(refusal)));
            }
            if let Some(recorder) = self.recorder.as_mut() {
                let sql = statement.map_or("", |statement| statement.sql.as_str());
                recorder.after_statement(self.conn, sql)?;
            }
        }
        match (in_transaction, now_in_transaction) {
            (false, true) => *self.transaction = Some(OpenTransaction::default()),
            (true, false) => self.ended(),
            _ => {}
        }
        Ok(Ok(outcome))
    }

    /// Runs a statement that writes outside a transaction in a transaction of its own, as an
    /// `Execute` of one statement runs.
    fn write_alone(
        &mut self,
        prepared: &mut Statement<'_>,
        class: Class,
        statement: Option<&SessionStatement>,
        interrupt: &Interrupt,
        budget: &mut Budget,
    ) -> Ran<StatementOutcome> {
        if let Err(error) = self.conn.execute_batch("BEGIN IMMEDIATE") {
            return own_error(error).map(Err);
        }
        let ran = self.write_in_own_transaction(prepared, class, statement, interrupt, budget);
        if !matches!(ran, Ok(Ok(_))) {
            self.roll_back();
        }
        ran
    }

    /// The body of [`write_alone`](Self::write_alone), inside its `BEGIN IMMEDIATE`; the caller
    /// rolls back whatever doesn't commit.
    fn write_in_own_transaction(
        &mut self,
        prepared: &mut Statement<'_>,
        class: Class,
        statement: Option<&SessionStatement>,
        interrupt: &Interrupt,
        budget: &mut Budget,
    ) -> Ran<StatementOutcome> {
        match self.begin_writing() {
            Ok(()) => {}
            Err(EngineError::Statement { message }) => {
                return Ok(Err(StatementError::new("SQLITE_AUTH", message)));
            }
            Err(storage) => return Err(storage),
        }
        let no_arguments = SessionStatement::default();
        let arguments = statement.unwrap_or(&no_arguments);
        let outcome = match step(
            self.conn,
            self.confinement,
            prepared,
            arguments,
            interrupt,
            budget,
        )? {
            Ok(outcome) => outcome,
            Err(error) => return Ok(Err(error)),
        };
        if let Some(refusal) = self.refusal_after(class)? {
            return Ok(Err(refusal));
        }
        if self.conn.is_autocommit() {
            return Ok(Err(StatementError::new(
                "SQLITE_ERROR",
                EngineError::TransactionControl.to_string(),
            )));
        }
        if let Some(recorder) = self.recorder.as_mut() {
            let sql = statement.map_or("", |statement| statement.sql.as_str());
            recorder.after_statement(self.conn, sql)?;
            recorder.commit(self.conn)?;
        }
        if let Err(error) = self.conn.execute_batch("COMMIT") {
            return own_error(error).map(Err);
        }
        self.committed()?;
        Ok(Ok(outcome))
    }

    /// Commits the open transaction with `prepared`, a `COMMIT` or `END`, after the recorder
    /// records its commit row. A commit that fails and leaves the transaction open, as a
    /// deferred constraint does, rolls it back.
    fn commit(
        &mut self,
        prepared: &mut Statement<'_>,
        statement: Option<&SessionStatement>,
        budget: &mut Budget,
    ) -> Ran<StatementOutcome> {
        let wrote = self
            .transaction
            .as_ref()
            .is_some_and(|transaction| transaction.wrote);
        if wrote
            && let Some(recorder) = self.recorder.as_ref()
            && let Err(error) = recorder.commit(self.conn)
        {
            self.roll_back();
            return Err(error);
        }
        let no_arguments = SessionStatement::default();
        let arguments = statement.unwrap_or(&no_arguments);
        // A transaction whose statements have all run always gets to commit: no interrupt.
        let stepped = step(
            self.conn,
            self.confinement,
            prepared,
            arguments,
            &Interrupt::new(),
            budget,
        )?;
        match stepped {
            Ok(outcome) => {
                self.committed()?;
                Ok(Ok(outcome))
            }
            Err(error) if self.conn.is_autocommit() => {
                self.ended();
                Ok(Err(error))
            }
            Err(error) => {
                self.roll_back();
                Ok(Err(rolled_back(error)))
            }
        }
    }

    /// Before a transaction's first write: records the staged sink marks, starts the recorder's
    /// transaction, and remembers the reserved schema, as an `Execute` does.
    fn begin_writing(&mut self) -> Result<(), EngineError> {
        if self
            .transaction
            .as_ref()
            .is_some_and(|transaction| transaction.wrote)
        {
            return Ok(());
        }
        if self.marks.is_staged() || self.recorder.is_some() {
            self.refuse_foreign_objects()?;
        }
        self.marks.flush(self.conn)?;
        if let Some(recorder) = self.recorder.as_mut() {
            recorder.begin(self.conn, *self.end)?;
        }
        self.reserved.remember(self.conn)?;
        if let Some(transaction) = self.transaction.as_mut() {
            transaction.wrote = true;
        }
        Ok(())
    }

    /// Why a statement that wrote must be undone, if it must: it changed the reserved
    /// namespace, or defined a trigger or view that names it.
    fn refusal_after(&mut self, class: Class) -> Result<Option<StatementError>, EngineError> {
        if self.reserved.changed(self.conn)? {
            return Ok(Some(StatementError::new(
                "SQLITE_AUTH",
                Refusal::Reserved.to_string(),
            )));
        }
        if class.defines_program {
            match refuse_programs_on_reserved_names(self.conn) {
                Ok(()) => {}
                Err(EngineError::Statement { message }) => {
                    return Ok(Some(StatementError::new("SQLITE_AUTH", message)));
                }
                Err(storage) => return Err(storage),
            }
        }
        Ok(None)
    }

    /// As [`SqliteDatabase::refuse_foreign_objects`]: fails if the reserved namespace holds an
    /// object the adapter didn't create.
    fn refuse_foreign_objects(&mut self) -> Result<(), EngineError> {
        let Some(foreign) = self.reserved.foreign(self.conn)? else {
            return Ok(());
        };
        tracing::warn!(
            path = %self.path.display(),
            kind = %foreign.kind,
            object = %foreign.name,
            reason = foreign.reason,
            "a foreign object in the reserved namespace stops change recording and sink marks"
        );
        Err(EngineError::Statement {
            message: foreign.to_string(),
        })
    }

    /// After a commit: the marks and the recorder's rows are permanent, and the log's end moved.
    fn committed(&mut self) -> Result<(), EngineError> {
        self.marks.committed();
        *self.transaction = None;
        if let Some(recorder) = self.recorder.as_mut() {
            recorder.committed();
            // The next transaction's changes are recorded from where this one ended.
            let after = self.end.map(encode_position);
            if let Some(batch) = read_wal_file(&self.wal, after.as_ref(), self.checkpointed)? {
                *self.end = Some(batch.end());
            }
        }
        Ok(())
    }

    /// After the transaction ended without a commit: forgets what it began.
    fn ended(&mut self) {
        self.reserved.forget();
        if let Some(recorder) = self.recorder.as_mut() {
            recorder.rolled_back();
        }
        *self.transaction = None;
    }

    /// Rolls back the open transaction, if any, and forgets what it began.
    fn roll_back(&mut self) {
        self.ended();
        if self.conn.is_autocommit() {
            return;
        }
        if let Err(error) = self.conn.execute_batch("ROLLBACK") {
            tracing::error!(path = %self.path.display(), %error, "session rollback failed");
        }
    }
}

/// `error`, saying the transaction rolled back.
fn rolled_back(error: StatementError) -> StatementError {
    StatementError {
        message: format!("{}; the transaction was rolled back", error.message),
        ..error
    }
}
