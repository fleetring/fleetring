//! SQL sessions: statements that run with the engine's own transaction semantics (ADR-0037).
//!
//! A session is the data plane's half of a Hrana stream. Its calls run one at a time on the
//! database's primary: each statement autocommits unless the session began a transaction, a
//! transaction can span calls, and a failed step of a batch leaves the earlier steps as they
//! are. Whether a statement is read-only, and whether it controls a transaction, is the engine's
//! answer when it prepares the statement, never a parse of the text above the adapter.
//!
//! The types here are engine-neutral: an engine that can run SQL sessions implements
//! [`SqlSessions`], and [`Database::sql_sessions`](crate::engine::Database::sql_sessions) hands it
//! out.

use std::fmt;

use crate::engine::{EngineError, Interrupt, Value};

/// Whether a session's statements may write (ADR-0037, point 17).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum SessionAccess {
    /// Statements may read, write, and control transactions.
    ReadWrite,
    /// Only statements the engine reports as read-only run; every other one fails with
    /// [`PERMISSION_DENIED`] without running.
    ReadOnly,
}

/// Whether a statement's result carries its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum WantRows {
    /// The result carries the rows the statement produced.
    #[default]
    Rows,
    /// The result carries no rows, whatever the statement produced.
    NoRows,
}

/// One statement of a session: SQL text with positional and named arguments.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionStatement {
    /// The statement's text: one statement.
    pub sql: String,
    /// Arguments bound by position, the first to parameter 1.
    pub args: Vec<Value>,
    /// Arguments bound by name. A name carries its prefix character (`:a`, `@a`, `$a`), as Hrana
    /// sends it; one without a prefix is tried with each.
    pub named_args: Vec<(String, Value)>,
    /// Whether the result carries rows.
    pub want_rows: WantRows,
}

impl SessionStatement {
    /// A statement without arguments, whose result carries its rows.
    #[must_use]
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            ..Self::default()
        }
    }
}

/// When a step of a batch runs, judged by the steps before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchCondition {
    /// The step at this index ran and succeeded.
    Ok(u32),
    /// The step at this index ran and failed.
    Error(u32),
    /// The condition doesn't hold.
    Not(Box<BatchCondition>),
    /// Every condition holds; true when there are none.
    And(Vec<BatchCondition>),
    /// Some condition holds; false when there are none.
    Or(Vec<BatchCondition>),
    /// The session is outside a transaction.
    IsAutocommit,
}

impl BatchCondition {
    /// How deeply the condition nests: 1 for one without inner conditions.
    #[must_use]
    pub fn depth(&self) -> usize {
        match self {
            Self::Ok(_) | Self::Error(_) | Self::IsAutocommit => 1,
            Self::Not(inner) => 1 + inner.depth(),
            Self::And(inner) | Self::Or(inner) => {
                1 + inner.iter().map(Self::depth).max().unwrap_or(0)
            }
        }
    }
}

/// One step of a batch: a statement, and the condition it runs under.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BatchStep {
    /// The step runs only if this holds; always, without one.
    pub condition: Option<BatchCondition>,
    /// The statement.
    pub statement: SessionStatement,
}

/// One call a session makes.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum SessionCall {
    /// Runs one statement.
    Execute(SessionStatement),
    /// Runs steps in order, each under its condition.
    Batch(Vec<BatchStep>),
    /// Runs the statements of one text in order until the first fails; their rows are dropped.
    Sequence(String),
    /// Describes one statement without running it.
    Describe(String),
    /// Reports whether the session is outside a transaction.
    GetAutocommit,
}

/// A column of a statement's result.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResultColumn {
    /// The column's name, if it has one.
    pub name: Option<String>,
    /// The declared type of the table column it comes from, if it comes from one.
    pub decltype: Option<String>,
}

/// A statement that ran.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StatementOutcome {
    /// The result's columns.
    pub columns: Vec<ResultColumn>,
    /// The result's rows, each with one value per column; none if the statement asked for none.
    pub rows: Vec<Vec<Value>>,
    /// The rows the statement itself inserted, updated, or deleted.
    pub affected_row_count: u64,
    /// The row ID of the last row inserted, after a statement that changed rows.
    pub last_insert_rowid: Option<i64>,
}

/// Why one statement failed. The session goes on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementError {
    /// What went wrong, for people.
    pub message: String,
    /// What went wrong, for programs: the engine's own code (SQLite's extended result code name,
    /// such as `SQLITE_CONSTRAINT_UNIQUE`), or one of this module's codes.
    pub code: String,
}

impl StatementError {
    /// An error with `code` and `message`.
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for StatementError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

impl std::error::Error for StatementError {}

/// The code of a statement a read-only session may not run: one that isn't read-only, or that
/// controls a transaction.
pub const PERMISSION_DENIED: &str = "PERMISSION_DENIED";

/// The code of a `SAVEPOINT` outside a transaction: sessions begin transactions with `BEGIN`.
pub const SAVEPOINT_OUTSIDE_TRANSACTION: &str = "FLEETRING_SAVEPOINT";

/// The code of a result holding a value that can't be sent: a real that isn't finite.
pub const UNSENDABLE_VALUE: &str = "FLEETRING_VALUE";

/// The code of a call whose results grew past what one answer may hold.
pub const RESULT_TOO_LARGE: &str = "FLEETRING_RESULT_TOO_LARGE";

/// The code of a statement stopped at its deadline (FR-078).
pub const DEADLINE_EXCEEDED: &str = "DEADLINE_EXCEEDED";

/// The code of a statement stopped because its call was cancelled.
pub const CANCELLED: &str = "CANCELLED";

/// The most bytes of rows one call's results may hold, so that an answer always fits in one
/// message to the router, with room to spare under the transport's 64 MiB.
pub const MAX_RESULT_BYTES: usize = 32 * 1024 * 1024;

/// What became of one step of a batch.
#[derive(Debug, Clone, PartialEq)]
pub enum StepOutcome {
    /// The step ran and succeeded.
    Ok(StatementOutcome),
    /// The step ran and failed.
    Error(StatementError),
    /// The step's condition didn't hold, so it didn't run.
    Skipped,
}

/// A statement described without running it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Description {
    /// Each parameter's name with its prefix, from parameter 1 on; `None` for a nameless one.
    pub params: Vec<Option<String>>,
    /// The result's columns.
    pub columns: Vec<ResultColumn>,
    /// Whether the statement is an `EXPLAIN`.
    pub is_explain: bool,
    /// Whether the engine reports the statement as read-only.
    pub is_readonly: bool,
}

/// What a call produced.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum CallOutcome {
    /// An [`Execute`](SessionCall::Execute)'s statement.
    Statement(Result<StatementOutcome, StatementError>),
    /// A [`Batch`](SessionCall::Batch)'s steps, one outcome each.
    Batch(Vec<StepOutcome>),
    /// A [`Sequence`](SessionCall::Sequence): ran to the end, or failed at one statement.
    Sequence(Result<(), StatementError>),
    /// A [`Describe`](SessionCall::Describe).
    Described(Result<Description, StatementError>),
    /// A [`GetAutocommit`](SessionCall::GetAutocommit); the answer is the call's
    /// [`CallResult::autocommit`].
    Autocommit,
}

/// Whether a session is outside a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Autocommit {
    /// Outside a transaction: each statement commits by itself.
    #[default]
    On,
    /// Inside a transaction the session began.
    InTransaction,
}

/// Whether a call may write, which decides whether it needs the database's writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WriteIntent {
    /// Every statement is read-only and none controls a transaction.
    ReadOnly,
    /// Some statement may write or control a transaction, or couldn't be prepared to tell.
    MayWrite,
}

/// A call that ran, and the session's state after it.
#[derive(Debug, Clone, PartialEq)]
pub struct CallResult {
    /// What the call produced.
    pub outcome: CallOutcome,
    /// Whether the session is outside a transaction after the call.
    pub autocommit: Autocommit,
}

/// Runs a session's calls with the engine's own transaction semantics (ADR-0037, point 12).
///
/// An open database has one writer, so it holds at most one session's transaction at a time;
/// the caller makes sure only that session's calls reach [`run`](Self::run) with
/// [`SessionAccess::ReadWrite`] while it is open, and runs other sessions' reads with
/// [`SessionAccess::ReadOnly`].
///
/// Every user statement is confined as the engine confines any other (ADR-0026, point 8), and
/// stops once `interrupt` is raised (FR-078).
pub trait SqlSessions: fmt::Debug + Send {
    /// Whether `call` may write: whether any of its statements isn't read-only, controls a
    /// transaction, or can't be prepared. Prepares statements and runs none.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if storage fails.
    fn intent(&mut self, call: &SessionCall) -> Result<WriteIntent, EngineError>;

    /// Runs `call` with `access`, unless `interrupt` stops its statements. A statement that fails
    /// is in the outcome; the session goes on.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if storage fails; the open transaction, if any, rolled back.
    fn run(
        &mut self,
        call: &SessionCall,
        access: SessionAccess,
        interrupt: &Interrupt,
    ) -> Result<CallResult, EngineError>;

    /// Whether a transaction is open on the writer.
    fn autocommit(&self) -> Autocommit;

    /// Rolls back the open transaction, if any.
    ///
    /// # Errors
    ///
    /// [`EngineError::Storage`] if the rollback fails.
    fn roll_back(&mut self) -> Result<(), EngineError>;
}

/// Whether `condition` holds after `outcomes`, the outcomes of the steps before it, with the
/// session's transaction state `autocommit`. A step index at or past `outcomes.len()`, a step that
/// hasn't run yet, is neither ok nor an error.
#[must_use]
pub fn holds(condition: &BatchCondition, outcomes: &[StepOutcome], autocommit: Autocommit) -> bool {
    let step = |index: u32| {
        usize::try_from(index)
            .ok()
            .and_then(|index| outcomes.get(index))
    };
    match condition {
        BatchCondition::Ok(index) => matches!(step(*index), Some(StepOutcome::Ok(_))),
        BatchCondition::Error(index) => matches!(step(*index), Some(StepOutcome::Error(_))),
        BatchCondition::Not(inner) => !holds(inner, outcomes, autocommit),
        BatchCondition::And(inner) => inner
            .iter()
            .all(|condition| holds(condition, outcomes, autocommit)),
        BatchCondition::Or(inner) => inner
            .iter()
            .any(|condition| holds(condition, outcomes, autocommit)),
        BatchCondition::IsAutocommit => autocommit == Autocommit::On,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conditions_follow_the_outcomes_of_earlier_steps() {
        let outcomes = [
            StepOutcome::Ok(StatementOutcome::default()),
            StepOutcome::Error(StatementError::new("SQLITE_ERROR", "no")),
            StepOutcome::Skipped,
        ];
        let on = Autocommit::On;
        assert!(holds(&BatchCondition::Ok(0), &outcomes, on));
        assert!(!holds(&BatchCondition::Ok(1), &outcomes, on));
        assert!(holds(&BatchCondition::Error(1), &outcomes, on));
        assert!(!holds(&BatchCondition::Error(0), &outcomes, on));
        // A skipped step is neither ok nor an error.
        assert!(!holds(&BatchCondition::Ok(2), &outcomes, on));
        assert!(!holds(&BatchCondition::Error(2), &outcomes, on));
        // A step that hasn't run yet is neither.
        assert!(!holds(&BatchCondition::Ok(3), &outcomes, on));
        assert!(!holds(&BatchCondition::Error(u32::MAX), &outcomes, on));
        let not_ok = BatchCondition::Not(Box::new(BatchCondition::Ok(1)));
        assert!(holds(&not_ok, &outcomes, on));
        let both = BatchCondition::And(vec![BatchCondition::Ok(0), BatchCondition::Error(1)]);
        assert!(holds(&both, &outcomes, on));
        assert!(holds(&BatchCondition::And(Vec::new()), &outcomes, on));
        let either = BatchCondition::Or(vec![BatchCondition::Ok(1), BatchCondition::Ok(0)]);
        assert!(holds(&either, &outcomes, on));
        assert!(!holds(&BatchCondition::Or(Vec::new()), &outcomes, on));
        assert!(holds(&BatchCondition::IsAutocommit, &outcomes, on));
        assert!(!holds(
            &BatchCondition::IsAutocommit,
            &outcomes,
            Autocommit::InTransaction
        ));
    }

    #[test]
    fn a_condition_knows_how_deeply_it_nests() {
        let leaf = BatchCondition::Ok(0);
        assert_eq!(leaf.depth(), 1);
        let nested = BatchCondition::Not(Box::new(BatchCondition::And(vec![
            leaf.clone(),
            BatchCondition::Or(vec![leaf]),
        ])));
        assert_eq!(nested.depth(), 4);
    }
}
