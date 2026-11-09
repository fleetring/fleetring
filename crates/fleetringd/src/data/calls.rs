//! A SQL session's calls and results, between the protocol's messages and the engine's types
//! (ADR-0037).

use fleetring_proto::v1::{
    self as proto, batch_condition::Condition, batch_step_result::Step, session_call::Call,
    session_result::Result as Answer,
};
use tonic::Status;

use crate::data::convert::{value_from_proto, value_to_proto};
use crate::engine::{
    BatchCondition, BatchStep, CallOutcome, CallResult, Description, ResultColumn, SessionCall,
    SessionStatement, StatementError, StatementOutcome, StepOutcome, WantRows,
};
use crate::publisher::Acknowledged;

/// How deeply a batch step's condition may nest. Clients build conditions a level or two deep;
/// the bound keeps a hostile one from exhausting the stack.
pub(crate) const MAX_CONDITION_DEPTH: usize = 32;

/// The call `call` carries, with the ID its result goes back under.
///
/// # Errors
///
/// `INVALID_ARGUMENT` for a call with nothing in it, a value with nothing in it, or a condition
/// nested deeper than [`MAX_CONDITION_DEPTH`].
pub(crate) fn call_from_proto(call: proto::SessionCall) -> Result<(u64, SessionCall), Status> {
    let id = call.call_id;
    let call = match call.call {
        Some(Call::Execute(statement)) => SessionCall::Execute(statement_from_proto(statement)?),
        Some(Call::Batch(batch)) => SessionCall::Batch(
            batch
                .steps
                .into_iter()
                .map(step_from_proto)
                .collect::<Result<_, _>>()?,
        ),
        Some(Call::Sequence(sql)) => SessionCall::Sequence(sql),
        Some(Call::Describe(sql)) => SessionCall::Describe(sql),
        Some(Call::GetAutocommit(_)) => SessionCall::GetAutocommit,
        None => return Err(Status::invalid_argument("a session call names no call")),
    };
    Ok((id, call))
}

/// `call` as the protocol carries it, under `id`.
pub(crate) fn call_to_proto(id: u64, call: &SessionCall) -> proto::SessionCall {
    let call = match call {
        SessionCall::Execute(statement) => Call::Execute(statement_to_proto(statement)),
        SessionCall::Batch(steps) => Call::Batch(proto::SessionBatch {
            steps: steps
                .iter()
                .map(|step| proto::BatchStep {
                    condition: step.condition.as_ref().map(condition_to_proto),
                    statement: Some(statement_to_proto(&step.statement)),
                })
                .collect(),
        }),
        SessionCall::Sequence(sql) => Call::Sequence(sql.clone()),
        SessionCall::Describe(sql) => Call::Describe(sql.clone()),
        SessionCall::GetAutocommit => Call::GetAutocommit(proto::GetAutocommit {}),
    };
    proto::SessionCall {
        call_id: id,
        call: Some(call),
    }
}

fn statement_from_proto(statement: proto::SessionStatement) -> Result<SessionStatement, Status> {
    let args = statement
        .args
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            value_from_proto(value).ok_or_else(|| {
                Status::invalid_argument(format!("argument {} has no value", index + 1))
            })
        })
        .collect::<Result<_, _>>()?;
    let named_args = statement
        .named_args
        .into_iter()
        .map(|named| {
            let value = named.value.and_then(value_from_proto).ok_or_else(|| {
                Status::invalid_argument(format!("argument {} has no value", named.name))
            })?;
            Ok((named.name, value))
        })
        .collect::<Result<_, Status>>()?;
    Ok(SessionStatement {
        sql: statement.sql,
        args,
        named_args,
        want_rows: match statement.want_rows {
            Some(false) => WantRows::NoRows,
            _ => WantRows::Rows,
        },
    })
}

fn statement_to_proto(statement: &SessionStatement) -> proto::SessionStatement {
    proto::SessionStatement {
        sql: statement.sql.clone(),
        args: statement.args.iter().cloned().map(value_to_proto).collect(),
        named_args: statement
            .named_args
            .iter()
            .map(|(name, value)| proto::NamedValue {
                name: name.clone(),
                value: Some(value_to_proto(value.clone())),
            })
            .collect(),
        want_rows: match statement.want_rows {
            WantRows::Rows => None,
            WantRows::NoRows => Some(false),
        },
    }
}

fn step_from_proto(step: proto::BatchStep) -> Result<BatchStep, Status> {
    let statement = step
        .statement
        .ok_or_else(|| Status::invalid_argument("a batch step has no statement"))?;
    Ok(BatchStep {
        condition: step
            .condition
            .map(|condition| condition_from_proto(condition, 1))
            .transpose()?,
        statement: statement_from_proto(statement)?,
    })
}

fn condition_from_proto(
    condition: proto::BatchCondition,
    depth: usize,
) -> Result<BatchCondition, Status> {
    if depth > MAX_CONDITION_DEPTH {
        return Err(Status::invalid_argument(format!(
            "a batch condition nests deeper than {MAX_CONDITION_DEPTH}"
        )));
    }
    let all = |conditions: proto::BatchConditions| {
        conditions
            .conditions
            .into_iter()
            .map(|condition| condition_from_proto(condition, depth + 1))
            .collect::<Result<Vec<_>, _>>()
    };
    Ok(match condition.condition {
        Some(Condition::Ok(step)) => BatchCondition::Ok(step),
        Some(Condition::Error(step)) => BatchCondition::Error(step),
        Some(Condition::Not(inner)) => {
            BatchCondition::Not(Box::new(condition_from_proto(*inner, depth + 1)?))
        }
        Some(Condition::And(inner)) => BatchCondition::And(all(inner)?),
        Some(Condition::Or(inner)) => BatchCondition::Or(all(inner)?),
        Some(Condition::IsAutocommit(_)) => BatchCondition::IsAutocommit,
        None => return Err(Status::invalid_argument("a batch condition is empty")),
    })
}

fn condition_to_proto(condition: &BatchCondition) -> proto::BatchCondition {
    let all = |conditions: &[BatchCondition]| proto::BatchConditions {
        conditions: conditions.iter().map(condition_to_proto).collect(),
    };
    let condition = match condition {
        BatchCondition::Ok(step) => Condition::Ok(*step),
        BatchCondition::Error(step) => Condition::Error(*step),
        BatchCondition::Not(inner) => Condition::Not(Box::new(condition_to_proto(inner))),
        BatchCondition::And(inner) => Condition::And(all(inner)),
        BatchCondition::Or(inner) => Condition::Or(all(inner)),
        BatchCondition::IsAutocommit => Condition::IsAutocommit(proto::IsAutocommit {}),
    };
    proto::BatchCondition {
        condition: Some(condition),
    }
}

/// The result of call `id`, which produced `result`, with `commit` if it committed.
pub(crate) fn result_to_proto(
    id: u64,
    result: CallResult,
    commit: Option<proto::Commit>,
) -> proto::SessionResult {
    let answer = match result.outcome {
        CallOutcome::Statement(Ok(outcome)) => Answer::Statement(outcome_to_proto(outcome)),
        CallOutcome::Statement(Err(error))
        | CallOutcome::Sequence(Err(error))
        | CallOutcome::Described(Err(error)) => Answer::Error(error_to_proto(error)),
        CallOutcome::Batch(steps) => Answer::Batch(proto::BatchResult {
            steps: steps
                .into_iter()
                .map(|step| proto::BatchStepResult {
                    step: Some(match step {
                        StepOutcome::Ok(outcome) => Step::Ok(outcome_to_proto(outcome)),
                        StepOutcome::Error(error) => Step::Error(error_to_proto(error)),
                        StepOutcome::Skipped => Step::Skipped(proto::Skipped {}),
                    }),
                })
                .collect(),
        }),
        CallOutcome::Sequence(Ok(())) => Answer::Sequence(proto::SequenceResult {}),
        CallOutcome::Described(Ok(description)) => {
            Answer::Described(description_to_proto(description))
        }
        CallOutcome::Autocommit => Answer::AutocommitResult(proto::AutocommitResult {}),
    };
    proto::SessionResult {
        call_id: id,
        result: Some(answer),
        autocommit: result.autocommit == crate::engine::Autocommit::On,
        commit,
    }
}

/// How a commit was acknowledged, under the lease at `epoch`.
pub(crate) fn commit_to_proto(acknowledged: Acknowledged, epoch: u64) -> proto::Commit {
    proto::Commit {
        acknowledged_by: acknowledged.acknowledged_by,
        replicas: acknowledged.replicas,
        under_replicated: acknowledged.is_under_replicated(),
        epoch,
    }
}

/// A result for call `id` saying it failed with `code` and `message`, and the session's state.
pub(crate) fn error_result(
    id: u64,
    code: &str,
    message: String,
    autocommit: bool,
) -> proto::SessionResult {
    proto::SessionResult {
        call_id: id,
        result: Some(Answer::Error(proto::SessionError {
            message,
            code: code.to_owned(),
        })),
        autocommit,
        commit: None,
    }
}

/// Why a call didn't run, and what becomes of its session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NotRun {
    /// `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, `FLEETRING_TRANSACTION_ABORTED`, or the code of
    /// the status routing failed with.
    pub(crate) code: &'static str,
    pub(crate) message: String,
    /// The primary that serves the database now, if the call was refused because it moved.
    pub(crate) primary: Option<String>,
    /// What becomes of the session after this answer.
    pub(crate) then: Then,
}

/// What becomes of a session after a call's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Then {
    /// It goes on.
    GoesOn,
    /// It ends.
    Ends,
}

/// The result of call `id`, which didn't run, with the session's state.
pub(crate) fn not_run_result(id: u64, not_run: NotRun, autocommit: bool) -> proto::SessionResult {
    proto::SessionResult {
        call_id: id,
        result: Some(Answer::NotRun(proto::NotRun {
            code: not_run.code.to_owned(),
            message: not_run.message,
            primary: not_run.primary,
            ends_session: not_run.then == Then::Ends,
        })),
        autocommit,
        commit: None,
    }
}

fn outcome_to_proto(outcome: StatementOutcome) -> proto::StatementResult {
    proto::StatementResult {
        columns: outcome.columns.into_iter().map(column_to_proto).collect(),
        rows: outcome
            .rows
            .into_iter()
            .map(|row| proto::Row {
                values: row.into_iter().map(value_to_proto).collect(),
            })
            .collect(),
        affected_row_count: outcome.affected_row_count,
        last_insert_rowid: outcome.last_insert_rowid,
    }
}

fn column_to_proto(column: ResultColumn) -> proto::SessionColumn {
    proto::SessionColumn {
        name: column.name,
        decltype: column.decltype,
    }
}

fn error_to_proto(error: StatementError) -> proto::SessionError {
    proto::SessionError {
        message: error.message,
        code: error.code,
    }
}

fn description_to_proto(description: Description) -> proto::Described {
    proto::Described {
        params: description
            .params
            .into_iter()
            .map(|name| proto::DescribedParam { name })
            .collect(),
        columns: description
            .columns
            .into_iter()
            .map(column_to_proto)
            .collect(),
        is_explain: description.is_explain,
        is_readonly: description.is_readonly,
    }
}

/// Whether `result` holds a statement a read-only session refused, for the audit log.
pub(crate) fn refused_a_write(result: &CallResult) -> bool {
    let refused = |error: &StatementError| error.code == crate::engine::PERMISSION_DENIED;
    match &result.outcome {
        CallOutcome::Statement(Err(error)) | CallOutcome::Sequence(Err(error)) => refused(error),
        CallOutcome::Batch(steps) => steps
            .iter()
            .any(|step| matches!(step, StepOutcome::Error(error) if refused(error))),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Value;

    #[test]
    fn session_calls_round_trip_through_the_protocol() {
        let statement = SessionStatement {
            sql: "SELECT ?, :a".to_owned(),
            args: vec![Value::Integer(1), Value::Null],
            named_args: vec![(":a".to_owned(), Value::Blob(vec![1, 2]))],
            want_rows: WantRows::NoRows,
        };
        let calls = [
            SessionCall::Execute(statement.clone()),
            SessionCall::Batch(vec![
                BatchStep {
                    condition: None,
                    statement: statement.clone(),
                },
                BatchStep {
                    condition: Some(BatchCondition::And(vec![
                        BatchCondition::Ok(0),
                        BatchCondition::Not(Box::new(BatchCondition::Or(vec![
                            BatchCondition::Error(0),
                            BatchCondition::IsAutocommit,
                        ]))),
                    ])),
                    statement: SessionStatement::new("COMMIT"),
                },
            ]),
            SessionCall::Sequence("SELECT 1; SELECT 2".to_owned()),
            SessionCall::Describe("SELECT 1".to_owned()),
            SessionCall::GetAutocommit,
        ];
        for (id, call) in calls.into_iter().enumerate() {
            let id = u64::try_from(id).expect("small");
            let back = call_from_proto(call_to_proto(id, &call)).expect("valid");
            assert_eq!(back, (id, call));
        }
    }

    #[test]
    fn a_condition_too_deep_is_refused() {
        let mut condition = BatchCondition::Ok(0);
        for _ in 0..MAX_CONDITION_DEPTH {
            condition = BatchCondition::Not(Box::new(condition));
        }
        let call = SessionCall::Batch(vec![BatchStep {
            condition: Some(condition),
            statement: SessionStatement::new("SELECT 1"),
        }]);
        let refused = call_from_proto(call_to_proto(1, &call)).expect_err("too deep");
        assert_eq!(refused.code(), tonic::Code::InvalidArgument);
        let empty = proto::SessionCall {
            call_id: 1,
            call: None,
        };
        assert_eq!(
            call_from_proto(empty).expect_err("empty").code(),
            tonic::Code::InvalidArgument
        );
    }
}
