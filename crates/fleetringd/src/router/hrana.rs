//! Hrana's JSON encoding (the libSQL `HRANA_3_SPEC`, "Hrana over HTTP" and "Shared
//! structures"), and its conversion to and from sessions' calls and results.
//!
//! Integers travel as decimal strings, blobs as base64 (accepted with or without padding, sent
//! without, as libSQL's own server sends them), and floats as JSON numbers. Fields a request
//! carries that this router doesn't know are ignored, as the specification allows.

use base64::Engine as _;
use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
use fleetring_proto::v1::{self as proto, batch_step_result::Step, value::Value as Kind};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};

use crate::engine::{
    BatchCondition, BatchStep as EngineStep, SessionStatement, Value as SqlValue, WantRows,
};

/// Blobs as Hrana sends them: the standard alphabet, padding optional on the way in and left
/// out on the way out.
const BASE64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// `POST v2/pipeline` and `POST v3/pipeline`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct PipelineRequest {
    #[serde(default)]
    pub(crate) baton: Option<String>,
    pub(crate) requests: Vec<StreamRequest>,
}

/// The answer to a pipeline.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct PipelineResponse {
    pub(crate) baton: Option<String>,
    pub(crate) base_url: Option<String>,
    pub(crate) results: Vec<StreamResult>,
}

/// `POST v3/cursor`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct CursorRequest {
    #[serde(default)]
    pub(crate) baton: Option<String>,
    pub(crate) batch: Batch,
}

/// The first line of a cursor's answer.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct CursorResponse {
    pub(crate) baton: Option<String>,
    pub(crate) base_url: Option<String>,
}

/// One line of a cursor's answer after the first.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum CursorEntry {
    StepBegin {
        step: u32,
        cols: Vec<Col>,
    },
    StepEnd {
        affected_row_count: u64,
        last_insert_rowid: Option<String>,
    },
    StepError {
        step: u32,
        error: Error,
    },
    Row {
        row: Vec<Value>,
    },
    Error {
        error: Error,
    },
}

/// One request of a pipeline.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum StreamRequest {
    Close,
    Execute {
        stmt: Stmt,
    },
    Batch {
        batch: Batch,
    },
    Sequence {
        #[serde(default)]
        sql: Option<String>,
        #[serde(default)]
        sql_id: Option<i32>,
    },
    Describe {
        #[serde(default)]
        sql: Option<String>,
        #[serde(default)]
        sql_id: Option<i32>,
    },
    StoreSql {
        sql_id: i32,
        sql: String,
    },
    CloseSql {
        sql_id: i32,
    },
    GetAutocommit,
}

impl StreamRequest {
    /// How many statements the request carries, toward a request's bound.
    pub(crate) fn statements(&self) -> usize {
        match self {
            Self::Batch { batch } => batch.steps.len().max(1),
            _ => 1,
        }
    }
}

/// A statement as Hrana carries it.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
pub(crate) struct Stmt {
    #[serde(default)]
    pub(crate) sql: Option<String>,
    #[serde(default)]
    pub(crate) sql_id: Option<i32>,
    #[serde(default)]
    pub(crate) args: Vec<Value>,
    #[serde(default)]
    pub(crate) named_args: Vec<NamedArg>,
    #[serde(default)]
    pub(crate) want_rows: Option<bool>,
}

/// A named argument.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct NamedArg {
    pub(crate) name: String,
    pub(crate) value: Value,
}

/// A batch.
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
pub(crate) struct Batch {
    pub(crate) steps: Vec<BatchStep>,
}

/// One step of a batch.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct BatchStep {
    #[serde(default)]
    pub(crate) condition: Option<BatchCond>,
    pub(crate) stmt: Stmt,
}

/// A batch step's condition.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BatchCond {
    Ok { step: u32 },
    Error { step: u32 },
    Not { cond: Box<BatchCond> },
    And { conds: Vec<BatchCond> },
    Or { conds: Vec<BatchCond> },
    IsAutocommit,
}

/// A value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum Value {
    Null,
    Integer {
        #[serde(deserialize_with = "integer", serialize_with = "decimal")]
        value: i64,
    },
    Float {
        value: f64,
    },
    Text {
        value: String,
    },
    Blob {
        #[serde(rename = "base64")]
        base64: String,
    },
}

/// An integer as a decimal string, or as a JSON number from a lenient client.
fn integer<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Either {
        Text(String),
        Number(i64),
    }
    match Either::deserialize(deserializer)? {
        Either::Text(text) => text
            .parse()
            .map_err(|_| de::Error::custom(format!("'{text}' isn't a 64-bit integer"))),
        Either::Number(number) => Ok(number),
    }
}

fn decimal<S: serde::Serializer>(value: &i64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}

/// One request's outcome.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum StreamResult {
    Ok { response: StreamResponse },
    Error { error: Error },
}

/// One request's answer.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum StreamResponse {
    Close,
    Execute { result: StmtResult },
    Batch { result: BatchResult },
    Sequence,
    Describe { result: DescribeResult },
    StoreSql,
    CloseSql,
    GetAutocommit { is_autocommit: bool },
}

/// A statement's result. `rows_read`, `rows_written`, and `query_duration_ms` are left out:
/// clients default them.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct StmtResult {
    pub(crate) cols: Vec<Col>,
    pub(crate) rows: Vec<Vec<Value>>,
    pub(crate) affected_row_count: u64,
    pub(crate) last_insert_rowid: Option<String>,
}

/// A result column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Col {
    pub(crate) name: Option<String>,
    pub(crate) decltype: Option<String>,
}

/// A batch's result.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct BatchResult {
    pub(crate) step_results: Vec<Option<StmtResult>>,
    pub(crate) step_errors: Vec<Option<Error>>,
}

/// A description's result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DescribeResult {
    pub(crate) params: Vec<DescribeParam>,
    pub(crate) cols: Vec<Col>,
    pub(crate) is_explain: bool,
    pub(crate) is_readonly: bool,
}

/// A described parameter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DescribeParam {
    pub(crate) name: Option<String>,
}

/// An error, for a request or for a whole HTTP request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Error {
    pub(crate) message: String,
    pub(crate) code: Option<String>,
}

impl Error {
    /// An error with `code` and `message`.
    pub(crate) fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: Some(code.to_owned()),
        }
    }
}

/// The router's own error codes; SQLite's pass through from the node.
pub(crate) mod code {
    /// The request isn't valid Hrana, or uses what the version doesn't have.
    pub(crate) const PROTOCOL_ERROR: &str = "PROTOCOL_ERROR";
    /// The request carries no valid token.
    pub(crate) const UNAUTHENTICATED: &str = "UNAUTHENTICATED";
    /// The token doesn't allow the request.
    pub(crate) const PERMISSION_DENIED: &str = "PERMISSION_DENIED";
    /// No such database, or no such endpoint.
    pub(crate) const NOT_FOUND: &str = "NOT_FOUND";
    /// The baton names a stream that expired or never existed.
    pub(crate) const STREAM_EXPIRED: &str = "STREAM_EXPIRED";
    /// The baton doesn't continue its stream: an old or forged one, or another database's.
    pub(crate) const STREAM_INVALID: &str = "STREAM_INVALID";
    /// The database's primary can't be reached now; nothing ran.
    pub(crate) const UNAVAILABLE: &str = "UNAVAILABLE";
    /// A bound was reached; nothing ran.
    pub(crate) const RESOURCE_EXHAUSTED: &str = "RESOURCE_EXHAUSTED";
    /// The connection to the primary broke before the result arrived: it may or may not have run.
    pub(crate) const OUTCOME_UNKNOWN: &str = "FLEETRING_OUTCOME_UNKNOWN";
    /// The stream's transaction ended on the primary without its commit.
    pub(crate) const TRANSACTION_ABORTED: &str = "FLEETRING_TRANSACTION_ABORTED";
    /// The request is too large.
    pub(crate) const TOO_LARGE: &str = "REQUEST_TOO_LARGE";
    /// Something inside the router failed.
    pub(crate) const INTERNAL: &str = "INTERNAL";
}

/// The engine's value for `value`.
///
/// # Errors
///
/// A protocol error for a blob that isn't base64.
pub(crate) fn value_to_sql(value: Value) -> Result<SqlValue, Error> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Integer { value } => SqlValue::Integer(value),
        Value::Float { value } => SqlValue::Real(value),
        Value::Text { value } => SqlValue::Text(value),
        Value::Blob { base64 } => SqlValue::Blob(
            BASE64
                .decode(base64.as_bytes())
                .map_err(|_| Error::new(code::PROTOCOL_ERROR, "a blob isn't valid base64"))?,
        ),
    })
}

/// Hrana's value for `value`, as a node sent it.
pub(crate) fn value_from_proto(value: proto::Value) -> Value {
    match value.value {
        None | Some(Kind::Null(_)) => Value::Null,
        Some(Kind::Integer(integer)) => Value::Integer { value: integer },
        Some(Kind::Real(real)) => Value::Float { value: real },
        Some(Kind::Text(text)) => Value::Text { value: text },
        Some(Kind::Blob(blob)) => Value::Blob {
            base64: BASE64.encode(blob),
        },
    }
}

/// The session statement `stmt` asks for, with its SQL text already resolved to `sql`.
///
/// # Errors
///
/// A protocol error for an argument that isn't a valid value.
pub(crate) fn statement(stmt: Stmt, sql: String) -> Result<SessionStatement, Error> {
    Ok(SessionStatement {
        sql,
        args: stmt
            .args
            .into_iter()
            .map(value_to_sql)
            .collect::<Result<_, _>>()?,
        named_args: stmt
            .named_args
            .into_iter()
            .map(|named| Ok((named.name, value_to_sql(named.value)?)))
            .collect::<Result<_, Error>>()?,
        want_rows: match stmt.want_rows {
            Some(false) => WantRows::NoRows,
            _ => WantRows::Rows,
        },
    })
}

/// The engine's condition for `condition`, which Hrana `version` must have, nested at most
/// `depth_left` deep.
///
/// # Errors
///
/// A protocol error for `is_autocommit` on Hrana 2, or a condition nested too deep.
pub(crate) fn condition(
    condition: BatchCond,
    allows_autocommit: bool,
    depth_left: usize,
) -> Result<BatchCondition, Error> {
    let Some(next) = depth_left.checked_sub(1) else {
        return Err(Error::new(
            code::PROTOCOL_ERROR,
            "a batch condition nests too deeply",
        ));
    };
    let all = |conditions: Vec<BatchCond>| {
        conditions
            .into_iter()
            .map(|inner| self::condition(inner, allows_autocommit, next))
            .collect::<Result<Vec<_>, _>>()
    };
    Ok(match condition {
        BatchCond::Ok { step } => BatchCondition::Ok(step),
        BatchCond::Error { step } => BatchCondition::Error(step),
        BatchCond::Not { cond } => {
            BatchCondition::Not(Box::new(self::condition(*cond, allows_autocommit, next)?))
        }
        BatchCond::And { conds } => BatchCondition::And(all(conds)?),
        BatchCond::Or { conds } => BatchCondition::Or(all(conds)?),
        BatchCond::IsAutocommit if allows_autocommit => BatchCondition::IsAutocommit,
        BatchCond::IsAutocommit => {
            return Err(Error::new(
                code::PROTOCOL_ERROR,
                "the is_autocommit condition is Hrana 3; use /v3/pipeline",
            ));
        }
    })
}

/// A step for the engine, from a resolved statement and condition.
pub(crate) fn step(statement: SessionStatement, condition: Option<BatchCondition>) -> EngineStep {
    EngineStep {
        condition,
        statement,
    }
}

/// Hrana's statement result for a node's.
pub(crate) fn stmt_result(result: proto::StatementResult) -> StmtResult {
    StmtResult {
        cols: result.columns.into_iter().map(col).collect(),
        rows: result
            .rows
            .into_iter()
            .map(|row| row.values.into_iter().map(value_from_proto).collect())
            .collect(),
        affected_row_count: result.affected_row_count,
        last_insert_rowid: result.last_insert_rowid.map(|rowid| rowid.to_string()),
    }
}

/// Hrana's column for a node's.
pub(crate) fn col(column: proto::SessionColumn) -> Col {
    Col {
        name: column.name,
        decltype: column.decltype,
    }
}

/// Hrana's error for a node's.
pub(crate) fn error(error: proto::SessionError) -> Error {
    Error {
        message: error.message,
        code: Some(error.code),
    }
}

/// Hrana's batch result for a node's.
pub(crate) fn batch_result(result: proto::BatchResult) -> BatchResult {
    let mut step_results = Vec::with_capacity(result.steps.len());
    let mut step_errors = Vec::with_capacity(result.steps.len());
    for step in result.steps {
        match step.step {
            Some(Step::Ok(outcome)) => {
                step_results.push(Some(stmt_result(outcome)));
                step_errors.push(None);
            }
            Some(Step::Error(failure)) => {
                step_results.push(None);
                step_errors.push(Some(error(failure)));
            }
            Some(Step::Skipped(_)) | None => {
                step_results.push(None);
                step_errors.push(None);
            }
        }
    }
    BatchResult {
        step_results,
        step_errors,
    }
}

/// Hrana's description for a node's.
pub(crate) fn describe_result(described: proto::Described) -> DescribeResult {
    DescribeResult {
        params: described
            .params
            .into_iter()
            .map(|param| DescribeParam { name: param.name })
            .collect(),
        cols: described.columns.into_iter().map(col).collect(),
        is_explain: described.is_explain,
        is_readonly: described.is_readonly,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn requests_decode_as_the_specification_shows_them() {
        let request: PipelineRequest = serde_json::from_value(json!({
            "baton": null,
            "requests": [
                {"type": "execute", "stmt": {
                    "sql": "SELECT ?, :a",
                    "args": [{"type": "integer", "value": "-9007199254740993"}],
                    "named_args": [{"name": ":a", "value": {"type": "blob", "base64": "AQI="}}],
                    "want_rows": false,
                    "replication_index": "7"
                }},
                {"type": "batch", "batch": {"steps": [
                    {"stmt": {"sql": "BEGIN"}},
                    {"condition": {"type": "not", "cond": {"type": "and", "conds": [
                        {"type": "ok", "step": 0}, {"type": "is_autocommit"}]}},
                     "stmt": {"sql_id": 3}}
                ]}},
                {"type": "sequence", "sql": "SELECT 1; SELECT 2"},
                {"type": "describe", "sql_id": 3},
                {"type": "store_sql", "sql_id": 3, "sql": "SELECT 1"},
                {"type": "close_sql", "sql_id": 3},
                {"type": "get_autocommit"},
                {"type": "close"}
            ]
        }))
        .expect("valid");
        assert_eq!(request.requests.len(), 8);
        let StreamRequest::Execute { stmt } = &request.requests[0] else {
            panic!("an execute: {:?}", request.requests[0]);
        };
        let statement = statement(stmt.clone(), "SELECT ?, :a".to_owned()).expect("valid");
        assert_eq!(statement.args, [SqlValue::Integer(-9_007_199_254_740_993)]);
        assert_eq!(
            statement.named_args,
            [(":a".to_owned(), SqlValue::Blob(vec![1, 2]))]
        );
        assert_eq!(statement.want_rows, WantRows::NoRows);
        let unknown = serde_json::from_value::<PipelineRequest>(json!({
            "baton": null, "requests": [{"type": "open_cursor"}]
        }));
        assert!(unknown.is_err(), "an unknown request type is refused");
    }

    #[test]
    fn integers_are_decimal_strings_and_blobs_unpadded() {
        let values = [
            Value::Null,
            Value::Integer { value: i64::MIN },
            Value::Float { value: 1.5 },
            Value::Text {
                value: "hé".to_owned(),
            },
        ];
        let encoded = serde_json::to_value(&values).expect("encodes");
        assert_eq!(
            encoded,
            json!([
                {"type": "null"},
                {"type": "integer", "value": "-9223372036854775808"},
                {"type": "float", "value": 1.5},
                {"type": "text", "value": "hé"}
            ])
        );
        let blob = value_from_proto(proto::Value {
            value: Some(Kind::Blob(vec![1, 2])),
        });
        assert_eq!(
            serde_json::to_value(&blob).expect("encodes"),
            json!({"type": "blob", "base64": "AQI"})
        );
        for padded in ["AQI=", "AQI"] {
            let decoded = value_to_sql(Value::Blob {
                base64: padded.to_owned(),
            });
            assert_eq!(
                decoded.expect("valid"),
                SqlValue::Blob(vec![1, 2]),
                "{padded}"
            );
        }
        let lenient: Value =
            serde_json::from_value(json!({"type": "integer", "value": 5})).expect("a number");
        assert_eq!(lenient, Value::Integer { value: 5 });
        let refused = serde_json::from_value::<Value>(json!({"type": "integer", "value": "1.5"}));
        assert!(refused.is_err());
    }

    #[test]
    fn responses_encode_as_the_specification_shows_them() {
        let response = PipelineResponse {
            baton: Some("b".to_owned()),
            base_url: None,
            results: vec![
                StreamResult::Ok {
                    response: StreamResponse::Execute {
                        result: StmtResult {
                            cols: vec![Col {
                                name: Some("id".to_owned()),
                                decltype: None,
                            }],
                            rows: vec![vec![Value::Integer { value: 1 }]],
                            affected_row_count: 1,
                            last_insert_rowid: Some("1".to_owned()),
                        },
                    },
                },
                StreamResult::Ok {
                    response: StreamResponse::GetAutocommit {
                        is_autocommit: true,
                    },
                },
                StreamResult::Error {
                    error: Error::new("SQLITE_ERROR", "no such table: t"),
                },
                StreamResult::Ok {
                    response: StreamResponse::Close,
                },
            ],
        };
        assert_eq!(
            serde_json::to_value(&response).expect("encodes"),
            json!({
                "baton": "b",
                "base_url": null,
                "results": [
                    {"type": "ok", "response": {"type": "execute", "result": {
                        "cols": [{"name": "id", "decltype": null}],
                        "rows": [[{"type": "integer", "value": "1"}]],
                        "affected_row_count": 1,
                        "last_insert_rowid": "1"
                    }}},
                    {"type": "ok", "response": {"type": "get_autocommit", "is_autocommit": true}},
                    {"type": "error", "error": {"message": "no such table: t", "code": "SQLITE_ERROR"}},
                    {"type": "ok", "response": {"type": "close"}}
                ]
            })
        );
        let entry = CursorEntry::StepEnd {
            affected_row_count: 0,
            last_insert_rowid: None,
        };
        assert_eq!(
            serde_json::to_value(&entry).expect("encodes"),
            json!({"type": "step_end", "affected_row_count": 0, "last_insert_rowid": null})
        );
    }

    #[test]
    fn conditions_are_bounded_and_versioned() {
        let mut deep = BatchCond::Ok { step: 0 };
        for _ in 0..40 {
            deep = BatchCond::Not {
                cond: Box::new(deep),
            };
        }
        assert!(condition(deep, true, 32).is_err());
        assert!(condition(BatchCond::IsAutocommit, false, 32).is_err());
        assert_eq!(
            condition(BatchCond::IsAutocommit, true, 32).expect("v3"),
            BatchCondition::IsAutocommit
        );
    }
}
