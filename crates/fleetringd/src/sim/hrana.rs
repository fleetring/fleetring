//! A simulated client's writes and reads through the router, as Hrana pipelines over HTTP/1.1 on
//! turmoil's network (ADR-0037).
//!
//! Each operation is one pipeline on a new stream that closes itself: `execute`, then `close`.
//! A write's acknowledgement comes from the response's `fleetring-*` headers, which name the
//! primary and the epoch, so the history checker holds the router to one primary per epoch.
//!
//! An answer the router gives only for a request that never ran (`UNAVAILABLE`,
//! `RESOURCE_EXHAUSTED`, a refusal, a SQLite error) is a rejection, which the checker holds to
//! "no effect"; one that leaves the outcome unknown (`FLEETRING_OUTCOME_UNKNOWN`,
//! `FLEETRING_UNACKNOWLEDGED`, an interrupted statement, a broken connection) is indeterminate.

use fleetring_control::{DatabaseName, LeaseEpoch};
use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper::{HeaderMap, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value as Json, json};
use tonic::Code;

use crate::sim::client::Via;
use crate::sim::workload::{Ack, Outcome, WRITES_TABLE};

/// Inserts `value` into `database`'s writes table through the router.
pub(super) async fn write(via: &Via, database: &DatabaseName, value: i64) -> Outcome {
    let statement = json!({
        "sql": format!("INSERT INTO {WRITES_TABLE} (value) VALUES (?)"),
        "args": [{"type": "integer", "value": value.to_string()}],
        "want_rows": false,
    });
    let (headers, result) = match pipeline(via, database, statement).await {
        Ok(answered) => answered,
        Err(outcome) => return outcome,
    };
    let number = |name: &str| -> Option<u64> { headers.get(name)?.to_str().ok()?.parse().ok() };
    let primary = headers
        .get("fleetring-primary")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<fleetring::NodeId>().ok());
    let committed = (
        primary,
        number("fleetring-epoch"),
        number("fleetring-acknowledged-by").and_then(|n| u32::try_from(n).ok()),
        number("fleetring-replicas").and_then(|n| u32::try_from(n).ok()),
    );
    match (result["type"].as_str(), committed) {
        (Some("ok"), (Some(primary), Some(epoch), Some(acknowledged_by), Some(replicas))) => {
            Outcome::Acknowledged(Ack::Written {
                acknowledged_by,
                replicas,
                under_replicated: acknowledged_by < replicas,
                primary: Some(primary),
                epoch: LeaseEpoch::new(epoch),
            })
        }
        // An insert that ran commits; one that ran without saying where is a broken answer.
        (Some("ok"), _) => Outcome::Indeterminate {
            code: Some(Code::Internal),
        },
        _ => refused(&result),
    }
}

/// Reads `database`'s writes table through the router: every value, or whether `value` is there.
pub(super) async fn read(via: &Via, database: &DatabaseName, value: Option<i64>) -> Outcome {
    let statement = match value {
        Some(value) => json!({
            "sql": format!("SELECT value FROM {WRITES_TABLE} WHERE value = ?"),
            "args": [{"type": "integer", "value": value.to_string()}],
        }),
        None => json!({"sql": format!("SELECT value FROM {WRITES_TABLE}")}),
    };
    let (_, result) = match pipeline(via, database, statement).await {
        Ok(answered) => answered,
        Err(outcome) => return outcome,
    };
    if result["type"] != "ok" {
        return refused(&result);
    }
    let rows = result["response"]["result"]["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let values: Option<std::collections::BTreeSet<i64>> = rows
        .iter()
        .map(|row| row[0]["value"].as_str().and_then(|text| text.parse().ok()))
        .collect();
    match values {
        Some(values) => Outcome::Acknowledged(Ack::Read { values }),
        None => Outcome::Indeterminate {
            code: Some(Code::Internal),
        },
    }
}

/// Sends `[execute statement, close]` to the router for `database`, and returns the answer's
/// headers and the execute's result, or the outcome of a request that got no such answer.
async fn pipeline(
    via: &Via,
    database: &DatabaseName,
    statement: Json,
) -> Result<(HeaderMap, Json), Outcome> {
    let lost = |_| Outcome::Indeterminate {
        code: Some(Code::Unavailable),
    };
    let stream = turmoil::net::TcpStream::connect(via.address.as_str())
        .await
        // Nothing was sent: the router was never reached.
        .map_err(|_| Outcome::Rejected {
            code: Code::Unavailable,
            message: format!("can't reach the router at {}", via.address),
        })?;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|_| Outcome::Rejected {
            code: Code::Unavailable,
            message: "the router's connection failed".to_owned(),
        })?;
    tokio::spawn(connection);
    let body = json!({
        "baton": null,
        "requests": [{"type": "execute", "stmt": statement}, {"type": "close"}],
    });
    let bytes = serde_json::to_vec(&body).map_err(|_| Outcome::Indeterminate { code: None })?;
    let request = Request::post(format!("/db/{database}/v3/pipeline"))
        .header("host", via.address.as_str())
        .header("authorization", format!("Bearer {}", via.token))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(bytes)))
        .map_err(|_| Outcome::Indeterminate { code: None })?;
    let response = sender.send_request(request).await.map_err(lost)?;
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(lost)?
        .to_bytes();
    let answer: Json = serde_json::from_slice(&body).unwrap_or(Json::Null);
    if status != StatusCode::OK {
        // A refused request ran nothing.
        return Err(Outcome::Rejected {
            code: match status {
                StatusCode::UNAUTHORIZED => Code::Unauthenticated,
                StatusCode::FORBIDDEN => Code::PermissionDenied,
                StatusCode::NOT_FOUND => Code::NotFound,
                StatusCode::SERVICE_UNAVAILABLE => Code::ResourceExhausted,
                _ => Code::InvalidArgument,
            },
            message: answer["message"].as_str().unwrap_or_default().to_owned(),
        });
    }
    Ok((headers, answer["results"][0].clone()))
}

/// The outcome of a request whose result is an error.
fn refused(result: &Json) -> Outcome {
    let code = result["error"]["code"].as_str().unwrap_or_default();
    let message = result["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let rejected = |code| Outcome::Rejected {
        code,
        message: message.clone(),
    };
    match code {
        // The router says these ran nothing.
        "UNAVAILABLE" => rejected(Code::Unavailable),
        "RESOURCE_EXHAUSTED" => rejected(Code::ResourceExhausted),
        "PERMISSION_DENIED" => rejected(Code::PermissionDenied),
        "NOT_FOUND" => rejected(Code::NotFound),
        "UNAUTHENTICATED" => rejected(Code::Unauthenticated),
        "PROTOCOL_ERROR" => rejected(Code::InvalidArgument),
        code if code.starts_with("SQLITE_") => rejected(Code::InvalidArgument),
        // What ran, if anything, is unknown.
        "FLEETRING_OUTCOME_UNKNOWN" | "FLEETRING_UNACKNOWLEDGED" => Outcome::Indeterminate {
            code: Some(Code::Aborted),
        },
        _ => Outcome::Indeterminate {
            code: Some(Code::Unknown),
        },
    }
}
