//! The self-replicating test engine: proof that nothing above the trait assumes a WAL.

use std::time::Duration;

use fleetring::NodeId;
use fleetringd::engine::{
    EngineConfig, MemoryEngine, Position, ReplicaPosition, Statement, Transaction, Value,
};
use fleetringd::{DatabaseError, RecordingSink, Registry};

use crate::common::{TestResult, name};

fn node(id: &str) -> NodeId {
    id.parse().expect("valid node id")
}

#[tokio::test]
async fn a_self_replicating_engine_reports_replicas_and_publishes_no_batches() -> TestResult {
    let sink = RecordingSink::new();
    let engine = MemoryEngine::with_replicas([node("n1"), node("n2")]);
    let registry = Registry::new(engine, sink.clone(), "unused", EngineConfig::default());
    let db = name("tenant");
    let handle = registry.create(&db).await?;
    assert_eq!(handle.position().await?, None);
    for i in 0..3 {
        handle
            .execute(Transaction::new([
                Statement::with_params("put", [Value::from("k"), Value::Integer(i)]),
                Statement::with_params("put", [Value::from("other"), Value::Integer(i)]),
            ]))
            .await?;
    }
    let position = Some(Position::new(3_u64.to_be_bytes()));
    assert_eq!(handle.position().await?, position);
    assert_eq!(
        handle.replica_positions().await?,
        vec![
            ReplicaPosition {
                node: node("n1"),
                position: position.clone()
            },
            ReplicaPosition {
                node: node("n2"),
                position: position.clone()
            },
        ]
    );
    assert!(sink.batches().is_empty(), "the engine replicates itself");

    let unsupported = handle
        .checkpoint()
        .await
        .expect_err("no frames to checkpoint");
    assert!(
        matches!(unsupported, DatabaseError::Unsupported { .. }),
        "{unsupported:?}"
    );
    let rows = handle.query(Statement::new("scan")).await?;
    assert_eq!(rows.rows.len(), 2);
    Ok(())
}

#[tokio::test]
async fn snapshots_copy_the_state_and_its_position() -> TestResult {
    let engine = MemoryEngine::new();
    let config = EngineConfig {
        idle_timeout: Duration::from_secs(60),
        ..EngineConfig::default()
    };
    let registry = Registry::new(engine, RecordingSink::new(), "root", config);
    let db = name("source");
    let handle = registry.create(&db).await?;
    handle
        .execute(Transaction::from(Statement::with_params(
            "put",
            [Value::from("k"), Value::from("v")],
        )))
        .await?;
    let snapshot = handle.snapshot("root/copy").await?;
    assert_eq!(snapshot.position, handle.position().await?);
    let copy = registry.open(&name("copy")).await?;
    let rows = copy
        .query(Statement::with_params("get", [Value::from("k")]))
        .await?;
    assert_eq!(rows.rows, vec![vec![Value::from("v")]]);
    Ok(())
}
