//! The object-store conformance suite, which every store passes, and the stores' own rules.

use std::num::NonZeroU32;
use std::sync::Arc;

use fleetringd::durability::{
    DirectoryStore, FaultyStore, MemoryStore, ObjectEntry, ObjectStore, StoreError,
};

use crate::common::TestResult;

fn entry(key: &str, size: u64) -> ObjectEntry {
    ObjectEntry {
        key: key.to_owned(),
        size,
    }
}

/// What every [`ObjectStore`] does, run against `store`, which holds nothing under `root`.
pub(crate) async fn conformance(store: Arc<dyn ObjectStore>, root: &str) -> TestResult {
    let key = |rest: &str| format!("{root}/{rest}");
    assert_eq!(store.get(&key("missing")).await?, None);
    assert_eq!(store.list(&key("")).await?, []);

    store.put(&key("a/one"), b"first".to_vec()).await?;
    assert_eq!(store.get(&key("a/one")).await?, Some(b"first".to_vec()));
    store.put(&key("a/two"), Vec::new()).await?;
    store.put(&key("a/three/deep"), vec![7; 1000]).await?;
    store.put(&key("ab"), b"x".to_vec()).await?;
    store.put(&key("b/one"), b"other".to_vec()).await?;

    // A put under one key leaves the others as they were; a second put replaces the object.
    store.put(&key("a/one"), b"replaced".to_vec()).await?;
    assert_eq!(store.get(&key("a/one")).await?, Some(b"replaced".to_vec()));
    assert_eq!(store.get(&key("a/two")).await?, Some(Vec::new()));
    assert_eq!(store.get(&key("b/one")).await?, Some(b"other".to_vec()));

    // A list returns exactly the keys under the prefix, in key order, with their sizes.
    assert_eq!(
        store.list(&key("a/")).await?,
        [
            entry(&key("a/one"), 8),
            entry(&key("a/three/deep"), 1000),
            entry(&key("a/two"), 0),
        ]
    );
    assert_eq!(
        store.list(&key("a")).await?,
        [
            entry(&key("a/one"), 8),
            entry(&key("a/three/deep"), 1000),
            entry(&key("a/two"), 0),
            entry(&key("ab"), 1),
        ]
    );
    assert_eq!(store.list(&key("a/t")).await?.len(), 2);
    assert_eq!(store.list(&key("c/")).await?, []);
    // A key's path that holds other objects holds no object itself.
    assert_eq!(store.get(&key("a/three")).await?, None);

    store.delete(&key("a/one")).await?;
    assert_eq!(store.get(&key("a/one")).await?, None);
    store.delete(&key("a/one")).await?;
    store.delete(&key("never")).await?;
    assert_eq!(
        store.list(&key("a/")).await?,
        [entry(&key("a/three/deep"), 1000), entry(&key("a/two"), 0)]
    );

    for bad in ["", "a//b", "a/../b", "./a", ".hidden", "a/"] {
        let refused = store.put(bad, Vec::new()).await;
        assert!(
            matches!(refused, Err(StoreError::InvalidKey { .. })),
            "put {bad:?}: {refused:?}"
        );
        assert!(
            matches!(store.get(bad).await, Err(StoreError::InvalidKey { .. })),
            "get {bad:?}"
        );
    }
    for key in ["a/one", "a/two", "a/three/deep", "ab", "b/one"] {
        store.delete(&format!("{root}/{key}")).await?;
    }
    assert_eq!(store.list(&format!("{root}/")).await?, []);
    Ok(())
}

#[tokio::test]
async fn memory_store_conforms() -> TestResult {
    conformance(Arc::new(MemoryStore::new()), "suite").await
}

#[tokio::test]
async fn directory_store_conforms() -> TestResult {
    let dir = tempfile::tempdir()?;
    conformance(
        Arc::new(DirectoryStore::new(dir.path().join("store"))),
        "suite",
    )
    .await
}

#[tokio::test]
async fn directory_store_refuses_dot_components() -> TestResult {
    let dir = tempfile::tempdir()?;
    let store = DirectoryStore::new(dir.path().join("store"));
    for key in ["../escape", "a/../../escape", "a/./b", ".tmp"] {
        assert!(
            matches!(
                store.put(key, b"x".to_vec()).await,
                Err(StoreError::InvalidKey { .. })
            ),
            "{key}"
        );
    }
    assert!(!dir.path().join("escape").exists());
    // A temporary file a crash left behind is no object.
    store.put("a/b", b"x".to_vec()).await?;
    std::fs::write(dir.path().join("store/a/.b.7.tmp"), b"partial")?;
    assert_eq!(store.list("a/").await?, [entry("a/b", 1)]);
    Ok(())
}

#[tokio::test]
async fn faulty_store_fails_exactly_every_nth_call() -> TestResult {
    let inner = MemoryStore::new();
    let store = FaultyStore::new(
        Arc::new(inner.clone()),
        NonZeroU32::new(3).ok_or("nonzero")?,
    );
    let mut failed = Vec::new();
    for call in 1..=9_u32 {
        let result = match call % 4 {
            0 => store.put(&format!("k/{call}"), vec![1]).await,
            1 => store.get("k/1").await.map(drop),
            2 => store.list("k/").await.map(drop),
            _ => store.delete("k/x").await,
        };
        if let Err(error) = result {
            assert!(error.is_transient(), "{error}");
            failed.push(call);
        }
    }
    assert_eq!(failed, [3, 6, 9]);
    // A failed put never reached the store; the others did.
    assert_eq!(inner.list("k/").await?, [entry("k/4", 1), entry("k/8", 1)]);
    Ok(())
}
