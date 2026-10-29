//! A sink that keeps every change in memory. For tests.

use std::sync::{Arc, Mutex};

use crate::changes::{Change, ChangeSink, Published};

/// A sink that records every change it is given and takes each one at once.
#[derive(Debug, Clone)]
pub struct MemorySink {
    name: String,
    changes: Arc<Mutex<Vec<Change>>>,
}

impl MemorySink {
    /// An empty sink named `name`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            changes: Arc::default(),
        }
    }

    /// Every change published so far, in publication order.
    ///
    /// # Panics
    ///
    /// If another thread panicked while publishing.
    #[must_use]
    pub fn changes(&self) -> Vec<Change> {
        self.changes.lock().expect("sink lock poisoned").clone()
    }
}

impl ChangeSink for MemorySink {
    fn name(&self) -> &str {
        &self.name
    }

    fn publish(&self, change: Change) -> Published {
        self.changes
            .lock()
            .expect("sink lock poisoned")
            .push(change);
        Box::pin(std::future::ready(Ok(())))
    }
}
