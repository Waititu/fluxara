use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use super::model::AcceptedEvent;
use async_trait::async_trait;

/// Outcome of attempting to store an event under its client-supplied ID.
#[derive(Clone, Debug)]
pub enum StoreOutcome {
    /// The ID was seen for the first time; the contained event was stored.
    Accepted(AcceptedEvent),
    /// The ID was already stored with identical content (idempotent replay).
    Duplicate(AcceptedEvent),
    /// The ID was already stored with different content.
    Conflict(AcceptedEvent),
}

/// Storage abstraction for accepted events.
///
/// The in-memory implementation below backs local development and API
/// tests. Phase 4 will provide a PostgreSQL-backed implementation of this
/// same trait without changing the HTTP contract.
pub trait EventStore: Clone + Send + Sync + 'static {
    fn insert(&self, event: AcceptedEvent) -> StoreOutcome;
    fn get(&self, id: &str) -> Option<AcceptedEvent>;
}

/// Error returned by asynchronous event-store implementations.
///
/// The internal detail is retained for structured logs but is never returned
/// to API clients.
#[derive(Debug)]
pub struct StoreError(pub(crate) String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StoreError {}

impl From<sqlx::Error> for StoreError {
    fn from(error: sqlx::Error) -> Self {
        Self(error.to_string())
    }
}

/// Async interface used by HTTP handlers. Kept separate from `EventStore`
/// so existing synchronous in-memory tests and callers remain compatible.
#[async_trait]
pub trait AsyncEventStore: Send + Sync + 'static {
    async fn insert_async(&self, event: AcceptedEvent) -> Result<StoreOutcome, StoreError>;

    async fn get_async(&self, id: &str) -> Result<Option<AcceptedEvent>, StoreError>;
}

#[async_trait]
impl AsyncEventStore for InMemoryEventStore {
    async fn insert_async(&self, event: AcceptedEvent) -> Result<StoreOutcome, StoreError> {
        Ok(EventStore::insert(self, event))
    }

    async fn get_async(&self, id: &str) -> Result<Option<AcceptedEvent>, StoreError> {
        Ok(EventStore::get(self, id))
    }
}

/// Concurrency-safe in-memory event store.
///
/// A single write lock guards the check-then-insert sequence, so
/// concurrent submissions of the same ID serialize: exactly one wins as
/// `Accepted` and the others resolve deterministically to `Duplicate` or
/// `Conflict` based on a semantic content comparison.
#[derive(Clone, Debug, Default)]
pub struct InMemoryEventStore {
    inner: Arc<RwLock<HashMap<String, AcceptedEvent>>>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl EventStore for InMemoryEventStore {
    fn insert(&self, event: AcceptedEvent) -> StoreOutcome {
        let mut events = self
            .inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        match events.get(&event.id) {
            Some(existing) if existing.content_eq(&event) => {
                StoreOutcome::Duplicate(existing.clone())
            }
            Some(existing) => StoreOutcome::Conflict(existing.clone()),
            None => {
                events.insert(event.id.clone(), event.clone());
                StoreOutcome::Accepted(event)
            }
        }
    }

    fn get(&self, id: &str) -> Option<AcceptedEvent> {
        self.inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(id)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::{EventStore, InMemoryEventStore, StoreOutcome};
    use crate::events::model::AcceptedEvent;
    use serde_json::json;

    fn accepted(id: &str, event_type: &str) -> AcceptedEvent {
        AcceptedEvent {
            id: id.to_owned(),
            event_type: event_type.to_owned(),
            schema_version: 1,
            occurred_at: "2026-10-16T12:00:00Z".to_owned(),
            payload: json!({"order_id": "123"}),
            received_at: "2026-10-16T12:00:01Z".to_owned(),
        }
    }

    #[test]
    fn first_insert_is_accepted_and_readable() {
        let store = InMemoryEventStore::new();
        let event = accepted("evt-1", "order.created");

        assert!(matches!(
            store.insert(event.clone()),
            StoreOutcome::Accepted(_)
        ));
        assert_eq!(store.get("evt-1"), Some(event));
        assert_eq!(store.get("unknown"), None);
    }

    #[test]
    fn identical_content_is_a_duplicate_and_keeps_the_original() {
        let store = InMemoryEventStore::new();
        let first = accepted("evt-1", "order.created");
        let replay = AcceptedEvent {
            received_at: "2026-10-16T12:05:00Z".to_owned(),
            ..first.clone()
        };

        assert!(matches!(store.insert(first), StoreOutcome::Accepted(_)));
        match store.insert(replay) {
            StoreOutcome::Duplicate(existing) => {
                assert_eq!(existing.received_at, "2026-10-16T12:00:01Z");
            }
            other => panic!("expected duplicate, got {other:?}"),
        }
    }

    #[test]
    fn different_content_is_a_conflict() {
        let store = InMemoryEventStore::new();

        assert!(matches!(
            store.insert(accepted("evt-1", "order.created")),
            StoreOutcome::Accepted(_)
        ));
        match store.insert(accepted("evt-1", "order.cancelled")) {
            StoreOutcome::Conflict(existing) => {
                assert_eq!(existing.event_type, "order.created");
            }
            other => panic!("expected conflict, got {other:?}"),
        }
    }
}
