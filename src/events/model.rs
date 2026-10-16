use serde::{Deserialize, Serialize};

/// Client-supplied event submission (versioned `v1` envelope).
///
/// JSON contract:
/// ```json
/// {
///   "id": "evt_01HXYZ",
///   "event_type": "order.created",
///   "schema_version": 1,
///   "occurred_at": "2026-10-16T12:00:00Z",
///   "payload": { "order_id": "123" }
/// }
/// ```
///
/// Unknown fields are rejected so the versioned contract stays strict.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EventSubmission {
    pub id: serde_json::Value,
    pub event_type: serde_json::Value,
    pub schema_version: serde_json::Value,
    pub occurred_at: serde_json::Value,
    pub payload: serde_json::Value,
}

/// Envelope used for `POST /v1/events/batch`.
///
/// ```json
/// { "events": [ { "id": "...", /* ... */ } ] }
/// ```
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchRequest {
    pub events: serde_json::Value,
}

/// Accepted (stored) representation of an event.
///
/// `received_at` is assigned by the server at acceptance time and is
/// excluded from idempotency comparisons: two submissions are identical
/// when `id`, `event_type`, `schema_version`, `occurred_at` (normalized
/// to UTC RFC 3339), and `payload` are all equal.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AcceptedEvent {
    pub id: String,
    pub event_type: String,
    pub schema_version: u32,
    pub occurred_at: String,
    pub payload: serde_json::Value,
    pub received_at: String,
}

impl AcceptedEvent {
    /// Semantic content equality used for idempotency and conflict checks.
    ///
    /// `received_at` is intentionally excluded: it is server-assigned and
    /// differs between the original acceptance and any replay.
    pub fn content_eq(&self, other: &AcceptedEvent) -> bool {
        self.id == other.id
            && self.event_type == other.event_type
            && self.schema_version == other.schema_version
            && self.occurred_at == other.occurred_at
            && self.payload == other.payload
    }
}
