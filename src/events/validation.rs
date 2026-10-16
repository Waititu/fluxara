use chrono::SecondsFormat;

use super::model::{AcceptedEvent, EventSubmission};

/// Maximum length of a client-supplied event ID, in characters.
pub const MAX_EVENT_ID_LEN: usize = 128;
/// Maximum length of an event type (after trimming), in characters.
pub const MAX_EVENT_TYPE_LEN: usize = 128;
/// The only event schema version accepted by this phase.
pub const SUPPORTED_SCHEMA_VERSION: u32 = 1;
/// Maximum serialized size of an event `payload`, in bytes.
pub const MAX_PAYLOAD_BYTES: usize = 256 * 1024;
/// Maximum number of events accepted in a single batch request.
pub const MAX_BATCH_EVENTS: usize = 100;
/// Maximum request body size for event endpoints, in bytes.
pub const MAX_REQUEST_BYTES: usize = 1024 * 1024;

/// Validation failure that maps to HTTP 422.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidEvent {
    pub message: String,
}

/// Limit failure that maps to HTTP 413.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventTooLarge {
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventValidationError {
    Invalid(InvalidEvent),
    TooLarge(EventTooLarge),
}

impl EventValidationError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(InvalidEvent {
            message: message.into(),
        })
    }

    fn too_large(message: impl Into<String>) -> Self {
        Self::TooLarge(EventTooLarge {
            message: message.into(),
        })
    }
}

impl std::fmt::Display for EventValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(inner) => write!(f, "{}", inner.message),
            Self::TooLarge(inner) => write!(f, "{}", inner.message),
        }
    }
}

/// Validated, normalized event content ready to be stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedEvent {
    pub id: String,
    pub event_type: String,
    pub schema_version: u32,
    pub occurred_at: String,
    pub payload: serde_json::Value,
}

impl ValidatedEvent {
    pub fn into_accepted(self, received_at: String) -> AcceptedEvent {
        AcceptedEvent {
            id: self.id,
            event_type: self.event_type,
            schema_version: self.schema_version,
            occurred_at: self.occurred_at,
            payload: self.payload,
            received_at,
        }
    }
}

fn is_valid_id_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':')
}

/// Validate a client-supplied event ID.
///
/// Rules: 1-128 characters, ASCII alphanumeric plus `.` `_` `-` `:`,
/// and must start with an ASCII alphanumeric character.
pub fn validate_id(value: &serde_json::Value) -> Result<String, EventValidationError> {
    let id = value
        .as_str()
        .ok_or_else(|| EventValidationError::invalid("`id` must be a string".to_owned()))?;
    if id.is_empty() {
        return Err(EventValidationError::invalid("`id` must not be empty"));
    }
    if id.len() > MAX_EVENT_ID_LEN {
        return Err(EventValidationError::invalid(format!(
            "`id` must be at most {MAX_EVENT_ID_LEN} characters"
        )));
    }
    let bytes = id.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() {
        return Err(EventValidationError::invalid(
            "`id` must start with a letter or digit",
        ));
    }
    if !bytes.iter().all(|b| is_valid_id_char(*b)) {
        return Err(EventValidationError::invalid(
            "`id` may only contain letters, digits, '.', '_', '-' and ':'",
        ));
    }
    Ok(id.to_owned())
}

/// Validate an event type: trimmed, non-empty, at most 128 characters.
pub fn validate_event_type(value: &serde_json::Value) -> Result<String, EventValidationError> {
    let raw = value
        .as_str()
        .ok_or_else(|| EventValidationError::invalid("`event_type` must be a string".to_owned()))?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(EventValidationError::invalid(
            "`event_type` must not be blank",
        ));
    }
    if trimmed.len() > MAX_EVENT_TYPE_LEN {
        return Err(EventValidationError::invalid(format!(
            "`event_type` must be at most {MAX_EVENT_TYPE_LEN} characters"
        )));
    }
    Ok(trimmed.to_owned())
}

/// Validate the schema version: must be the integer 1.
pub fn validate_schema_version(value: &serde_json::Value) -> Result<u32, EventValidationError> {
    let version = value.as_u64().ok_or_else(|| {
        EventValidationError::invalid("`schema_version` must be the integer 1".to_owned())
    })?;
    if version != u64::from(SUPPORTED_SCHEMA_VERSION) {
        return Err(EventValidationError::invalid(format!(
            "unsupported `schema_version`; only version {SUPPORTED_SCHEMA_VERSION} is accepted"
        )));
    }
    Ok(SUPPORTED_SCHEMA_VERSION)
}

/// Validate an occurrence timestamp and normalize it to UTC RFC 3339.
///
/// Accepts any valid RFC 3339 timestamp with a timezone offset.
pub fn validate_occurred_at(value: &serde_json::Value) -> Result<String, EventValidationError> {
    let raw = value.as_str().ok_or_else(|| {
        EventValidationError::invalid(
            "`occurred_at` must be an RFC 3339 timestamp string".to_owned(),
        )
    })?;
    let parsed = chrono::DateTime::parse_from_rfc3339(raw).map_err(|_| {
        EventValidationError::invalid("`occurred_at` must be a valid RFC 3339 timestamp")
    })?;
    Ok(parsed
        .with_timezone(&chrono::Utc)
        .to_rfc3339_opts(SecondsFormat::AutoSi, true))
}

/// Validate payload presence and size.
///
/// Any JSON value is accepted; its re-serialized size must not exceed
/// [`MAX_PAYLOAD_BYTES`].
pub fn validate_payload(
    value: &serde_json::Value,
) -> Result<serde_json::Value, EventValidationError> {
    if value.is_null() {
        // `payload` deserializes a missing field as an error before we get
        // here; an explicit `null` is structurally present but invalid.
        return Err(EventValidationError::invalid("`payload` must be present"));
    }
    let size = serde_json::to_vec(value)
        .map_err(|_| EventValidationError::invalid("`payload` must be valid JSON"))?;
    if size.len() > MAX_PAYLOAD_BYTES {
        return Err(EventValidationError::too_large(format!(
            "`payload` exceeds the limit of {MAX_PAYLOAD_BYTES} bytes"
        )));
    }
    Ok(value.clone())
}

/// Validate a full event submission and return normalized content.
pub fn validate_submission(
    submission: &EventSubmission,
) -> Result<ValidatedEvent, EventValidationError> {
    Ok(ValidatedEvent {
        id: validate_id(&submission.id)?,
        event_type: validate_event_type(&submission.event_type)?,
        schema_version: validate_schema_version(&submission.schema_version)?,
        occurred_at: validate_occurred_at(&submission.occurred_at)?,
        payload: validate_payload(&submission.payload)?,
    })
}

/// Current UTC time formatted as RFC 3339, for `received_at`.
pub fn now_utc_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_EVENT_ID_LEN, MAX_EVENT_TYPE_LEN, MAX_PAYLOAD_BYTES, validate_event_type, validate_id,
        validate_occurred_at, validate_payload, validate_schema_version,
    };
    use serde_json::json;

    #[test]
    fn accepts_a_well_formed_id() {
        assert_eq!(
            validate_id(&json!("evt_01HXYZ-abc:def.ghi")).unwrap(),
            "evt_01HXYZ-abc:def.ghi"
        );
    }

    #[test]
    fn rejects_empty_and_blank_ids() {
        assert!(validate_id(&json!("")).is_err());
        assert!(validate_id(&json!("   ")).is_err());
    }

    #[test]
    fn rejects_ids_with_invalid_characters_or_leading_punctuation() {
        assert!(validate_id(&json!("-evt-1")).is_err());
        assert!(validate_id(&json!("evt 1")).is_err());
        assert!(validate_id(&json!("evt/1")).is_err());
    }

    #[test]
    fn rejects_oversized_ids() {
        let long = "a".repeat(MAX_EVENT_ID_LEN + 1);
        assert!(validate_id(&json!(long)).is_err());
    }

    #[test]
    fn rejects_non_string_ids() {
        assert!(validate_id(&json!(42)).is_err());
        assert!(validate_id(&json!(null)).is_err());
    }

    #[test]
    fn trims_event_type_and_rejects_blank_or_oversized_values() {
        assert_eq!(
            validate_event_type(&json!("  order.created  ")).unwrap(),
            "order.created"
        );
        assert!(validate_event_type(&json!("")).is_err());
        assert!(validate_event_type(&json!("   ")).is_err());
        let long = "t".repeat(MAX_EVENT_TYPE_LEN + 1);
        assert!(validate_event_type(&json!(long)).is_err());
    }

    #[test]
    fn only_schema_version_one_is_supported() {
        assert_eq!(validate_schema_version(&json!(1)).unwrap(), 1);
        assert!(validate_schema_version(&json!(0)).is_err());
        assert!(validate_schema_version(&json!(2)).is_err());
        assert!(validate_schema_version(&json!("1")).is_err());
    }

    #[test]
    fn accepts_rfc3339_timestamps_and_normalizes_to_utc() {
        let normalized = validate_occurred_at(&json!("2026-10-16T14:00:00+02:00")).unwrap();
        assert_eq!(normalized, "2026-10-16T12:00:00Z");
    }

    #[test]
    fn rejects_invalid_timestamps() {
        assert!(validate_occurred_at(&json!("not-a-timestamp")).is_err());
        assert!(validate_occurred_at(&json!("2026-10-16 12:00:00")).is_err());
        assert!(validate_occurred_at(&json!(123)).is_err());
    }

    #[test]
    fn rejects_oversized_payloads() {
        let big = "x".repeat(MAX_PAYLOAD_BYTES + 1);
        assert!(validate_payload(&json!({"data": big})).is_err());
        assert!(validate_payload(&json!(null)).is_err());
    }
}
