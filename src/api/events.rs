use axum::{
    Json,
    body::Bytes,
    extract::{Path, Request, State},
    http::StatusCode,
};
use serde::Serialize;
use std::sync::Arc;

use crate::{
    error::AppError,
    events::{
        AcceptedEvent, AsyncEventStore, BatchRequest, EventSubmission, InMemoryEventStore,
        MAX_BATCH_EVENTS, MAX_REQUEST_BYTES, StoreError, StoreOutcome,
        validation::{EventValidationError, now_utc_rfc3339, validate_submission},
    },
};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn AsyncEventStore>,
}

impl AppState {
    pub fn new(store: InMemoryEventStore) -> Self {
        Self {
            store: Arc::new(store),
        }
    }

    pub fn with_store(store: Arc<dyn AsyncEventStore>) -> Self {
        Self { store }
    }
}

#[derive(Debug, Serialize)]
pub struct BatchResult {
    pub id: String,
    pub status: u16,
    pub result: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<AcceptedEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<BatchError>,
}

#[derive(Debug, Serialize)]
pub struct BatchError {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct BatchResponse {
    pub results: Vec<BatchResult>,
}

fn store_error(error: StoreError) -> AppError {
    tracing::error!(error = %error, "event store operation failed");
    AppError::Internal
}

fn parse_error_to_app_error(error: serde_json::Error) -> AppError {
    if error.is_syntax() || error.is_eof() || error.is_io() {
        AppError::malformed_json()
    } else {
        AppError::Unprocessable(format!("Invalid event: {error}"))
    }
}

/// Read the request body with a hard limit enforced while collecting bytes.
///
/// This prevents the application from buffering an arbitrarily large body
/// before checking its size. The limit applies even when Content-Length is
/// missing or does not accurately describe the incoming body.
async fn read_bounded_body(request: Request) -> Result<Bytes, AppError> {
    use http_body_util::BodyExt;

    let mut body = request.into_body();
    let mut collected = Vec::new();

    while let Some(frame_result) = body.frame().await {
        let frame = frame_result
            .map_err(|_| AppError::BadRequest("Unable to read request body".to_owned()))?;

        if let Ok(data) = frame.into_data() {
            if data.len() > MAX_REQUEST_BYTES.saturating_sub(collected.len()) {
                return Err(AppError::PayloadTooLarge(format!(
                    "Request body exceeds the limit of {MAX_REQUEST_BYTES} bytes"
                )));
            }

            collected.extend_from_slice(&data);
        }
    }

    Ok(Bytes::from(collected))
}

fn parse_single_event(bytes: &Bytes) -> Result<EventSubmission, AppError> {
    serde_json::from_slice::<EventSubmission>(bytes).map_err(parse_error_to_app_error)
}

pub async fn post_event(
    State(state): State<AppState>,
    request: Request,
) -> Result<(StatusCode, Json<AcceptedEvent>), AppError> {
    let bytes = read_bounded_body(request).await?;
    let submission = parse_single_event(&bytes)?;
    let validated = validate_submission(&submission).map_err(AppError::validation)?;
    let accepted = validated.into_accepted(now_utc_rfc3339());

    match state
        .store
        .insert_async(accepted)
        .await
        .map_err(store_error)?
    {
        StoreOutcome::Accepted(event) => Ok((StatusCode::CREATED, Json(event))),
        StoreOutcome::Duplicate(event) => Ok((StatusCode::OK, Json(event))),
        StoreOutcome::Conflict(_) => Err(AppError::Conflict),
    }
}

pub async fn get_event(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<AcceptedEvent>, AppError> {
    state
        .store
        .get_async(&id)
        .await
        .map_err(store_error)?
        .map(Json)
        .ok_or(AppError::EventNotFound)
}

fn attempted_id(item: &serde_json::Value) -> String {
    item.get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn invalid_result(id: String, message: String) -> BatchResult {
    BatchResult {
        id,
        status: StatusCode::UNPROCESSABLE_ENTITY.as_u16(),
        result: "invalid",
        event: None,
        error: Some(BatchError {
            code: "unprocessable_entity",
            message,
        }),
    }
}

fn too_large_result(id: String, message: String) -> BatchResult {
    BatchResult {
        id,
        status: StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
        result: "too_large",
        event: None,
        error: Some(BatchError {
            code: "payload_too_large",
            message,
        }),
    }
}

pub async fn post_batch(
    State(state): State<AppState>,
    request: Request,
) -> Result<Json<BatchResponse>, AppError> {
    let bytes = read_bounded_body(request).await?;
    let request =
        serde_json::from_slice::<BatchRequest>(&bytes).map_err(parse_error_to_app_error)?;

    let events = match request.events {
        serde_json::Value::Array(events) => events,
        _ => {
            return Err(AppError::Unprocessable(
                "`events` must be an array".to_owned(),
            ));
        }
    };

    if events.is_empty() {
        return Err(AppError::Unprocessable(
            "`events` must contain at least one event".to_owned(),
        ));
    }

    if events.len() > MAX_BATCH_EVENTS {
        return Err(AppError::PayloadTooLarge(format!(
            "Batch exceeds the limit of {MAX_BATCH_EVENTS} events"
        )));
    }

    // Process sequentially in request order so duplicate IDs within a
    // batch resolve deterministically and previously accepted entries in
    // the same batch are visible to later entries. The batch is not
    // atomic: entries before a failure stay accepted.
    let mut results = Vec::with_capacity(events.len());

    for item in &events {
        let id = attempted_id(item);

        let submission: EventSubmission = match serde_json::from_value(item.clone()) {
            Ok(submission) => submission,
            Err(error) => {
                results.push(invalid_result(id, format!("Invalid event: {error}")));
                continue;
            }
        };

        let validated = match validate_submission(&submission) {
            Ok(validated) => validated,
            Err(EventValidationError::Invalid(invalid)) => {
                results.push(invalid_result(id, invalid.message));
                continue;
            }
            Err(EventValidationError::TooLarge(too_large)) => {
                results.push(too_large_result(id, too_large.message));
                continue;
            }
        };

        let accepted = validated.into_accepted(now_utc_rfc3339());

        match state
            .store
            .insert_async(accepted)
            .await
            .map_err(store_error)?
        {
            StoreOutcome::Accepted(event) => results.push(BatchResult {
                id,
                status: StatusCode::CREATED.as_u16(),
                result: "accepted",
                event: Some(event),
                error: None,
            }),
            StoreOutcome::Duplicate(event) => results.push(BatchResult {
                id,
                status: StatusCode::OK.as_u16(),
                result: "duplicate",
                event: Some(event),
                error: None,
            }),
            StoreOutcome::Conflict(_) => results.push(BatchResult {
                id,
                status: StatusCode::CONFLICT.as_u16(),
                result: "conflict",
                event: None,
                error: Some(BatchError {
                    code: "conflict",
                    message: "Event ID already exists with different content".to_owned(),
                }),
            }),
        }
    }

    Ok(Json(BatchResponse { results }))
}
