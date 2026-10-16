use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::events::validation::EventValidationError;

#[derive(Debug)]
pub enum AppError {
    NotFound,
    EventNotFound,
    BadRequest(String),
    Unprocessable(String),
    Conflict,
    PayloadTooLarge(String),
    Internal,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorDetails,
}

#[derive(Serialize)]
struct ErrorDetails {
    code: &'static str,
    message: String,
}

impl AppError {
    pub fn malformed_json() -> Self {
        Self::BadRequest("Request body contains malformed JSON".to_owned())
    }

    pub fn validation(error: EventValidationError) -> Self {
        match error {
            EventValidationError::Invalid(invalid) => Self::Unprocessable(invalid.message.clone()),
            EventValidationError::TooLarge(too_large) => {
                Self::PayloadTooLarge(too_large.message.clone())
            }
        }
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "The requested resource was not found"),
            Self::EventNotFound => write!(f, "The requested event was not found"),
            Self::BadRequest(message)
            | Self::Unprocessable(message)
            | Self::PayloadTooLarge(message) => write!(f, "{message}"),
            Self::Conflict => {
                write!(f, "Event ID already exists with different content")
            }
            Self::Internal => write!(f, "An internal server error occurred"),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "The requested resource was not found".to_owned(),
            ),
            Self::EventNotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "The requested event was not found".to_owned(),
            ),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, "bad_request", message),
            Self::Unprocessable(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "unprocessable_entity",
                message,
            ),
            Self::Conflict => (
                StatusCode::CONFLICT,
                "conflict",
                "Event ID already exists with different content".to_owned(),
            ),
            Self::PayloadTooLarge(message) => {
                (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large", message)
            }
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "An internal server error occurred".to_owned(),
            ),
        };

        (
            status,
            Json(ErrorResponse {
                error: ErrorDetails { code, message },
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::AppError;
    use axum::{body::to_bytes, http::StatusCode, response::IntoResponse};

    #[tokio::test]
    async fn internal_error_does_not_expose_internal_details() {
        let response = AppError::Internal.into_response();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains("internal_error"));
        assert!(!body.contains("database password"));
    }

    #[tokio::test]
    async fn validation_errors_use_the_shared_envelope_without_leaking_payloads() {
        let response =
            AppError::Unprocessable("`event_type` must not be blank".to_owned()).into_response();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(payload["error"]["code"], "unprocessable_entity");
        assert!(
            payload["error"]["message"]
                .as_str()
                .unwrap()
                .contains("event_type")
        );
    }
}
