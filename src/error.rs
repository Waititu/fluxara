use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug)]
pub enum AppError {
    NotFound,
    Internal,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorDetails,
}

#[derive(Serialize)]
struct ErrorDetails {
    code: &'static str,
    message: &'static str,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "The requested resource was not found",
            ),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "An internal server error occurred",
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
}
