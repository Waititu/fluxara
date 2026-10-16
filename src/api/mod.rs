use axum::{Json, Router, routing::get};
use tower_http::{
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

use crate::{
    application::{HealthService, HealthSnapshot},
    error::AppError,
};

async fn health() -> Json<HealthSnapshot> {
    Json(HealthService.snapshot())
}

async fn not_found() -> AppError {
    AppError::NotFound
}

pub fn router() -> Router {
    Router::new()
        .route("/health", get(health))
        .fallback(not_found)
        .layer(TraceLayer::new_for_http())
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
}
