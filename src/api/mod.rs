pub mod events;

use axum::{Json, Router, routing::get};
use std::sync::Arc;
use tower_http::{
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};

use crate::{
    application::{HealthService, HealthSnapshot},
    error::AppError,
    events::{AsyncEventStore, InMemoryEventStore},
};

use self::events::AppState;

async fn health() -> Json<HealthSnapshot> {
    Json(HealthService.snapshot())
}

async fn not_found() -> AppError {
    AppError::NotFound
}

/// Base router with a fresh in-memory store.
///
/// Kept for backwards compatibility (existing callers and the health
/// contract); event routes share one store per router instance.
pub fn router() -> Router {
    router_with_store(InMemoryEventStore::new())
}

/// Router sharing the provided store, used by tests to isolate state.
pub fn router_with_store(store: InMemoryEventStore) -> Router {
    router_with_async_store(Arc::new(store))
}

/// Router using a shared asynchronous event store (production entry point).
pub fn router_with_async_store(store: Arc<dyn AsyncEventStore>) -> Router {
    let state = AppState::with_store(store);
    Router::new()
        .route("/health", get(health))
        .route("/v1/events", axum::routing::post(self::events::post_event))
        .route(
            "/v1/events/batch",
            axum::routing::post(self::events::post_batch),
        )
        .route("/v1/events/{id}", get(self::events::get_event))
        .fallback(not_found)
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
}
