use axum::{Json, Router, routing::get};
use serde::Serialize;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Serialize, PartialEq)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    version: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "fluxara",
        version: env!("CARGO_PKG_VERSION"),
    })
}

fn app() -> Router {
    Router::new().route("/health", get(health))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("fluxara=info")),
        )
        .init();

    let listener = TcpListener::bind("127.0.0.1:3000").await?;

    info!(
        address = %listener.local_addr()?,
        "Fluxara API starting"
    );

    axum::serve(listener, app()).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_reports_ok() {
        let Json(response) = health().await;
        assert_eq!(response.status, "ok");
    }

    #[tokio::test]
    async fn health_identifies_fluxara() {
        let Json(response) = health().await;
        assert_eq!(response.service, "fluxara");
        assert_eq!(response.version, env!("CARGO_PKG_VERSION"));
    }
}
