use std::{error::Error, sync::Arc};

use fluxara::{api::router_with_async_store, config::Config, events::PostgresEventStore};
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_DATABASE_URL: &str =
    "postgres://fluxara:fluxara_local_dev_only@127.0.0.1:55432/fluxara";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("fluxara=info,tower_http=info")),
        )
        .init();

    let config = Config::from_env()?;
    let database_url =
        std::env::var("FLUXARA_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned());

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    info!("PostgreSQL connected and migrations applied");

    let store = Arc::new(PostgresEventStore::new(pool));
    let listener = TcpListener::bind(config.bind_addr).await?;

    info!(
        address = %listener.local_addr()?,
        "Fluxara API starting"
    );

    axum::serve(listener, router_with_async_store(store))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Fluxara API stopped");
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                warn!(%error, "failed to listen for Ctrl-C");
            }
        }
        _ = terminate => {}
    }

    info!("shutdown signal received");
}
