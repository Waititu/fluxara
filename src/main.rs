use std::error::Error;

use fluxara::{api::router, config::Config};
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("fluxara=info,tower_http=info")),
        )
        .init();

    let config = Config::from_env()?;
    let listener = TcpListener::bind(config.bind_addr).await?;

    info!(
        address = %listener.local_addr()?,
        "Fluxara API starting"
    );

    axum::serve(listener, router())
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
