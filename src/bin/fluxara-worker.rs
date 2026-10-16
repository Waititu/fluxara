use std::{error::Error, time::Duration};

use fluxara::events::PostgresJobQueue;
use sqlx::postgres::PgPoolOptions;
use tokio::time::sleep;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

const DEFAULT_DATABASE_URL: &str =
    "postgres://fluxara:fluxara_local_dev_only@127.0.0.1:55432/fluxara";

const LEASE_SECONDS: i64 = 30;
const POLL_INTERVAL: Duration = Duration::from_millis(500);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("fluxara=info")),
        )
        .init();

    let database_url =
        std::env::var("FLUXARA_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    let queue = PostgresJobQueue::new(pool);
    let worker_id = format!("worker-{}", std::process::id());

    info!(%worker_id, "Fluxara worker started");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Worker shutdown requested");
                break;
            }
            result = queue.claim_one(&worker_id, LEASE_SECONDS) => {
                match result {
                    Ok(Some(job)) => {
                        info!(
                            event_id = %job.event_id,
                            event_type = %job.event_type,
                            attempt = job.attempts,
                            max_attempts = job.max_attempts,
                            "Processing event"
                        );

                        // Phase 5 uses a deterministic local handler.
                        // Replace this with a real consumer in a later phase.
                        //
                        // Do not log the event payload: it may contain sensitive data.
                        let _payload = job.payload;

                        match queue.mark_succeeded(&job.event_id, &worker_id).await {
                            Ok(()) => {
                                info!(event_id = %job.event_id, "Event processed");
                            }
                            Err(error) => {
                                error!(
                                    event_id = %job.event_id,
                                    %error,
                                    "Could not mark event as succeeded"
                                );
                            }
                        }
                    }
                    Ok(None) => sleep(POLL_INTERVAL).await,
                    Err(error) => {
                        error!(%error, "Failed to claim job");
                        sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }
    }

    info!("Fluxara worker stopped");
    Ok(())
}
