use std::{
    error::Error,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{Router, extract::State, routing::get};
use fluxara::events::{ClaimedJob, PostgresJobQueue};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::{task::JoinSet, time::sleep};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_DATABASE_URL: &str =
    "postgres://fluxara:fluxara_local_dev_only@127.0.0.1:55432/fluxara";

#[derive(Clone, Debug)]
struct Settings {
    concurrency: usize,
    lease_seconds: i64,
    poll_interval_ms: u64,
    retry_base_seconds: i64,
    retry_max_seconds: i64,
    metrics_addr: SocketAddr,
}

impl Settings {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        let concurrency = env_parse("FLUXARA_WORKER_CONCURRENCY", 4_usize)?;
        let lease_seconds = env_parse("FLUXARA_WORKER_LEASE_SECONDS", 30_i64)?;
        let poll_interval_ms = env_parse("FLUXARA_WORKER_POLL_INTERVAL_MS", 500_u64)?;
        let retry_base_seconds = env_parse("FLUXARA_RETRY_BASE_SECONDS", 2_i64)?;
        let retry_max_seconds = env_parse("FLUXARA_RETRY_MAX_SECONDS", 300_i64)?;
        let metrics_addr: SocketAddr = std::env::var("FLUXARA_METRICS_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:9464".to_owned())
            .parse()?;

        if !(1..=128).contains(&concurrency) {
            return Err("FLUXARA_WORKER_CONCURRENCY must be between 1 and 128".into());
        }
        if lease_seconds < 1 {
            return Err("FLUXARA_WORKER_LEASE_SECONDS must be at least 1".into());
        }
        if poll_interval_ms < 10 {
            return Err("FLUXARA_WORKER_POLL_INTERVAL_MS must be at least 10".into());
        }
        if retry_base_seconds < 1 || retry_max_seconds < retry_base_seconds {
            return Err(
                "retry settings must satisfy 1 <= FLUXARA_RETRY_BASE_SECONDS <= FLUXARA_RETRY_MAX_SECONDS"
                    .into(),
            );
        }

        Ok(Self {
            concurrency,
            lease_seconds,
            poll_interval_ms,
            retry_base_seconds,
            retry_max_seconds,
            metrics_addr,
        })
    }
}

fn env_parse<T>(name: &str, default: T) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
{
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(Box::new(error)),
    }
}

struct Metrics {
    pool: PgPool,
    claimed: AtomicU64,
    succeeded: AtomicU64,
    failed: AtomicU64,
    claim_errors: AtomicU64,
    ownership_errors: AtomicU64,
}

impl Metrics {
    fn new(pool: PgPool) -> Self {
        Self {
            pool,
            claimed: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            claim_errors: AtomicU64::new(0),
            ownership_errors: AtomicU64::new(0),
        }
    }
}

fn counter(metrics: &Metrics, name: &str) -> u64 {
    match name {
        "claimed" => metrics.claimed.load(Ordering::Relaxed),
        "succeeded" => metrics.succeeded.load(Ordering::Relaxed),
        "failed" => metrics.failed.load(Ordering::Relaxed),
        "claim_errors" => metrics.claim_errors.load(Ordering::Relaxed),
        "ownership_errors" => metrics.ownership_errors.load(Ordering::Relaxed),
        _ => 0,
    }
}

async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> String {
    let mut output = String::from(
        "# HELP fluxara_worker_jobs_total Jobs observed by this worker process.\n\
         # TYPE fluxara_worker_jobs_total counter\n",
    );

    for (name, help) in [
        ("claimed", "Jobs claimed"),
        ("succeeded", "Jobs successfully completed"),
        ("failed", "Jobs that failed processing"),
        ("claim_errors", "Database claim errors"),
        ("ownership_errors", "Job ownership or completion errors"),
    ] {
        output.push_str(&format!(
            "# HELP fluxara_worker_{name}_total {help}.\n\
             # TYPE fluxara_worker_{name}_total counter\n\
             fluxara_worker_{name}_total {}\n",
            counter(&metrics, name)
        ));
    }

    match sqlx::query_as::<_, (String, i64)>(
        "SELECT status, COUNT(*)::BIGINT FROM event_jobs GROUP BY status",
    )
    .fetch_all(&metrics.pool)
    .await
    {
        Ok(rows) => {
            output.push_str(
                "# HELP fluxara_queue_jobs Number of durable jobs by status.\n\
                 # TYPE fluxara_queue_jobs gauge\n",
            );
            for status in ["pending", "processing", "succeeded", "dead_letter"] {
                let count = rows
                    .iter()
                    .find(|(row_status, _)| row_status == status)
                    .map(|(_, count)| *count)
                    .unwrap_or(0);
                output.push_str(&format!(
                    "fluxara_queue_jobs{{status=\"{status}\"}} {count}\n"
                ));
            }
            output.push_str("fluxara_queue_metrics_up 1\n");
        }
        Err(error) => {
            warn!(%error, "Unable to collect PostgreSQL queue metrics");
            output.push_str("fluxara_queue_metrics_up 0\n");
        }
    }

    output
}

async fn process_event(_job: &ClaimedJob) -> Result<(), String> {
    // Phase 6 retains the deterministic local handler. In a later phase,
    // replace this function with the actual event consumer.
    //
    // A real consumer must make external side effects idempotent because
    // expired leases can cause at-least-once delivery.
    Ok(())
}

async fn run_job(
    queue: PostgresJobQueue,
    metrics: Arc<Metrics>,
    settings: Settings,
    worker_id: String,
    job: ClaimedJob,
) {
    info!(
        event_id = %job.event_id,
        event_type = %job.event_type,
        attempt = job.attempts,
        max_attempts = job.max_attempts,
        "Processing event"
    );

    match process_event(&job).await {
        Ok(()) => match queue.mark_succeeded(&job.event_id, &worker_id).await {
            Ok(()) => {
                metrics.succeeded.fetch_add(1, Ordering::Relaxed);
                info!(event_id = %job.event_id, "Event processed");
            }
            Err(error) => {
                metrics.ownership_errors.fetch_add(1, Ordering::Relaxed);
                error!(
                    event_id = %job.event_id,
                    %error,
                    "Could not mark event as succeeded; ownership may have expired"
                );
            }
        },
        Err(process_error) => {
            metrics.failed.fetch_add(1, Ordering::Relaxed);
            error!(
                event_id = %job.event_id,
                error = %process_error,
                "Event processing failed"
            );

            if let Err(queue_error) = queue
                .mark_failed_with_policy(
                    &job.event_id,
                    &worker_id,
                    &process_error,
                    settings.retry_base_seconds,
                    settings.retry_max_seconds,
                )
                .await
            {
                metrics.ownership_errors.fetch_add(1, Ordering::Relaxed);
                error!(
                    event_id = %job.event_id,
                    %queue_error,
                    "Could not record job failure; ownership may have expired"
                );
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("fluxara=info")),
        )
        .init();

    let settings = Settings::from_env()?;
    let database_url =
        std::env::var("FLUXARA_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.to_owned());

    let pool = PgPoolOptions::new()
        .max_connections((settings.concurrency + 5).min(133) as u32)
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;

    let queue = PostgresJobQueue::new(pool.clone());
    let metrics = Arc::new(Metrics::new(pool));
    let worker_id = format!("worker-{}", std::process::id());

    let metrics_router = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(metrics.clone());
    let listener = tokio::net::TcpListener::bind(settings.metrics_addr).await?;
    let metrics_addr = listener.local_addr()?;
    let metrics_server = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, metrics_router).await {
            error!(%error, "Metrics server stopped unexpectedly");
        }
    });

    info!(
        %worker_id,
        concurrency = settings.concurrency,
        lease_seconds = settings.lease_seconds,
        poll_interval_ms = settings.poll_interval_ms,
        retry_base_seconds = settings.retry_base_seconds,
        retry_max_seconds = settings.retry_max_seconds,
        metrics_addr = %metrics_addr,
        "Fluxara worker started"
    );

    let mut tasks = JoinSet::new();
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);
    let mut shutting_down = false;

    loop {
        if shutting_down {
            if tasks.is_empty() {
                break;
            }
            if let Some(Err(error)) = tasks.join_next().await {
                error!(%error, "Worker task terminated unexpectedly");
            }
            continue;
        }

        if tasks.len() >= settings.concurrency {
            tokio::select! {
                signal = &mut shutdown_signal => {
                    signal?;
                    info!("Shutdown requested; stopping new claims and draining active jobs");
                    shutting_down = true;
                }
                result = tasks.join_next() => {
                    if let Some(Err(error)) = result {
                        error!(%error, "Worker task terminated unexpectedly");
                    }
                }
            }
            continue;
        }

        tokio::select! {
            signal = &mut shutdown_signal => {
                signal?;
                info!("Shutdown requested; stopping new claims and draining active jobs");
                shutting_down = true;
            }
            result = queue.claim_one(&worker_id, settings.lease_seconds) => {
                match result {
                    Ok(Some(job)) => {
                        metrics.claimed.fetch_add(1, Ordering::Relaxed);
                        tasks.spawn(run_job(
                            queue.clone(),
                            metrics.clone(),
                            settings.clone(),
                            worker_id.clone(),
                            job,
                        ));
                    }
                    Ok(None) => {
                        tokio::select! {
                            signal = &mut shutdown_signal => {
                                signal?;
                                info!("Shutdown requested; stopping new claims");
                                shutting_down = true;
                            }
                            _ = sleep(Duration::from_millis(settings.poll_interval_ms)) => {}
                        }
                    }
                    Err(error) => {
                        metrics.claim_errors.fetch_add(1, Ordering::Relaxed);
                        error!(%error, "Failed to claim job");
                        tokio::select! {
                            signal = &mut shutdown_signal => {
                                signal?;
                                info!("Shutdown requested; stopping new claims");
                                shutting_down = true;
                            }
                            _ = sleep(Duration::from_secs(2)) => {}
                        }
                    }
                }
            }
        }
    }

    metrics_server.abort();
    info!("All in-flight jobs drained; Fluxara worker stopped");
    Ok(())
}
