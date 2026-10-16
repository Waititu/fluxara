use std::time::{SystemTime, UNIX_EPOCH};

use fluxara::events::{
    AcceptedEvent, AsyncEventStore, PostgresEventStore, PostgresJobQueue, StoreOutcome,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

fn unique_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after UNIX epoch")
        .as_nanos();

    format!("{prefix}-{nanos}")
}

fn event(id: String) -> AcceptedEvent {
    AcceptedEvent {
        id,
        event_type: "fluxara.phase5.test".to_owned(),
        schema_version: 1,
        occurred_at: "2026-10-16T12:00:00Z".to_owned(),
        payload: json!({"source": "phase5-integration-test"}),
        received_at: "2026-10-16T12:00:00Z".to_owned(),
    }
}

async fn database() -> Option<(sqlx::PgPool, PostgresJobQueue, PostgresEventStore)> {
    let url = match std::env::var("FLUXARA_TEST_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping PostgreSQL job tests: FLUXARA_TEST_DATABASE_URL is unset");
            return None;
        }
    };

    // Keep test tables separate from the application's public schema.
    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("test database should be reachable");

    sqlx::query("CREATE SCHEMA IF NOT EXISTS fluxara_jobs_test")
        .execute(&admin_pool)
        .await
        .expect("job test schema should be creatable");

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET search_path TO fluxara_jobs_test")
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .expect("isolated job-test pool should connect");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("migrations should succeed in the isolated schema");

    // Reset only the dedicated test schema, never the application's tables.
    sqlx::query("TRUNCATE TABLE event_jobs, events CASCADE")
        .execute(&pool)
        .await
        .expect("isolated job-test tables should be reset");

    let queue = PostgresJobQueue::new(pool.clone());
    let store = PostgresEventStore::new(pool.clone());

    Some((pool, queue, store))
}

#[tokio::test]
async fn jobs_are_atomic_retryable_recoverable_and_claimed_once() {
    let Some((pool, queue, store)) = database().await else {
        return;
    };

    let worker_a = "phase5-worker-a";
    let worker_b = "phase5-worker-b";

    // A newly accepted event must have exactly one durable job.
    let first_id = unique_id("phase5-atomic");
    assert!(matches!(
        store.insert_async(event(first_id.clone())).await.unwrap(),
        StoreOutcome::Accepted(_)
    ));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_jobs WHERE event_id = $1")
        .bind(&first_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    // A replay must not create another job.
    assert!(matches!(
        store.insert_async(event(first_id.clone())).await.unwrap(),
        StoreOutcome::Duplicate(_)
    ));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM event_jobs WHERE event_id = $1")
        .bind(&first_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);

    // Successful processing is terminal.
    let claimed = queue.claim_one(worker_a, 30).await.unwrap().unwrap();
    assert_eq!(claimed.event_id, first_id);
    assert_eq!(claimed.attempts, 1);

    queue.mark_succeeded(&first_id, worker_a).await.unwrap();

    let status: String = sqlx::query_scalar("SELECT status FROM event_jobs WHERE event_id = $1")
        .bind(&first_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "succeeded");

    // Retry with backoff; move availability forward in the test to avoid
    // waiting for production backoff delays.
    let retry_id = unique_id("phase5-retry");
    store.insert_async(event(retry_id.clone())).await.unwrap();

    let claimed = queue.claim_one(worker_a, 30).await.unwrap().unwrap();
    assert_eq!(claimed.event_id, retry_id);

    queue
        .mark_failed(&retry_id, worker_a, "simulated transient failure")
        .await
        .unwrap();

    let status: String = sqlx::query_scalar("SELECT status FROM event_jobs WHERE event_id = $1")
        .bind(&retry_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "pending");

    sqlx::query("UPDATE event_jobs SET available_at = NOW() WHERE event_id = $1")
        .bind(&retry_id)
        .execute(&pool)
        .await
        .unwrap();

    let claimed = queue.claim_one(worker_b, 30).await.unwrap().unwrap();
    assert_eq!(claimed.event_id, retry_id);
    assert_eq!(claimed.attempts, 2);

    // A worker cannot complete a job owned by another worker.
    assert!(queue.mark_succeeded(&retry_id, worker_a).await.is_err());

    queue.mark_succeeded(&retry_id, worker_b).await.unwrap();

    // Expired processing leases recover jobs abandoned by crashed workers.
    let recovery_id = unique_id("phase5-recovery");
    store
        .insert_async(event(recovery_id.clone()))
        .await
        .unwrap();

    let claimed = queue.claim_one(worker_a, 30).await.unwrap().unwrap();
    assert_eq!(claimed.event_id, recovery_id);

    sqlx::query(
        "UPDATE event_jobs SET locked_at = NOW() - INTERVAL '2 minutes' WHERE event_id = $1",
    )
    .bind(&recovery_id)
    .execute(&pool)
    .await
    .unwrap();

    let recovered = queue.claim_one(worker_b, 30).await.unwrap().unwrap();
    assert_eq!(recovered.event_id, recovery_id);
    assert_eq!(recovered.attempts, 2);

    queue.mark_succeeded(&recovery_id, worker_b).await.unwrap();

    // Repeated failures eventually move the job to dead-letter state.
    let dead_id = unique_id("phase5-dead");
    store.insert_async(event(dead_id.clone())).await.unwrap();

    for attempt in 1..=5 {
        let claimed = queue.claim_one(worker_a, 30).await.unwrap().unwrap();
        assert_eq!(claimed.event_id, dead_id);
        assert_eq!(claimed.attempts, attempt);

        queue
            .mark_failed(&dead_id, worker_a, "simulated permanent failure")
            .await
            .unwrap();

        sqlx::query("UPDATE event_jobs SET available_at = NOW() WHERE event_id = $1")
            .bind(&dead_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    let status: String = sqlx::query_scalar("SELECT status FROM event_jobs WHERE event_id = $1")
        .bind(&dead_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "dead_letter");

    let error: Option<String> =
        sqlx::query_scalar("SELECT last_error FROM event_jobs WHERE event_id = $1")
            .bind(&dead_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(error.as_deref(), Some("simulated permanent failure"));

    // Two concurrent workers should not claim the same job.
    let concurrent_a = unique_id("phase5-concurrent-a");
    let concurrent_b = unique_id("phase5-concurrent-b");
    store
        .insert_async(event(concurrent_a.clone()))
        .await
        .unwrap();
    store
        .insert_async(event(concurrent_b.clone()))
        .await
        .unwrap();

    let (left, right) = tokio::join!(
        queue.claim_one("phase5-concurrent-left", 30),
        queue.claim_one("phase5-concurrent-right", 30),
    );

    let left = left.unwrap().unwrap();
    let right = right.unwrap().unwrap();

    assert_ne!(left.event_id, right.event_id);

    eprintln!(
        "Phase 5 verified: atomic enqueue, replay idempotency, retry, ownership, recovery, dead-letter, and concurrent claims."
    );
}
