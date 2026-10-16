use serde_json::Value;
use sqlx::{FromRow, PgPool};

use super::store::StoreError;

#[derive(Clone, Debug, FromRow)]
pub struct ClaimedJob {
    pub event_id: String,
    pub event_type: String,
    pub payload: Value,
    pub attempts: i32,
    pub max_attempts: i32,
}

#[derive(Clone, Debug)]
pub struct PostgresJobQueue {
    pool: PgPool,
}

impl PostgresJobQueue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Atomically claim one available job.
    ///
    /// SKIP LOCKED allows multiple worker processes to claim different jobs.
    /// Expired processing leases make abandoned jobs eligible for recovery.
    pub async fn claim_one(
        &self,
        worker_id: &str,
        lease_seconds: i64,
    ) -> Result<Option<ClaimedJob>, StoreError> {
        let mut tx = self.pool.begin().await?;

        // A worker that crashed on its final attempt must not strand a job
        // permanently in "processing".
        sqlx::query(
            r#"
            UPDATE event_jobs
            SET status = 'dead_letter',
                locked_at = NULL,
                worker_id = NULL,
                last_error = COALESCE(last_error, 'Worker lease expired on final attempt'),
                updated_at = NOW()
            WHERE status = 'processing'
              AND attempts >= max_attempts
              AND locked_at < NOW() - ($1 * INTERVAL '1 second')
            "#,
        )
        .bind(lease_seconds)
        .execute(&mut *tx)
        .await?;

        let job = sqlx::query_as::<_, ClaimedJob>(
            r#"
            WITH candidate AS (
                SELECT event_id
                FROM event_jobs
                WHERE (
                    status = 'pending'
                    AND available_at <= NOW()
                    AND attempts < max_attempts
                ) OR (
                    status = 'processing'
                    AND locked_at < NOW() - ($1 * INTERVAL '1 second')
                    AND attempts < max_attempts
                )
                ORDER BY available_at, created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE event_jobs AS j
            SET status = 'processing',
                attempts = j.attempts + 1,
                locked_at = NOW(),
                worker_id = $2,
                updated_at = NOW()
            FROM candidate AS c, events AS e
            WHERE j.event_id = c.event_id
              AND e.id = j.event_id
            RETURNING j.event_id, e.event_type, e.payload,
                      j.attempts, j.max_attempts
            "#,
        )
        .bind(lease_seconds)
        .bind(worker_id)
        .fetch_optional(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(job)
    }

    pub async fn mark_succeeded(&self, event_id: &str, worker_id: &str) -> Result<(), StoreError> {
        let result = sqlx::query(
            r#"
            UPDATE event_jobs
            SET status = 'succeeded',
                completed_at = NOW(),
                locked_at = NULL,
                worker_id = NULL,
                last_error = NULL,
                updated_at = NOW()
            WHERE event_id = $1
              AND status = 'processing'
              AND worker_id = $2
            "#,
        )
        .bind(event_id)
        .bind(worker_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() != 1 {
            return Err(StoreError(
                "job completion rejected: worker no longer owns the job".to_owned(),
            ));
        }

        Ok(())
    }

    pub async fn mark_failed(
        &self,
        event_id: &str,
        worker_id: &str,
        error: &str,
    ) -> Result<(), StoreError> {
        self.mark_failed_with_policy(event_id, worker_id, error, 2, 300)
            .await
    }

    /// Release a failed job using a configurable exponential retry policy.
    ///
    /// The base delay is doubled for each failed attempt and capped at
    /// `max_delay_seconds`. A final failed attempt is sent to dead-letter.
    pub async fn mark_failed_with_policy(
        &self,
        event_id: &str,
        worker_id: &str,
        error: &str,
        base_delay_seconds: i64,
        max_delay_seconds: i64,
    ) -> Result<(), StoreError> {
        if base_delay_seconds < 1 || max_delay_seconds < base_delay_seconds {
            return Err(StoreError(
                "retry delays must satisfy 1 <= base_delay_seconds <= max_delay_seconds".to_owned(),
            ));
        }

        let result = sqlx::query(
            r#"
            UPDATE event_jobs
            SET status = CASE
                    WHEN attempts >= max_attempts THEN 'dead_letter'
                    ELSE 'pending'
                END,
                available_at = CASE
                    WHEN attempts >= max_attempts THEN available_at
                    ELSE NOW() +
                        (LEAST($4 * POWER(2.0, GREATEST(attempts - 1, 0)), $5)::INTEGER
                         * INTERVAL '1 second')
                END,
                locked_at = NULL,
                worker_id = NULL,
                last_error = LEFT($3, 2000),
                updated_at = NOW()
            WHERE event_id = $1
              AND status = 'processing'
              AND worker_id = $2
            "#,
        )
        .bind(event_id)
        .bind(worker_id)
        .bind(error)
        .bind(base_delay_seconds as f64)
        .bind(max_delay_seconds as f64)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() != 1 {
            return Err(StoreError(
                "job failure update rejected: worker no longer owns the job".to_owned(),
            ));
        }

        Ok(())
    }
}
