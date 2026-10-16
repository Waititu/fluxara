use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::{FromRow, PgPool, Postgres, Transaction};

use super::{
    model::AcceptedEvent,
    store::{AsyncEventStore, StoreError, StoreOutcome},
};

/// PostgreSQL-backed durable event store.
#[derive(Clone, Debug)]
pub struct PostgresEventStore {
    pool: PgPool,
}

impl PostgresEventStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[derive(Debug, FromRow)]
struct EventRow {
    id: String,
    event_type: String,
    schema_version: i32,
    occurred_at: DateTime<Utc>,
    payload: serde_json::Value,
    received_at: DateTime<Utc>,
}

impl EventRow {
    fn into_event(self) -> AcceptedEvent {
        AcceptedEvent {
            id: self.id,
            event_type: self.event_type,
            schema_version: self.schema_version as u32,
            occurred_at: self
                .occurred_at
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
            payload: self.payload,
            received_at: self
                .received_at
                .to_rfc3339_opts(SecondsFormat::AutoSi, true),
        }
    }
}

const EVENT_COLUMNS: &str = "id, event_type, schema_version, occurred_at, payload, received_at";

async fn select_existing(
    tx: &mut Transaction<'_, Postgres>,
    id: &str,
) -> Result<Option<AcceptedEvent>, StoreError> {
    let query = format!("SELECT {EVENT_COLUMNS} FROM events WHERE id = $1 FOR UPDATE");
    let row = sqlx::query_as::<_, EventRow>(&query)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await?;
    Ok(row.map(EventRow::into_event))
}

#[async_trait::async_trait]
impl AsyncEventStore for PostgresEventStore {
    async fn insert_async(&self, event: AcceptedEvent) -> Result<StoreOutcome, StoreError> {
        let occurred_at = DateTime::parse_from_rfc3339(&event.occurred_at)
            .map_err(|error| StoreError(format!("invalid occurred_at timestamp: {error}")))?
            .with_timezone(&Utc);

        let mut tx = self.pool.begin().await?;

        let query = format!(
            "INSERT INTO events
                (id, event_type, schema_version, occurred_at, payload)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (id) DO NOTHING
             RETURNING {EVENT_COLUMNS}"
        );

        let inserted = sqlx::query_as::<_, EventRow>(&query)
            .bind(&event.id)
            .bind(&event.event_type)
            .bind(event.schema_version as i32)
            .bind(occurred_at)
            .bind(&event.payload)
            .fetch_optional(&mut *tx)
            .await?;

        if let Some(row) = inserted {
            let stored = row.into_event();
            tx.commit().await?;
            return Ok(StoreOutcome::Accepted(stored));
        }

        // The primary-key constraint serializes concurrent inserts of the
        // same ID. Under READ COMMITTED, this query sees the committed winner.
        let existing = select_existing(&mut tx, &event.id).await?.ok_or_else(|| {
            StoreError("event conflicted during insert but was not found afterward".to_owned())
        })?;

        tx.commit().await?;

        if existing.content_eq(&event) {
            Ok(StoreOutcome::Duplicate(existing))
        } else {
            Ok(StoreOutcome::Conflict(existing))
        }
    }

    async fn get_async(&self, id: &str) -> Result<Option<AcceptedEvent>, StoreError> {
        let query = format!("SELECT {EVENT_COLUMNS} FROM events WHERE id = $1");
        let row = sqlx::query_as::<_, EventRow>(&query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(EventRow::into_event))
    }
}
