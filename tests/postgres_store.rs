use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use fluxara::events::{AcceptedEvent, AsyncEventStore, PostgresEventStore, StoreOutcome};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

fn event(id: String, event_type: &str) -> AcceptedEvent {
    AcceptedEvent {
        id,
        event_type: event_type.to_owned(),
        schema_version: 1,
        occurred_at: "2026-10-16T12:00:00Z".to_owned(),
        payload: json!({"order_id": "phase4-test"}),
        received_at: "2026-10-16T12:00:01Z".to_owned(),
    }
}

async fn test_store() -> Option<PostgresEventStore> {
    let url = std::env::var("FLUXARA_TEST_DATABASE_URL").ok()?;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&url)
        .await
        .expect("connect to FLUXARA_TEST_DATABASE_URL");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("apply Fluxara migrations in test database");
    Some(PostgresEventStore::new(pool))
}

#[tokio::test]
async fn postgres_persists_events_and_resolves_duplicates_conflicts_and_races() {
    let Some(store) = test_store().await else {
        eprintln!("skipping PostgreSQL integration test: FLUXARA_TEST_DATABASE_URL is unset");
        return;
    };

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let id = format!("phase4-{unique}");
    let original = event(id.clone(), "order.created");

    assert!(matches!(
        store.insert_async(original.clone()).await.unwrap(),
        StoreOutcome::Accepted(_)
    ));

    let replay = AcceptedEvent {
        received_at: "2026-10-16T15:00:00Z".to_owned(),
        ..original.clone()
    };
    assert!(matches!(
        store.insert_async(replay).await.unwrap(),
        StoreOutcome::Duplicate(_)
    ));

    assert!(matches!(
        store
            .insert_async(event(id.clone(), "order.cancelled"))
            .await
            .unwrap(),
        StoreOutcome::Conflict(_)
    ));

    let read_back = store.get_async(&id).await.unwrap().expect("stored event");
    assert_eq!(read_back.event_type, "order.created");
    assert_eq!(read_back.payload, original.payload);

    // A new store instance over the same database must still see the event.
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var("FLUXARA_TEST_DATABASE_URL").unwrap())
        .await
        .unwrap();
    let reopened = PostgresEventStore::new(pool);
    assert_eq!(reopened.get_async(&id).await.unwrap().unwrap().id, id);

    // Concurrent submissions of the same ID: exactly one insert wins.
    let race_id = format!("phase4-race-{unique}");
    let shared = Arc::new(reopened);
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let store = Arc::clone(&shared);
        let candidate = event(race_id.clone(), "order.created");
        tasks.push(tokio::spawn(async move {
            store.insert_async(candidate).await.unwrap()
        }));
    }

    let mut accepted = 0;
    let mut duplicates = 0;
    for task in tasks {
        match task.await.unwrap() {
            StoreOutcome::Accepted(_) => accepted += 1,
            StoreOutcome::Duplicate(_) => duplicates += 1,
            StoreOutcome::Conflict(_) => panic!("identical concurrent event conflicted"),
        }
    }
    assert_eq!(accepted, 1);
    assert_eq!(duplicates, 15);
}
