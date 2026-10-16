use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode, header},
};
use fluxara::{api::router_with_store, events::InMemoryEventStore};
use serde_json::{Value, json};
use tower::ServiceExt;

fn test_app() -> Router {
    router_with_store(InMemoryEventStore::new())
}

fn valid_event(id: &str) -> Value {
    json!({
        "id": id,
        "event_type": "order.created",
        "schema_version": 1,
        "occurred_at": "2026-10-16T12:00:00Z",
        "payload": {"order_id": "123", "total": 99.99}
    })
}

async fn send_json(
    app: Router,
    method: &str,
    uri: &str,
    body: Vec<u8>,
) -> (StatusCode, HeaderMap, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), 8 * 1024 * 1024)
        .await
        .unwrap();
    let payload: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({"_raw": String::from_utf8_lossy(&bytes)}));
    (status, headers, payload)
}

async fn post_value(app: Router, uri: &str, value: &Value) -> (StatusCode, HeaderMap, Value) {
    send_json(app, "POST", uri, serde_json::to_vec(value).unwrap()).await
}

fn request_id(headers: &HeaderMap) -> &str {
    headers
        .get("x-request-id")
        .expect("response should contain a request ID")
        .to_str()
        .unwrap()
}

#[tokio::test]
async fn create_and_retrieve_a_valid_event() {
    let app = test_app();

    let (status, headers, body) =
        post_value(app.clone(), "/v1/events", &valid_event("evt-1")).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(!request_id(&headers).is_empty());
    assert_eq!(body["id"], "evt-1");
    assert_eq!(body["event_type"], "order.created");
    assert_eq!(body["schema_version"], 1);
    assert_eq!(body["occurred_at"], "2026-10-16T12:00:00Z");
    assert_eq!(body["payload"]["order_id"], "123");
    let received_at = body["received_at"].as_str().expect("received_at is set");
    assert!(chrono::DateTime::parse_from_rfc3339(received_at).is_ok());

    let (status, headers, fetched) = send_json(app, "GET", "/v1/events/evt-1", Vec::new()).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!request_id(&headers).is_empty());
    assert_eq!(fetched, body);
}

#[tokio::test]
async fn occurred_at_is_normalized_to_utc() {
    let mut event = valid_event("evt-tz");
    event["occurred_at"] = json!("2026-10-16T14:00:00+02:00");

    let (status, _, body) = post_value(test_app(), "/v1/events", &event).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["occurred_at"], "2026-10-16T12:00:00Z");
}

#[tokio::test]
async fn unknown_event_id_returns_json_not_found() {
    let (status, headers, body) =
        send_json(test_app(), "GET", "/v1/events/no-such-id", Vec::new()).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(!request_id(&headers).is_empty());
    assert_eq!(body["error"]["code"], "not_found");
    assert_eq!(
        body["error"]["message"],
        "The requested event was not found"
    );
}

#[tokio::test]
async fn syntactically_invalid_id_in_path_is_still_not_found() {
    let (status, _, body) = send_json(test_app(), "GET", "/v1/events/%21%21%21", Vec::new()).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "not_found");
}

#[tokio::test]
async fn identical_duplicate_is_an_idempotent_replay() {
    let app = test_app();
    let event = valid_event("evt-dup");

    let (first_status, _, first) = post_value(app.clone(), "/v1/events", &event).await;
    assert_eq!(first_status, StatusCode::CREATED);

    let (second_status, _, second) = post_value(app.clone(), "/v1/events", &event).await;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(second, first);

    // The stored record is unchanged: retrieval returns the original.
    let (_, _, fetched) = send_json(app, "GET", "/v1/events/evt-dup", Vec::new()).await;
    assert_eq!(fetched, first);
}

#[tokio::test]
async fn conflicting_duplicate_returns_409_and_keeps_the_original() {
    let app = test_app();

    let (status, _, _) = post_value(app.clone(), "/v1/events", &valid_event("evt-1")).await;
    assert_eq!(status, StatusCode::CREATED);

    let mut conflict = valid_event("evt-1");
    conflict["event_type"] = json!("order.cancelled");
    let (status, _, body) = post_value(app.clone(), "/v1/events", &conflict).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], "conflict");

    let (_, _, fetched) = send_json(app, "GET", "/v1/events/evt-1", Vec::new()).await;
    assert_eq!(fetched["event_type"], "order.created");
}

#[tokio::test]
async fn batch_accepts_valid_events_with_per_event_results() {
    let app = test_app();
    let batch = json!({"events": [valid_event("evt-b1"), valid_event("evt-b2")]});

    let (status, headers, body) = post_value(app.clone(), "/v1/events/batch", &batch).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!request_id(&headers).is_empty());
    assert_eq!(body["results"].as_array().unwrap().len(), 2);
    assert_eq!(body["results"][0]["status"], 201);
    assert_eq!(body["results"][0]["result"], "accepted");
    assert_eq!(body["results"][0]["event"]["id"], "evt-b1");
    assert_eq!(body["results"][1]["status"], 201);

    // Accepted batch entries are retrievable individually.
    let (status, _, _) = send_json(app, "GET", "/v1/events/evt-b1", Vec::new()).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn batch_reports_mixed_outcomes_without_atomicity() {
    let app = test_app();
    let mut conflict = valid_event("evt-m1");
    conflict["event_type"] = json!("order.cancelled");
    let mut invalid = valid_event("evt-m2");
    invalid["event_type"] = json!("   ");

    let batch = json!({"events": [valid_event("evt-m1"), conflict, invalid]});
    let (status, _, body) = post_value(app.clone(), "/v1/events/batch", &batch).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"][0]["result"], "accepted");
    assert_eq!(body["results"][1]["status"], 409);
    assert_eq!(body["results"][1]["result"], "conflict");
    assert_eq!(body["results"][2]["status"], 422);
    assert_eq!(body["results"][2]["result"], "invalid");

    // The valid entry stays accepted despite sibling failures.
    let (status, _, _) = send_json(app, "GET", "/v1/events/evt-m1", Vec::new()).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn batch_duplicate_ids_with_identical_content_are_duplicates() {
    let batch = json!({"events": [valid_event("evt-d"), valid_event("evt-d")]});
    let (status, _, body) = post_value(test_app(), "/v1/events/batch", &batch).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"][0]["result"], "accepted");
    assert_eq!(body["results"][0]["status"], 201);
    assert_eq!(body["results"][1]["result"], "duplicate");
    assert_eq!(body["results"][1]["status"], 200);
    assert_eq!(
        body["results"][1]["event"]["received_at"],
        body["results"][0]["event"]["received_at"]
    );
}

#[tokio::test]
async fn batch_duplicate_ids_with_different_content_conflict() {
    let mut second = valid_event("evt-c");
    second["payload"] = json!({"order_id": "different"});
    let batch = json!({"events": [valid_event("evt-c"), second]});

    let (status, _, body) = post_value(test_app(), "/v1/events/batch", &batch).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"][0]["result"], "accepted");
    assert_eq!(body["results"][1]["result"], "conflict");
    assert_eq!(body["results"][1]["status"], 409);
}

#[tokio::test]
async fn batch_replays_previously_accepted_ids_as_duplicates() {
    let app = test_app();
    let (status, _, _) = post_value(app.clone(), "/v1/events", &valid_event("evt-pre")).await;
    assert_eq!(status, StatusCode::CREATED);

    let batch = json!({"events": [valid_event("evt-pre")]});
    let (status, _, body) = post_value(app, "/v1/events/batch", &batch).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"][0]["result"], "duplicate");
    assert_eq!(body["results"][0]["status"], 200);
}

#[tokio::test]
async fn validation_failures_return_422_with_shared_envelope() {
    let cases: Vec<(&str, Value)> = vec![
        (
            "missing id",
            json!({
                "event_type": "order.created", "schema_version": 1,
                "occurred_at": "2026-10-16T12:00:00Z", "payload": {}
            }),
        ),
        (
            "empty id",
            json!({
                "id": "", "event_type": "order.created", "schema_version": 1,
                "occurred_at": "2026-10-16T12:00:00Z", "payload": {}
            }),
        ),
        (
            "bad id chars",
            json!({
                "id": "evt 1", "event_type": "order.created", "schema_version": 1,
                "occurred_at": "2026-10-16T12:00:00Z", "payload": {}
            }),
        ),
        (
            "oversized id",
            json!({
                "id": "a".repeat(129), "event_type": "order.created", "schema_version": 1,
                "occurred_at": "2026-10-16T12:00:00Z", "payload": {}
            }),
        ),
        (
            "blank type",
            json!({
                "id": "evt-x", "event_type": "   ", "schema_version": 1,
                "occurred_at": "2026-10-16T12:00:00Z", "payload": {}
            }),
        ),
        (
            "oversized type",
            json!({
                "id": "evt-x", "event_type": "t".repeat(129), "schema_version": 1,
                "occurred_at": "2026-10-16T12:00:00Z", "payload": {}
            }),
        ),
        (
            "unsupported version",
            json!({
                "id": "evt-x", "event_type": "t", "schema_version": 2,
                "occurred_at": "2026-10-16T12:00:00Z", "payload": {}
            }),
        ),
        (
            "string version",
            json!({
                "id": "evt-x", "event_type": "t", "schema_version": "1",
                "occurred_at": "2026-10-16T12:00:00Z", "payload": {}
            }),
        ),
        (
            "bad timestamp",
            json!({
                "id": "evt-x", "event_type": "t", "schema_version": 1,
                "occurred_at": "not-a-timestamp", "payload": {}
            }),
        ),
        (
            "null payload",
            json!({
                "id": "evt-x", "event_type": "t", "schema_version": 1,
                "occurred_at": "2026-10-16T12:00:00Z", "payload": null
            }),
        ),
        (
            "missing payload",
            json!({
                "id": "evt-x", "event_type": "t", "schema_version": 1,
                "occurred_at": "2026-10-16T12:00:00Z"
            }),
        ),
        (
            "unknown field",
            json!({
                "id": "evt-x", "event_type": "t", "schema_version": 1,
                "occurred_at": "2026-10-16T12:00:00Z", "payload": {},
                "surprise": "nope"
            }),
        ),
    ];

    for (name, event) in cases {
        let (status, _, body) = post_value(test_app(), "/v1/events", &event).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "case: {name}");
        assert_eq!(
            body["error"]["code"], "unprocessable_entity",
            "case: {name}"
        );
    }
}

#[tokio::test]
async fn malformed_json_returns_400() {
    let (status, _, body) = send_json(
        test_app(),
        "POST",
        "/v1/events",
        b"{\"id\": \"evt-1\", ".to_vec(),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "bad_request");
}

#[tokio::test]
async fn valid_json_array_is_422_not_400() {
    let (status, _, body) =
        send_json(test_app(), "POST", "/v1/events", b"[1, 2, 3]".to_vec()).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], "unprocessable_entity");
}

#[tokio::test]
async fn oversized_payload_returns_413() {
    let mut event = valid_event("evt-big");
    event["payload"] = json!({"data": "x".repeat(300 * 1024)});

    let (status, _, body) = post_value(test_app(), "/v1/events", &event).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["error"]["code"], "payload_too_large");
}

#[tokio::test]
async fn oversized_request_body_returns_413() {
    let mut event = valid_event("evt-huge");
    event["payload"] = json!({"data": "x".repeat(2 * 1024 * 1024)});

    let (status, _, body) = post_value(test_app(), "/v1/events", &event).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["error"]["code"], "payload_too_large");
}

#[tokio::test]
async fn batch_limits_are_enforced() {
    // Empty batch.
    let (status, _, _) = post_value(test_app(), "/v1/events/batch", &json!({"events": []})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Missing events field.
    let (status, _, _) = post_value(test_app(), "/v1/events/batch", &json!({})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Non-array events.
    let (status, _, _) =
        post_value(test_app(), "/v1/events/batch", &json!({"events": "nope"})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

    // Too many events.
    let events: Vec<Value> = (0..101).map(|i| valid_event(&format!("evt-{i}"))).collect();
    let (status, _, body) =
        post_value(test_app(), "/v1/events/batch", &json!({"events": events})).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["error"]["code"], "payload_too_large");

    // Malformed batch JSON.
    let (status, _, body) = send_json(
        test_app(),
        "POST",
        "/v1/events/batch",
        b"{\"events\": [".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "bad_request");
}

#[tokio::test]
async fn batch_accepts_the_maximum_batch_size() {
    let events: Vec<Value> = (0..100)
        .map(|i| valid_event(&format!("evt-max-{i}")))
        .collect();
    let (status, _, body) =
        post_value(test_app(), "/v1/events/batch", &json!({"events": events})).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"].as_array().unwrap().len(), 100);
    assert!(
        body["results"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["status"] == 201)
    );
}

#[tokio::test]
async fn concurrent_duplicate_submissions_resolve_consistently() {
    let app = test_app();
    let body = serde_json::to_vec(&valid_event("evt-concurrent")).unwrap();

    let tasks: Vec<_> = (0..32)
        .map(|_| {
            let app = app.clone();
            let body = body.clone();
            tokio::spawn(async move {
                let request = Request::builder()
                    .method("POST")
                    .uri("/v1/events")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap();
                let response = app.oneshot(request).await.unwrap();
                let status = response.status();
                let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
                let payload: Value = serde_json::from_slice(&bytes).unwrap();
                (status, payload)
            })
        })
        .collect();

    let mut created = 0;
    let mut duplicates = 0;
    let mut received_at_values = Vec::new();
    for task in tasks {
        let (status, payload) = task.await.unwrap();
        match status {
            StatusCode::CREATED => created += 1,
            StatusCode::OK => duplicates += 1,
            other => panic!("unexpected status {other}"),
        }
        received_at_values.push(payload["received_at"].as_str().unwrap().to_owned());
    }

    assert_eq!(created, 1);
    assert_eq!(duplicates, 31);
    assert!(
        received_at_values
            .iter()
            .all(|v| v == &received_at_values[0])
    );
}
