use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use fluxara::api::router;
use serde_json::{Value, json};
use tower::ServiceExt;

#[tokio::test]
async fn health_returns_the_compatible_json_contract_and_request_id() {
    let response = router()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let request_id = response
        .headers()
        .get("x-request-id")
        .expect("response should contain a request ID");

    assert!(!request_id.to_str().unwrap().is_empty());

    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        payload,
        json!({
            "status": "ok",
            "service": "fluxara",
            "version": env!("CARGO_PKG_VERSION")
        })
    );
}

#[tokio::test]
async fn unknown_routes_return_a_consistent_json_error() {
    let response = router()
        .oneshot(
            Request::builder()
                .uri("/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );

    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let payload: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["error"]["code"], "not_found");
    assert_eq!(
        payload["error"]["message"],
        "The requested resource was not found"
    );
}
