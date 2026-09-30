use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
};
use std::net::SocketAddr;
use straight_rs::NodeConfig;
use straight_rs_model::UserId;
use straight_rs_worker::{SecretString, WorkerBuilder, WorkerConfigBuilder};
use tower::ServiceExt;
#[allow(dead_code)]
mod common {
    #[path = "../common/fake_gateway.rs"]
    pub mod fake_gateway;
    #[path = "../common/mock_lavalink.rs"]
    pub mod mock_lavalink;
}
use common::{fake_gateway::FakeGateway, mock_lavalink::MockLavalink};

const TOKEN: &str = "synthetic-test-api-token-32-bytes-long";
async fn app() -> axum::Router {
    let lavalink = MockLavalink::start().await;
    let config = WorkerConfigBuilder::new(
        UserId(9),
        SecretString::new("synthetic-bot-secret"),
        SecretString::new(TOKEN),
        vec![NodeConfig::new(
            lavalink.host(),
            "synthetic-lavalink-secret",
        )],
    )
    .build()
    .unwrap();
    // Keep the mock alive for the lifetime of the worker's client connection.
    std::mem::forget(lavalink);
    WorkerBuilder::new(config, FakeGateway)
        .build()
        .await
        .unwrap()
        .router()
}
fn request(uri: &str, auth: Option<&str>, body: &str) -> Request<Body> {
    let mut builder = if uri == "/healthz" || uri == "/not-found" {
        Request::get(uri)
    } else {
        Request::post(uri)
    }
    .header("content-type", "application/json");
    if let Some(value) = auth {
        builder = builder.header("authorization", value);
    }
    builder
        .extension(ConnectInfo(
            "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
        ))
        .body(Body::from(body.to_owned()))
        .unwrap()
}
#[tokio::test]
async fn missing_auth_is_rejected() {
    let response = app()
        .await
        .oneshot(
            Request::get("/healthz")
                .extension(ConnectInfo(
                    "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
#[tokio::test]
async fn malformed_auth_is_rejected() {
    for value in ["Bearer", "Basic abc", "Bearer wrong"] {
        let response = app()
            .await
            .oneshot(request("/healthz", Some(value), ""))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
#[tokio::test]
async fn oversized_body_is_rejected_with_sanitized_413() {
    let body = format!("{{\"identifier\":\"{}\"}}", "x".repeat(1_048_576));
    let response = app()
        .await
        .oneshot(request(
            "/v1/guilds/1/play",
            Some(&format!("Bearer {TOKEN}")),
            &body,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .unwrap()
        .to_bytes();
    let text = String::from_utf8_lossy(&bytes);
    assert!(text.contains("body_too_large"));
    assert!(!text.contains("synthetic"));
}

#[tokio::test]
async fn authorized_health_and_invalid_route_are_structured() {
    let app = app().await;
    let response = app
        .clone()
        .oneshot(request("/healthz", Some(&format!("Bearer {TOKEN}")), ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .oneshot(request("/not-found", Some(&format!("Bearer {TOKEN}")), ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
