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
struct Harness {
    router: axum::Router,
    _worker: straight_rs_worker::RunningWorker,
    _mock: MockLavalink,
}
async fn app() -> Harness {
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
    let worker = WorkerBuilder::new(config, FakeGateway)
        .build()
        .await
        .unwrap();
    let router = worker.router();
    Harness {
        router,
        _worker: worker,
        _mock: lavalink,
    }
}
fn request(uri: &str, auth: Option<&str>, body: &str) -> Request<Body> {
    let mut builder = if uri == "/healthz" || uri == "/not-found" || uri.ends_with("/player") {
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
        .router
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
            .router
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
        .router
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
        .router
        .clone()
        .oneshot(request("/healthz", Some(&format!("Bearer {TOKEN}")), ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .router
        .oneshot(request("/not-found", Some(&format!("Bearer {TOKEN}")), ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

async fn body_text(response: axum::response::Response) -> String {
    String::from_utf8(
        http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap()
}

#[tokio::test]
async fn invalid_snowflakes_and_mutation_values_are_structured_bad_requests() {
    let app = app().await;
    let cases = [
        ("/v1/guilds/nope/player", ""),
        ("/v1/guilds/0/player", ""),
        ("/v1/guilds/01/player", ""),
        ("/v1/guilds/1/join", r#"{"channel_id":"01"}"#),
        ("/v1/guilds/1/join", r#"{"channel_id":"0"}"#),
        ("/v1/guilds/1/play", r#"{"identifier":"x","extra":true}"#),
        ("/v1/guilds/1/join", r#"{"channel_id":"2","extra":true}"#),
        ("/v1/guilds/1/pause", r#"{"paused":true,"extra":true}"#),
        ("/v1/guilds/1/seek", r#"{"position_ms":1,"extra":true}"#),
        ("/v1/guilds/1/volume", r#"{"volume":1,"extra":true}"#),
        ("/v1/guilds/1/play", r#"{"identifier":"  "}"#),
        ("/v1/guilds/1/seek", r#"{"position_ms":86400001}"#),
        ("/v1/guilds/1/volume", r#"{"volume":1001}"#),
    ];
    for (uri, body) in cases {
        let response = app
            .router
            .clone()
            .oneshot(request(uri, Some(&format!("Bearer {TOKEN}")), body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}: {body}");
        let text = body_text(response).await;
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["error"]["code"], "invalid_request", "{text}");
        assert!(json["error"]["message"].is_string(), "{text}");
    }
}

#[tokio::test]
async fn auth_error_body_and_debug_omit_the_synthetic_api_token() {
    let response = app()
        .await
        .router
        .oneshot(request("/healthz", Some("Bearer wrong"), ""))
        .await
        .unwrap();
    let text = body_text(response).await;
    assert!(!text.contains(TOKEN));
    assert!(serde_json::from_str::<serde_json::Value>(&text).unwrap()["error"].is_object());
    let auth = straight_rs_worker::auth::AuthState::new(
        SecretString::new(TOKEN),
        straight_rs_worker::auth::RateLimitConfig {
            requests: 2,
            window: std::time::Duration::from_secs(1),
            table_capacity: 4,
        },
    );
    assert!(!format!("{auth:?}").contains(TOKEN));
}
