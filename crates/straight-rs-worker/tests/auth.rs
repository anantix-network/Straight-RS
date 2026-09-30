use axum::{
    Router,
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
    routing::get,
};
use serde_json::Value;
use std::{net::SocketAddr, time::Duration};
use straight_rs_worker::{
    SecretString,
    auth::{self, AuthState, RateLimitConfig},
};
use tower::ServiceExt;
const TOKEN: &str = "synthetic-auth-test-token-at-least-32-bytes";
fn app() -> Router {
    let state = AuthState::new(
        SecretString::new(TOKEN),
        RateLimitConfig {
            requests: 1,
            window: Duration::from_secs(10),
            table_capacity: 1,
        },
    );
    auth::secure(
        Router::new().route("/healthz", get(|| async { "ok" })),
        state,
    )
}
fn req(token: Option<&str>, ip: &str) -> Request<Body> {
    let mut b = Request::get("/healthz");
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"))
    }
    b.extension(ConnectInfo(ip.parse::<SocketAddr>().unwrap()))
        .body(Body::empty())
        .unwrap()
}
#[tokio::test]
async fn missing_malformed_and_wrong_tokens_are_401() {
    for token in [None, Some("Basic bad"), Some("Bearer wrong")] {
        let r = app().oneshot(req(token, "127.0.0.1:1")).await.unwrap();
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    }
}
#[tokio::test]
async fn valid_bearer_passes() {
    let r = app()
        .oneshot(req(Some(TOKEN), "127.0.0.1:1"))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
}
#[tokio::test(start_paused = true)]
async fn limits_requests_resets_window_and_rejects_unseen_peer_when_full() {
    let a = app();
    let r = a
        .clone()
        .oneshot(req(Some(TOKEN), "127.0.0.1:1"))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let r = a
        .clone()
        .oneshot(req(Some(TOKEN), "127.0.0.1:1"))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
    let r = a
        .clone()
        .oneshot(req(Some(TOKEN), "127.0.0.2:1"))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::TOO_MANY_REQUESTS);
    tokio::time::advance(Duration::from_secs(10)).await;
    let r = a.oneshot(req(Some(TOKEN), "127.0.0.2:1")).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
}
#[tokio::test]
async fn same_ip_with_distinct_ports_shares_rate_limit_bucket() {
    let state = AuthState::new(
        SecretString::new(TOKEN),
        RateLimitConfig {
            requests: 1,
            window: Duration::from_secs(10),
            table_capacity: 4,
        },
    );
    let router = auth::secure(
        Router::new().route("/healthz", get(|| async { "ok" })),
        state,
    );
    let first = router
        .clone()
        .oneshot(req(Some(TOKEN), "127.0.0.1:1001"))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let second = router
        .oneshot(req(Some(TOKEN), "127.0.0.1:2002"))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn auth_errors_use_envelope_without_credentials() {
    let r = app()
        .oneshot(req(Some("wrong-secret"), "127.0.0.1:1"))
        .await
        .unwrap();
    let bytes = http_body_util::BodyExt::collect(r.into_body())
        .await
        .unwrap()
        .to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["error"]["code"], "unauthorized");
    assert!(!String::from_utf8_lossy(&bytes).contains("wrong-secret"));
}
