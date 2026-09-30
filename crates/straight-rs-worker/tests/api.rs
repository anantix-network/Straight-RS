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
use common::{
    fake_gateway::{ControlledGateway, FakeGateway, RecordingGateway},
    mock_lavalink::{MockLavalink, synthetic_track},
};

const TOKEN: &str = "synthetic-test-api-token-32-bytes-long";
struct Harness {
    router: axum::Router,
    worker: straight_rs_worker::RunningWorker,
    mock: MockLavalink,
}
async fn app() -> Harness {
    app_with(FakeGateway, MockLavalink::start().await).await
}
async fn app_with<D: straight_rs_worker::GatewayDriver>(
    gateway: D,
    lavalink: MockLavalink,
) -> Harness {
    app_with_timeout(gateway, lavalink, None).await
}
async fn app_with_timeout<D: straight_rs_worker::GatewayDriver>(
    gateway: D,
    lavalink: MockLavalink,
    callback_timeout: Option<std::time::Duration>,
) -> Harness {
    let mut config = WorkerConfigBuilder::new(
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
    if let Some(t) = callback_timeout {
        config.callback_timeout = t;
    }
    let worker = WorkerBuilder::new(config, gateway).build().await.unwrap();
    let router = worker.router();
    Harness {
        router,
        worker,
        mock: lavalink,
    }
}
async fn wait_ready(h: &Harness) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !h.worker.status().ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("readiness deadline elapsed");
}
fn auth_header() -> String {
    format!("Bearer {TOKEN}")
}
/// Joins through the API and waits (bounded) for the bot's own voice-state
/// event to create the player, so later mutating calls act on a real session.
async fn join_and_wait(h: &Harness, guild: u64, channel: u64) {
    let (status, text) = call(
        h,
        &format!("/v1/guilds/{guild}/join"),
        &format!(r#"{{"channel_id":"{channel}"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{text}");
    wait_player(h, guild).await;
}
async fn wait_player(h: &Harness, guild: u64) {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let (status, _) = call(h, &format!("/v1/guilds/{guild}/player"), "").await;
            if status == StatusCode::OK {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("voice session deadline elapsed");
}
async fn call(h: &Harness, uri: &str, body: &str) -> (StatusCode, String) {
    let response = h
        .router
        .clone()
        .oneshot(request(uri, Some(&auth_header()), body))
        .await
        .unwrap();
    let status = response.status();
    (status, body_text(response).await)
}
fn request(uri: &str, auth: Option<&str>, body: &str) -> Request<Body> {
    let mut builder =
        if uri == "/healthz" || uri == "/readyz" || uri == "/not-found" || uri.ends_with("/player")
        {
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

#[tokio::test]
async fn readyz_is_503_when_only_gateway_is_unready_then_200() {
    let gateway = ControlledGateway::withheld_ready();
    let ready = gateway.ready.clone();
    let h = app_with(gateway, MockLavalink::start().await).await;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !h.worker.status().lavalink_ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!h.worker.status().gateway_ready);
    let (status, text) = call(&h, "/readyz", "").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{text}");
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(
        json["error"]["code"].is_string() && json["error"]["message"].is_string(),
        "{text}"
    );
    ready.send(straight_rs_worker::GatewayEvent::Ready).unwrap();
    wait_ready(&h).await;
    let (status, _) = call(&h, "/readyz", "").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn readyz_is_503_when_only_lavalink_is_unready_then_200() {
    let mock = MockLavalink::start_paused().await;
    let release = mock.release_ready();
    let h = app_with(FakeGateway, mock).await;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !h.worker.status().gateway_ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!h.worker.status().lavalink_ready);
    let (status, text) = call(&h, "/readyz", "").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{text}");
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(
        json["error"]["code"].is_string() && json["error"]["message"].is_string(),
        "{text}"
    );
    release.send(true).unwrap();
    wait_ready(&h).await;
    let (status, _) = call(&h, "/readyz", "").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn load_empty_is_404_and_error_is_generic_502() {
    let h = app().await;
    wait_ready(&h).await;
    join_and_wait(&h, 1, 2).await;
    h.mock
        .set_load_body(serde_json::json!({"loadType":"empty","data":{}}));
    let (status, text) = call(&h, "/v1/guilds/1/play", r#"{"identifier":"x"}"#).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{text}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&text).unwrap()["error"]["code"],
        "not_found"
    );
    h.mock.set_load_body(serde_json::json!({"loadType":"error","data":{"message":"synthetic-provider-secret","severity":"fault","cause":"c"}}));
    let (status, text) = call(&h, "/v1/guilds/1/play", r#"{"identifier":"x"}"#).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{text}");
    assert!(!text.contains("synthetic"), "{text}");
    assert!(!text.contains("provider"), "{text}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&text).unwrap()["error"]["code"],
        "internal_error"
    );
}

#[tokio::test]
async fn track_search_and_playlist_selection_return_204() {
    let h = app().await;
    wait_ready(&h).await;
    join_and_wait(&h, 1, 2).await;
    let bodies = [
        serde_json::json!({"loadType":"track","data":synthetic_track("enc-track")}),
        serde_json::json!({"loadType":"search","data":[synthetic_track("enc-first"), synthetic_track("enc-second")]}),
        serde_json::json!({"loadType":"playlist","data":{"info":{"name":"p","selectedTrack":1},"pluginInfo":{},"tracks":[synthetic_track("enc-a"), synthetic_track("enc-b")]}}),
        serde_json::json!({"loadType":"playlist","data":{"info":{"name":"p","selectedTrack":-1},"pluginInfo":{},"tracks":[synthetic_track("enc-c"), synthetic_track("enc-d")]}}),
    ];
    let expected = ["enc-track", "enc-first", "enc-b", "enc-c"];
    for (body, want) in bodies.into_iter().zip(expected) {
        h.mock.set_load_body(body);
        let (status, text) = call(&h, "/v1/guilds/1/play", r#"{"identifier":"x"}"#).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{text}");
        let last = h
            .mock
            .requests()
            .into_iter()
            .rev()
            .find(|r| r.method == "PATCH")
            .unwrap();
        assert_eq!(last.body["track"]["encoded"], want);
    }
}

#[tokio::test]
async fn stop_never_leaves_voice_but_leave_does() {
    let gateway = RecordingGateway {
        echo: true,
        ..Default::default()
    };
    let calls = gateway.calls.clone();
    let h = app_with(gateway, MockLavalink::start().await).await;
    wait_ready(&h).await;
    join_and_wait(&h, 1, 2).await;
    h.mock
        .set_load_body(serde_json::json!({"loadType":"track","data":synthetic_track("enc")}));
    let (status, text) = call(&h, "/v1/guilds/1/play", r#"{"identifier":"x"}"#).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{text}");
    let (status, text) = call(&h, "/v1/guilds/1/stop", "").await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{text}");
    assert_eq!(calls.lock().unwrap().len(), 1, "stop must not touch voice");
    assert!(calls.lock().unwrap()[0].1.is_some());
    assert!(!h.mock.requests().iter().any(|r| r.method == "DELETE"));
    let (status, text) = call(&h, "/v1/guilds/1/leave", "").await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{text}");
    let recorded = calls.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2);
    assert!(recorded[1].1.is_none());
}

#[tokio::test]
async fn player_json_omits_user_data_plugin_info_and_voice_credentials() {
    let h = app().await;
    wait_ready(&h).await;
    join_and_wait(&h, 1, 2).await;
    h.mock
        .set_load_body(serde_json::json!({"loadType":"track","data":synthetic_track("enc")}));
    let (status, text) = call(&h, "/v1/guilds/1/play", r#"{"identifier":"x"}"#).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{text}");
    let (status, text) = call(&h, "/v1/guilds/1/player", "").await;
    assert_eq!(status, StatusCode::OK, "{text}");
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["track"]["identifier"], "id1", "{text}");
    for forbidden in [
        "userData",
        "pluginInfo",
        "synthetic-user-secret",
        "synthetic-plugin-secret",
        "synthetic-voice",
        "token",
        "sessionId",
        "endpoint",
        "synthetic-bot-secret",
        "synthetic-lavalink-secret",
        TOKEN,
    ] {
        assert!(!text.contains(forbidden), "{forbidden} leaked: {text}");
    }
}

#[tokio::test]
async fn unknown_guild_player_is_404_without_creating_state() {
    let h = app().await;
    wait_ready(&h).await;
    let before = h.mock.requests().len();
    for _ in 0..2 {
        let (status, text) = call(&h, "/v1/guilds/777/player", "").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{text}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&text).unwrap()["error"]["code"],
            "not_found"
        );
    }
    assert_eq!(h.mock.requests().len(), before, "no Lavalink traffic");
    // A later read is still 404: the GET did not create a player.
    let (status, _) = call(&h, "/v1/guilds/777/player", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

fn code_of(text: &str) -> String {
    serde_json::from_str::<serde_json::Value>(text).unwrap()["error"]["code"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

#[tokio::test]
async fn mutating_routes_on_unknown_guild_are_404_and_create_no_state() {
    let h = app().await;
    wait_ready(&h).await;
    let cases = [
        ("/v1/guilds/777/pause", r#"{"paused":true}"#),
        ("/v1/guilds/777/resume", ""),
        ("/v1/guilds/777/seek", r#"{"position_ms":5}"#),
        ("/v1/guilds/777/volume", r#"{"volume":50}"#),
        ("/v1/guilds/777/stop", ""),
    ];
    for (uri, body) in cases {
        let (status, text) = call(&h, uri, body).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {text}");
        assert_eq!(code_of(&text), "not_found", "{uri}: {text}");
        let (status, text) = call(&h, "/v1/guilds/777/player", "").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "after {uri}: {text}");
    }
    assert!(
        !h.mock
            .requests()
            .iter()
            .any(|r| (r.method == "PATCH" || r.method == "DELETE") && r.path.contains("777")),
        "no player mutation may reach Lavalink"
    );
}

#[tokio::test]
async fn join_and_leave_do_not_create_player_state_until_a_voice_event() {
    let gateway = RecordingGateway::default();
    let calls = gateway.calls.clone();
    let h = app_with(gateway, MockLavalink::start().await).await;
    wait_ready(&h).await;
    let (status, text) = call(&h, "/v1/guilds/777/join", r#"{"channel_id":"5"}"#).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{text}");
    let (status, _) = call(&h, "/v1/guilds/777/player", "").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "join must not create a player"
    );
    let (status, text) = call(&h, "/v1/guilds/777/leave", "").await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{text}");
    let (status, _) = call(&h, "/v1/guilds/777/player", "").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "leave must not create a player"
    );
    let recorded = calls.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2, "{recorded:?}");
    assert_eq!(recorded[0].0, straight_rs_model::GuildId(777));
    assert_eq!(recorded[0].1, Some(straight_rs_model::ChannelId(5)));
    assert_eq!(recorded[1].0, straight_rs_model::GuildId(777));
    assert!(recorded[1].1.is_none());
}

#[tokio::test]
async fn play_requires_an_active_voice_session_and_skips_load_without_one() {
    let gateway = ControlledGateway::new(false);
    let events = gateway.events.clone();
    let h = app_with(gateway, MockLavalink::start().await).await;
    wait_ready(&h).await;
    h.mock
        .set_load_body(serde_json::json!({"loadType":"track","data":synthetic_track("enc")}));
    let (status, text) = call(&h, "/v1/guilds/888/play", r#"{"identifier":"x"}"#).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{text}");
    assert_eq!(code_of(&text), "not_found", "{text}");
    assert_eq!(h.mock.load_requests(), 0, "load must not run without voice");
    let (status, _) = call(&h, "/v1/guilds/888/player", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    events
        .send(common::fake_gateway::voice_state(
            straight_rs_model::GuildId(888),
            Some(straight_rs_model::ChannelId(3)),
        ))
        .unwrap();
    wait_player(&h, 888).await;
    let (status, text) = call(&h, "/v1/guilds/888/play", r#"{"identifier":"x"}"#).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{text}");
    assert_eq!(h.mock.load_requests(), 1);
}

#[tokio::test]
async fn stalled_request_body_hits_the_deadline_with_structured_504() {
    let h = app_with_timeout(
        FakeGateway,
        MockLavalink::start().await,
        Some(std::time::Duration::from_millis(100)),
    )
    .await;
    wait_ready(&h).await;
    let stalled = Request::post("/v1/guilds/1/play")
        .header("content-type", "application/json")
        .header("authorization", auth_header())
        .extension(ConnectInfo(
            "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
        ))
        .body(Body::from_stream(futures_util::stream::pending::<
            Result<axum::body::Bytes, std::convert::Infallible>,
        >()))
        .unwrap();
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        h.router.clone().oneshot(stalled),
    )
    .await
    .expect("stalled body must be cut off by the API deadline")
    .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let text = body_text(response).await;
    assert_eq!(code_of(&text), "deadline", "{text}");
    // A normal request under the same config is unaffected.
    let (status, text) = call(&h, "/v1/guilds/1/player", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{text}");
    assert_eq!(code_of(&text), "not_found");
}
