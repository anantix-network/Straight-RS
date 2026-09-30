mod common;
use common::*;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;
use straight_rs::{Error, Event};

#[tokio::test]
async fn connects_with_auth_headers_and_enables_resume() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let h = mock.state.ws_headers.lock().unwrap()[0].clone();
    assert_eq!(h["authorization"].to_str().unwrap(), "pw");
    assert_eq!(h["user-id"].to_str().unwrap(), "1");
    assert!(
        h["client-name"]
            .to_str()
            .unwrap()
            .starts_with("straight-rs/")
    );
    assert!(h.get("session-id").is_none());
    assert_eq!(c.nodes()[0].session_id().unwrap().as_str(), "mock-session");
    eventually(Duration::from_secs(5), || {
        !mock
            .requests_matching("PATCH", "/v4/sessions/mock-session")
            .is_empty()
    })
    .await;
    let r = &mock.requests_matching("PATCH", "/v4/sessions/mock-session")[0];
    assert_eq!(r.body, serde_json::json!({"resuming": true, "timeout": 60}));
}

#[tokio::test]
async fn build_with_events_captures_immediate_ready_event() {
    let mock = Mock::start().await;
    let mut builder = straight_rs::LavalinkClient::builder(straight_rs::UserId(1));
    builder = builder.node(straight_rs::NodeConfig::new(mock.host(), "pw"));
    let (_client, mut events) = builder.build_with_events().await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("Ready was not captured")
        .expect("event channel closed");
    assert!(matches!(event, Event::Ready { .. }));
}

#[tokio::test]
async fn reconnects_after_drop_and_sends_session_id() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let mut rx = c.events();
    mock.close_ws();
    next_event(&mut rx, |e| matches!(e, Event::NodeDisconnected { .. })).await;
    eventually(Duration::from_secs(5), || {
        mock.state.ws_connections.load(SeqCst) == 2
    })
    .await;
    let h = mock.state.ws_headers.lock().unwrap()[1].clone();
    assert_eq!(h["session-id"].to_str().unwrap(), "mock-session");
    next_event(&mut rx, |e| matches!(e, Event::NodeConnected { .. })).await;
}

#[tokio::test]
async fn unknown_and_garbage_frames_do_not_kill_the_node() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let mut rx = c.events();
    mock.push_text("not json at all");
    mock.push_text(r#"{"op":"weird","x":1}"#);
    mock.push_text(r#"{"op":"event","type":"NewEvent","guildId":"1"}"#);
    mock.push_text(STATS_10);
    let a = next_event(&mut rx, |e| matches!(e, Event::Unknown { .. })).await;
    assert!(matches!(a, Event::Unknown { ref op, .. } if op == "weird"));
    let b = next_event(&mut rx, |e| matches!(e, Event::Unknown { .. })).await;
    assert!(matches!(b, Event::Unknown { ref op, .. } if op == "event"));
    next_event(&mut rx, |e| matches!(e, Event::Stats { .. })).await;
    assert_eq!(mock.state.ws_connections.load(SeqCst), 1);
    assert!(c.nodes()[0].is_ready());
}

#[tokio::test]
async fn stats_update_penalty_and_players() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    mock.push_text(STATS_10);
    eventually(Duration::from_secs(5), || c.nodes()[0].penalty() == 10).await;
    assert_eq!(c.nodes()[0].players(), 10);
    assert_eq!(c.nodes()[0].stats().unwrap().playing_players, 5);
}

#[tokio::test]
async fn load_uses_a_ready_node_and_fails_without_one() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    assert!(matches!(
        c.load("x").await.unwrap(),
        straight_rs::LoadResult::Empty(_)
    ));
    mock.kill();
    eventually(Duration::from_secs(5), || !c.nodes()[0].is_ready()).await;
    assert!(matches!(c.load("x").await.unwrap_err(), Error::NoNode));
}

#[tokio::test]
async fn node_info_and_version_are_available() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    assert_eq!(c.nodes()[0].version().await.unwrap(), "4.0.8");
    assert_eq!(c.nodes()[0].info().await.unwrap().version.major, 4);
}

#[tokio::test]
async fn wait_ready_times_out_when_nothing_listens() {
    let mut b = straight_rs::LavalinkClient::builder(straight_rs::UserId(1));
    b = b.node(straight_rs::NodeConfig::new("127.0.0.1:1", "pw"));
    let c = b.build().await.unwrap();
    assert!(matches!(
        c.wait_ready(Duration::from_millis(300)).await.unwrap_err(),
        Error::Timeout
    ));
}

#[tokio::test]
async fn builder_requires_a_node() {
    let r = straight_rs::LavalinkClient::builder(straight_rs::UserId(1))
        .build()
        .await;
    assert!(matches!(r, Err(Error::Config(_))));
}

async fn build_with(
    tweak: impl FnOnce(&mut straight_rs::NodeConfig),
    b: impl FnOnce(straight_rs::ClientBuilder) -> straight_rs::ClientBuilder,
) -> straight_rs::Result<straight_rs::LavalinkClient> {
    let mut cfg = straight_rs::NodeConfig::new("127.0.0.1:1", "pw");
    tweak(&mut cfg);
    b(straight_rs::LavalinkClient::builder(straight_rs::UserId(1)).node(cfg))
        .build()
        .await
}

fn assert_config_err(r: straight_rs::Result<straight_rs::LavalinkClient>, what: &str) {
    match r {
        Err(Error::Config(msg)) => assert!(msg.contains(what), "{what}: {msg}"),
        Err(e) => panic!("{what}: expected Config error, got {e:?}"),
        Ok(_) => panic!("{what}: expected Config error, got a client"),
    }
}

#[tokio::test]
async fn event_capacity_is_validated() {
    assert_config_err(
        build_with(|_| {}, |b| b.event_capacity(0)).await,
        "event_capacity",
    );
    assert_config_err(
        build_with(|_| {}, |b| b.event_capacity(usize::MAX)).await,
        "event_capacity",
    );
    assert!(build_with(|_| {}, |b| b.event_capacity(1)).await.is_ok());
}

#[tokio::test]
async fn ping_settings_are_validated() {
    assert_config_err(
        build_with(|c| c.ping_interval = Duration::ZERO, |b| b).await,
        "ping_interval",
    );
    assert_config_err(
        build_with(
            |c| {
                c.ping_interval = Duration::from_secs(10);
                c.ping_timeout = Duration::from_secs(5);
            },
            |b| b,
        )
        .await,
        "ping_timeout",
    );
    assert_config_err(
        build_with(
            |c| {
                c.ping_interval = Duration::MAX;
                c.ping_timeout = Duration::MAX;
            },
            |b| b,
        )
        .await,
        "ping_interval",
    );
}

#[tokio::test]
async fn invalid_header_values_are_config_errors_at_build() {
    assert_config_err(
        build_with(|c| c.password = "pw\nevil".into(), |b| b).await,
        "password",
    );
    assert_config_err(
        build_with(|_| {}, |b| b.client_name("bot\r\n")).await,
        "client_name",
    );
    assert_config_err(
        build_with(|c| c.host = "bad host/ x".into(), |b| b).await,
        "host",
    );
}

#[cfg(not(feature = "tls"))]
#[tokio::test]
async fn secure_node_without_tls_feature_is_a_config_error() {
    assert_config_err(build_with(|c| c.secure = true, |b| b).await, "tls");
}

#[tokio::test]
async fn shutdown_stops_the_client_and_its_players() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(straight_rs::GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    c.shutdown();
    assert!(!c.nodes()[0].is_ready());
    assert!(c.nodes()[0].session_id().is_none());
    let before = mock.requests().len();
    assert!(matches!(p.set_volume(5).await.unwrap_err(), Error::Closed));
    assert!(matches!(c.load("x").await.unwrap_err(), Error::Closed));
    assert!(matches!(
        c.player(straight_rs::GuildId(43))
            .play(&sample_track("A"))
            .await
            .unwrap_err(),
        Error::Closed
    ));
    assert!(matches!(p.destroy().await.unwrap_err(), Error::Closed));
    assert!(matches!(p.fetch().await.unwrap_err(), Error::Closed));
    assert!(matches!(
        c.wait_ready(Duration::from_millis(50)).await.unwrap_err(),
        Error::Closed
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(mock.requests().len(), before, "no requests after shutdown");
    assert!(!c.nodes()[0].is_ready());
}

#[tokio::test]
async fn dropping_the_last_client_closes_player_handles() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(straight_rs::GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    let before = mock.requests().len();
    drop(c);
    assert!(matches!(p.pause(true).await.unwrap_err(), Error::Closed));
    assert_eq!(mock.requests().len(), before);
}
