mod common;
use common::*;
use larplink::{Error, Event};
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

#[tokio::test]
async fn connects_with_auth_headers_and_enables_resume() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let h = mock.state.ws_headers.lock().unwrap()[0].clone();
    assert_eq!(h["authorization"].to_str().unwrap(), "pw");
    assert_eq!(h["user-id"].to_str().unwrap(), "1");
    assert!(h["client-name"].to_str().unwrap().starts_with("larplink/"));
    assert!(h.get("session-id").is_none());
    assert_eq!(c.nodes()[0].session_id().unwrap().as_str(), "mock-session");
    eventually(Duration::from_secs(5), || !mock.requests_matching("PATCH", "/v4/sessions/mock-session").is_empty()).await;
    let r = &mock.requests_matching("PATCH", "/v4/sessions/mock-session")[0];
    assert_eq!(r.body, serde_json::json!({"resuming": true, "timeout": 60}));
}

#[tokio::test]
async fn reconnects_after_drop_and_sends_session_id() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let mut rx = c.events();
    mock.close_ws();
    next_event(&mut rx, |e| matches!(e, Event::NodeDisconnected { .. })).await;
    eventually(Duration::from_secs(5), || mock.state.ws_connections.load(SeqCst) == 2).await;
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
    assert!(matches!(c.load("x").await.unwrap(), larplink::LoadResult::Empty(_)));
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
    let mut b = larplink::LavalinkClient::builder(larplink::UserId(1));
    b = b.node(larplink::NodeConfig::new("127.0.0.1:1", "pw"));
    let c = b.build().await.unwrap();
    assert!(matches!(c.wait_ready(Duration::from_millis(300)).await.unwrap_err(), Error::Timeout));
}

#[tokio::test]
async fn builder_requires_a_node() {
    let r = larplink::LavalinkClient::builder(larplink::UserId(1)).build().await;
    assert!(matches!(r, Err(Error::Config(_))));
}
