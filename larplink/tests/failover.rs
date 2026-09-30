mod common;
use common::*;
use larplink::{ChannelId, Error, Event, GuildId, VoiceServerUpdate, VoiceStateUpdate};
use std::time::Duration;

const G: &str = "/v4/sessions/mock-session/players/42";
const LONG: Duration = Duration::from_secs(20);

async fn join_and_play(c: &larplink::LavalinkClient) -> larplink::Player {
    let p = c.player(GuildId(42));
    c.voice_state_update(GuildId(42), VoiceStateUpdate { channel_id: Some(ChannelId(9)), session_id: "sess".into() }).await.unwrap();
    c.voice_server_update(GuildId(42), VoiceServerUpdate { token: "tok".into(), endpoint: Some("e:443".into()) }).await.unwrap();
    p.play(&sample_track("ABC")).await.unwrap();
    p
}

#[tokio::test]
async fn resumed_false_recreates_players_on_the_same_node() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let _p = join_and_play(&c).await;
    let before = mock.requests_matching("PATCH", G).len();
    mock.close_ws(); // mock says resumed=false on reconnect
    eventually(LONG, || mock.requests_matching("PATCH", G).len() > before).await;
    let last = mock.requests_matching("PATCH", G).pop().unwrap().body;
    assert_eq!(last["track"]["encoded"], "ABC");
    assert_eq!(last["voice"]["token"], "tok");
    assert_eq!(last["volume"], 100);
    assert_eq!(last["paused"], false);
}

#[tokio::test]
async fn resumed_true_does_not_resend_state() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let _p = join_and_play(&c).await;
    let before = mock.requests_matching("PATCH", G).len();
    let mut rx = c.events();
    mock.set_resumed(true);
    mock.close_ws();
    next_event(&mut rx, |e| matches!(e, Event::NodeConnected { .. })).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(mock.requests_matching("PATCH", G).len(), before);
}

#[tokio::test]
async fn players_migrate_to_another_node_after_grace() {
    let (a, b) = (Mock::start().await, Mock::start().await);
    let c = client_with(&[&a, &b], |cfg| cfg.failover_grace = Duration::from_millis(300)).await;
    let p = join_and_play(&c).await;
    assert_eq!(p.node_index(), Some(0));
    let mut rx = c.events();
    a.kill();
    let e = next_event(&mut rx, |e| matches!(e, Event::PlayerMigrated { .. })).await;
    assert!(matches!(e, Event::PlayerMigrated { from: 0, to: 1, .. }));
    assert_eq!(p.node_index(), Some(1));
    let last = b.requests_matching("PATCH", G).pop().unwrap().body;
    assert_eq!(last["track"]["encoded"], "ABC");
    assert_eq!(last["voice"]["token"], "tok");
    // player keeps working on the new node
    p.set_volume(30).await.unwrap();
}

#[tokio::test]
async fn short_outage_within_grace_does_not_migrate() {
    let (a, b) = (Mock::start().await, Mock::start().await);
    let c = client_with(&[&a, &b], |cfg| cfg.failover_grace = Duration::from_secs(30)).await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    a.close_ws(); // drops the socket; the server stays up so it reconnects
    next_event(&mut rx, |e| matches!(e, Event::NodeConnected { .. })).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(p.node_index(), Some(0));
    assert!(b.requests_matching("PATCH", G).is_empty());
}

#[tokio::test]
async fn orphaned_players_are_rescued_when_the_node_returns() {
    let mock = Mock::start().await;
    let addr = mock.addr.to_string();
    let c = client_with(&[&mock], |cfg| cfg.failover_grace = Duration::from_millis(200)).await;
    let p = join_and_play(&c).await;
    mock.kill();
    eventually(LONG, || !c.nodes()[0].is_ready()).await;
    tokio::time::sleep(Duration::from_millis(500)).await; // grace elapsed, nowhere to go
    assert!(matches!(p.pause(true).await.unwrap_err(), Error::NoNode));
    let back = Mock::start_on(&addr, false).await;
    eventually(LONG, || !back.requests_matching("PATCH", G).is_empty()).await;
    let body = back.requests_matching("PATCH", G).pop().unwrap().body;
    assert_eq!(body["track"]["encoded"], "ABC");
    assert_eq!(body["voice"]["token"], "tok");
    p.pause(true).await.unwrap(); // usable again
}

#[tokio::test]
async fn migrated_away_player_is_deleted_when_old_node_returns_resumed() {
    let (a, b) = (Mock::start().await, Mock::start().await);
    let a_addr = a.addr.to_string();
    let c = client_with(&[&a, &b], |cfg| cfg.failover_grace = Duration::from_millis(300)).await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    a.kill();
    next_event(&mut rx, |e| matches!(e, Event::PlayerMigrated { .. })).await;
    let a2 = Mock::start_on(&a_addr, true).await; // server kept the old session
    eventually(LONG, || !a2.requests_matching("DELETE", G).is_empty()).await;
    assert_eq!(p.node_index(), Some(1));
}
