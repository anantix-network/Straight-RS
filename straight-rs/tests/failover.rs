mod common;
use common::*;
use std::time::Duration;
use straight_rs::{ChannelId, Error, Event, GuildId, VoiceServerUpdate, VoiceStateUpdate};

const G: &str = "/v4/sessions/mock-session/players/42";
const LONG: Duration = Duration::from_secs(20);

async fn join_and_play(c: &straight_rs::LavalinkClient) -> straight_rs::Player {
    let p = c.player(GuildId(42));
    c.voice_state_update(
        GuildId(42),
        VoiceStateUpdate {
            channel_id: Some(ChannelId(9)),
            session_id: "sess".into(),
        },
    )
    .await
    .unwrap();
    c.voice_server_update(
        GuildId(42),
        VoiceServerUpdate {
            token: "tok".into(),
            endpoint: Some("e:443".into()),
        },
    )
    .await
    .unwrap();
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
    let c = client_with(&[&a, &b], |cfg| {
        cfg.failover_grace = Duration::from_millis(300)
    })
    .await;
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
    // Grace must exceed the first reconnect delay (<= 500ms backoff) but stay
    // short enough that the failover timer really fires during the test.
    let c = client_with(&[&a, &b], |cfg| {
        cfg.failover_grace = Duration::from_millis(1000)
    })
    .await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    let t0 = tokio::time::Instant::now();
    a.close_ws(); // drops the socket; the server stays up so it reconnects fast
    next_event(&mut rx, |e| matches!(e, Event::NodeConnected { .. })).await;
    tokio::time::sleep_until(t0 + Duration::from_millis(1800)).await; // well past the grace
    assert_eq!(p.node_index(), Some(0));
    assert!(b.requests_matching("PATCH", G).is_empty());
    while let Ok(e) = rx.try_recv() {
        assert!(!matches!(e, Event::PlayerMigrated { .. }));
    }
}

/// The timer of an earlier outage must not migrate players during a later,
/// still-within-grace outage (epoch check).
#[tokio::test]
async fn timer_of_earlier_outage_does_not_migrate_during_a_new_outage() {
    let (a, b) = (Mock::start().await, Mock::start().await);
    let c = client_with(&[&a, &b], |cfg| {
        cfg.failover_grace = Duration::from_millis(2000)
    })
    .await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    let t0 = tokio::time::Instant::now();
    a.close_ws(); // outage 1: reconnects within ~500ms
    next_event(&mut rx, |e| matches!(e, Event::NodeConnected { .. })).await;
    tokio::time::sleep_until(t0 + Duration::from_millis(1000)).await;
    a.kill(); // outage 2 starts at ~1000ms; its grace ends at ~3000ms
              // Outage 1's timer fires at ~2000ms with the node down again.
    tokio::time::sleep_until(t0 + Duration::from_millis(2600)).await;
    assert_eq!(p.node_index(), Some(0));
    assert!(b.requests_matching("PATCH", G).is_empty());
    while let Ok(e) = rx.try_recv() {
        assert!(!matches!(e, Event::PlayerMigrated { .. }));
    }
}

#[tokio::test]
async fn orphaned_players_are_rescued_when_the_node_returns() {
    let mock = Mock::start().await;
    let addr = mock.addr.to_string();
    let c = client_with(&[&mock], |cfg| {
        cfg.failover_grace = Duration::from_millis(200)
    })
    .await;
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
    let c = client_with(&[&a, &b], |cfg| {
        cfg.failover_grace = Duration::from_millis(300)
    })
    .await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    a.kill();
    next_event(&mut rx, |e| matches!(e, Event::PlayerMigrated { .. })).await;
    let a2 = Mock::start_on(&a_addr, true).await; // server kept the old session
    eventually(LONG, || !a2.requests_matching("DELETE", G).is_empty()).await;
    assert_eq!(p.node_index(), Some(1));
}

#[tokio::test]
async fn orphan_is_rescued_on_exactly_one_of_two_returning_nodes() {
    let (a, b) = (Mock::start().await, Mock::start().await);
    let (a_addr, b_addr) = (a.addr.to_string(), b.addr.to_string());
    let c = client_with(&[&a, &b], |cfg| {
        cfg.failover_grace = Duration::from_millis(200)
    })
    .await;
    let _p = join_and_play(&c).await;
    let mut rx = c.events();
    a.kill();
    b.kill();
    eventually(LONG, || c.nodes().iter().all(|n| !n.is_ready())).await;
    tokio::time::sleep(Duration::from_millis(600)).await; // grace elapsed, orphaned
    let (a2, b2) = (
        Mock::start_on(&a_addr, false).await,
        Mock::start_on(&b_addr, false).await,
    );
    eventually(LONG, || {
        !a2.requests_matching("PATCH", G).is_empty() || !b2.requests_matching("PATCH", G).is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await; // let both nodes settle
    let total = a2.requests_matching("PATCH", G).len() + b2.requests_matching("PATCH", G).len();
    assert_eq!(total, 1, "player must be created on exactly one node");
    let mut migrated = 0;
    while let Ok(e) = rx.try_recv() {
        if matches!(e, Event::PlayerMigrated { .. }) {
            migrated += 1;
        }
    }
    assert!(migrated <= 1, "got {migrated} PlayerMigrated events");
}

#[tokio::test]
async fn destroyed_player_is_not_resurrected_when_node_returns() {
    let mock = Mock::start().await;
    let addr = mock.addr.to_string();
    let c = client_with(&[&mock], |cfg| {
        cfg.failover_grace = Duration::from_millis(200)
    })
    .await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    mock.kill();
    eventually(LONG, || !c.nodes()[0].is_ready()).await;
    tokio::time::sleep(Duration::from_millis(500)).await; // orphaned
    let _ = p.destroy().await;
    let back = Mock::start_on(&addr, false).await;
    next_event(&mut rx, |e| matches!(e, Event::NodeConnected { .. })).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(back.requests_matching("PATCH", G).is_empty());
    while let Ok(e) = rx.try_recv() {
        assert!(!matches!(e, Event::PlayerMigrated { .. }));
    }
}

#[tokio::test]
async fn pending_failover_timer_does_not_migrate_after_shutdown() {
    let (a, b) = (Mock::start().await, Mock::start().await);
    let c = client_with(&[&a, &b], |cfg| {
        cfg.failover_grace = Duration::from_millis(300)
    })
    .await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    a.kill();
    next_event(&mut rx, |e| {
        matches!(e, Event::NodeDisconnected { node: 0 })
    })
    .await;
    c.shutdown(); // the failover timer of node 0 is still pending
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(b.requests_matching("PATCH", G).is_empty());
    assert_eq!(p.node_index(), Some(0));
    while let Ok(e) = rx.try_recv() {
        assert!(!matches!(e, Event::PlayerMigrated { .. }));
    }
}

/// I1: a user write that reaches a node after a lost session, before the
/// restore ran, must carry the restore state instead of clobbering it.
#[tokio::test]
async fn write_racing_a_session_restore_carries_the_restore_state() {
    use std::sync::atomic::Ordering::SeqCst;
    let mock = Mock::start().await;
    mock.state.forget_players_on_new_session.store(true, SeqCst);
    let c = client_with(&[&mock], |cfg| {
        cfg.request_timeout = Duration::from_secs(10)
    })
    .await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    // w1 holds the player's gate across the reconnect (slow, then fails),
    // so w2 is queued on the gate ahead of the restore task.
    mock.state.fail_next_patches.store(1, SeqCst);
    mock.state.patch_delay_ms.store(1500, SeqCst);
    let before = mock.requests_matching("PATCH", G).len();
    let (a, b) = (p.clone(), p.clone());
    let w1 = tokio::spawn(async move { a.set_volume(50).await });
    eventually(LONG, || mock.requests_matching("PATCH", G).len() > before).await;
    mock.state.patch_delay_ms.store(0, SeqCst);
    let w2 = tokio::spawn(async move { b.set_volume(55).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    mock.close_ws(); // reconnects with resumed=false: a blank session
    next_event(&mut rx, |e| {
        matches!(e, Event::Ready { resumed: false, .. })
    })
    .await;
    assert!(w1.await.unwrap().is_err());
    w2.await.unwrap().unwrap();
    let patches = mock.requests_matching("PATCH", G);
    let w2_body = &patches
        .iter()
        .find(|r| r.body["volume"] == 55)
        .expect("w2 PATCH")
        .body;
    assert_eq!(w2_body["track"]["encoded"], "ABC");
    assert_eq!(w2_body["voice"]["token"], "tok");
    assert_eq!(&*p.track().unwrap().encoded, "ABC");
    assert_eq!(p.volume(), 55);
    // The pending restore sees the player already rebuilt on this session.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(mock.requests_matching("PATCH", G).len(), patches.len());
    assert_eq!(&*p.track().unwrap().encoded, "ABC");
    assert_eq!(p.volume(), 55);
}

/// I5: a player orphaned by a failed migration is moved inline by the next
/// write while a healthy node exists.
#[tokio::test]
async fn orphan_from_a_failed_migration_is_rescued_by_the_next_write() {
    use std::sync::atomic::Ordering::SeqCst;
    let (a, b) = (Mock::start().await, Mock::start().await);
    let c = client_with(&[&a, &b], |cfg| {
        cfg.failover_grace = Duration::from_millis(300)
    })
    .await;
    let p = join_and_play(&c).await;
    assert_eq!(p.node_index(), Some(0));
    b.state.fail_next_patches.store(1, SeqCst);
    let mut rx = c.events();
    a.kill();
    eventually(LONG, || !b.requests_matching("PATCH", G).is_empty()).await;
    tokio::time::sleep(Duration::from_millis(200)).await; // migration failed -> orphaned
    assert_eq!(p.node_index(), Some(0));
    p.set_volume(30).await.unwrap();
    assert_eq!(p.node_index(), Some(1));
    let last = b.requests_matching("PATCH", G).pop().unwrap().body;
    assert_eq!(last["track"]["encoded"], "ABC");
    assert_eq!(last["voice"]["token"], "tok");
    assert_eq!(last["volume"], 30);
    let e = next_event(&mut rx, |e| matches!(e, Event::PlayerMigrated { .. })).await;
    assert!(matches!(e, Event::PlayerMigrated { from: 0, to: 1, .. }));
    assert_eq!(&*p.track().unwrap().encoded, "ABC");
}

/// I3: destroying a player while its node is down deletes it once the node
/// comes back with the old session.
#[tokio::test]
async fn destroy_while_node_is_down_deletes_when_it_resumes() {
    let mock = Mock::start().await;
    let addr = mock.addr.to_string();
    let c = client_with(&[&mock], |cfg| cfg.failover_grace = LONG).await;
    let p = join_and_play(&c).await;
    mock.kill();
    eventually(LONG, || !c.nodes()[0].is_ready()).await;
    p.destroy().await.unwrap();
    assert!(c.get_player(GuildId(42)).is_none());
    let back = Mock::start_on(&addr, true).await; // session (and player) survived
    eventually(LONG, || !back.requests_matching("DELETE", G).is_empty()).await;
    assert!(back.requests_matching("PATCH", G).is_empty());
}

/// M2: stale cleanup checks and deletes under the player's gate, so it
/// cannot interleave with a write in flight.
#[tokio::test]
async fn stale_cleanup_waits_for_the_player_gate() {
    use std::sync::atomic::Ordering::SeqCst;
    let (a, b) = (Mock::start().await, Mock::start().await);
    let a_addr = a.addr.to_string();
    let c = client_with(&[&a, &b], |cfg| {
        cfg.failover_grace = Duration::from_millis(300);
        cfg.request_timeout = Duration::from_secs(10);
    })
    .await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    a.kill();
    next_event(&mut rx, |e| matches!(e, Event::PlayerMigrated { .. })).await;
    b.state.patch_delay_ms.store(2500, SeqCst);
    let q = p.clone();
    let w = tokio::spawn(async move { q.set_volume(9).await });
    eventually(LONG, || b.state.in_flight.load(SeqCst) == 1).await;
    let a2 = Mock::start_on(&a_addr, true).await;
    eventually(LONG, || !a2.requests_matching("DELETE", G).is_empty()).await;
    assert!(
        w.is_finished(),
        "stale DELETE was sent while a write held the player's gate"
    );
    w.await.unwrap().unwrap();
    assert_eq!(p.node_index(), Some(1));
}

/// M3: a migration that completes after the old node already came back
/// (resumed) must not leave the player running there too.
#[tokio::test]
async fn migration_finishing_after_the_old_node_resumed_cleans_it_up() {
    use std::sync::atomic::Ordering::SeqCst;
    let (a, b) = (Mock::start().await, Mock::start().await);
    let a_addr = a.addr.to_string();
    let c = client_with(&[&a, &b], |cfg| {
        cfg.failover_grace = Duration::from_millis(300);
        cfg.request_timeout = Duration::from_secs(15);
    })
    .await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    b.state.patch_delay_ms.store(5000, SeqCst);
    a.kill();
    // Migration PATCH to b is in flight (slow) ...
    eventually(LONG, || !b.requests_matching("PATCH", G).is_empty()).await;
    // ... while node 0 comes back with its old session.
    let a2 = Mock::start_on(&a_addr, true).await;
    next_event(&mut rx, |e| matches!(e, Event::NodeConnected { node: 0 })).await;
    next_event(&mut rx, |e| matches!(e, Event::PlayerMigrated { .. })).await;
    assert_eq!(p.node_index(), Some(1));
    eventually(LONG, || !a2.requests_matching("DELETE", G).is_empty()).await;
}
