mod common;
use common::*;
use larplink::{Error, Event, Filters, GuildId};
use serde_json::json;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

const G: &str = "/v4/sessions/mock-session/players/42";

fn track_end(enc: &str, reason: &str) -> String {
    format!(r#"{{"op":"event","type":"TrackEndEvent","guildId":"42","track":{},"reason":"{reason}"}}"#, track_json(enc))
}

#[tokio::test]
async fn play_sends_patch_and_updates_snapshot() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    let r = mock.requests_matching("PATCH", G);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].body["track"]["encoded"], "ABC");
    assert_eq!(r[0].query, "noReplace=false");
    assert_eq!(&*p.track().unwrap().encoded, "ABC");
    assert_eq!(p.node_index(), Some(0));
}

#[tokio::test]
async fn stop_pause_seek_volume_filters() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    p.pause(true).await.unwrap();
    assert!(p.is_paused());
    p.seek(5_000).await.unwrap();
    assert_eq!(p.snapshot().position, 5_000);
    p.set_volume(5_000).await.unwrap();
    assert_eq!(p.volume(), 1000);
    let f = Filters { volume: Some(0.5), ..Default::default() };
    p.set_filters(f.clone()).await.unwrap();
    assert_eq!(p.snapshot().filters, f);
    p.stop().await.unwrap();
    assert!(p.track().is_none());
    let bodies: Vec<_> = mock.requests_matching("PATCH", G).into_iter().map(|r| r.body).collect();
    assert_eq!(bodies[1], json!({"paused": true}));
    assert_eq!(bodies[2], json!({"position": 5000}));
    assert_eq!(bodies[3], json!({"volume": 1000}));
    assert_eq!(bodies[4], json!({"filters": {"volume": 0.5}}));
    assert_eq!(bodies[5], json!({"track": {"encoded": null}}));
}

#[tokio::test]
async fn position_interpolates_between_updates_and_freezes_when_paused() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    mock.push_text(r#"{"op":"playerUpdate","guildId":"42","state":{"time":0,"position":10000,"connected":true,"ping":5}}"#);
    eventually(Duration::from_secs(5), || p.position() >= 10_000).await;
    let a = p.position();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(p.position() >= a + 90);
    p.pause(true).await.unwrap();
    let frozen = p.position();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(p.position(), frozen);
}

#[tokio::test]
async fn writes_for_one_guild_never_overlap_and_keep_call_order() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.set_volume(1).await.unwrap(); // assign node
    mock.state.patch_delay_ms.store(150, SeqCst);
    let (a, b) = (p.clone(), p.clone());
    let t1 = tokio::spawn(async move { a.set_volume(10).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let t2 = tokio::spawn(async move { b.set_volume(20).await.unwrap() });
    t1.await.unwrap();
    t2.await.unwrap();
    assert_eq!(mock.state.max_in_flight.load(SeqCst), 1);
    let vols: Vec<_> = mock.requests_matching("PATCH", G).iter().map(|r| r.body["volume"].as_u64().unwrap()).collect();
    assert_eq!(vols, vec![1, 10, 20]);
    assert_eq!(p.volume(), 20);
}

#[tokio::test]
async fn track_end_clears_track_except_when_replaced_or_other_track() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    let mut rx = c.events();
    p.play(&sample_track("ABC")).await.unwrap();
    mock.push_text(track_end("ABC", "replaced"));
    next_event(&mut rx, |e| matches!(e, Event::TrackEnd { .. })).await;
    assert!(p.track().is_some());
    mock.push_text(track_end("OTHER", "finished"));
    next_event(&mut rx, |e| matches!(e, Event::TrackEnd { .. })).await;
    assert!(p.track().is_some());
    mock.push_text(track_end("ABC", "finished"));
    let e = next_event(&mut rx, |e| matches!(e, Event::TrackEnd { .. })).await;
    assert!(e.may_start_next());
    eventually(Duration::from_secs(5), || p.track().is_none()).await;
}

#[tokio::test]
async fn player_events_only_yield_own_guild() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    let mut ev = p.events();
    let other = format!(r#"{{"op":"event","type":"TrackStartEvent","guildId":"43","track":{}}}"#, track_json("X"));
    let mine = format!(r#"{{"op":"event","type":"TrackStartEvent","guildId":"42","track":{}}}"#, track_json("Y"));
    mock.push_text(other);
    mock.push_text(mine);
    let e = tokio::time::timeout(Duration::from_secs(5), ev.recv()).await.unwrap().unwrap();
    assert_eq!(e.guild(), Some(GuildId(42)));
}

#[tokio::test]
async fn destroy_deletes_on_node_and_forgets_player() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    p.destroy().await.unwrap();
    assert_eq!(mock.requests_matching("DELETE", G).len(), 1);
    assert!(c.get_player(GuildId(42)).is_none());
}

#[tokio::test]
async fn writes_fail_with_no_node_when_pool_is_down() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    mock.kill();
    eventually(Duration::from_secs(5), || !c.nodes()[0].is_ready()).await;
    let e = c.player(GuildId(1)).play(&sample_track("A")).await.unwrap_err();
    assert!(matches!(e, Error::NoNode));
}

#[tokio::test]
async fn join_without_gateway_is_a_config_error() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let e = c.player(GuildId(1)).join(larplink::ChannelId(2)).await.unwrap_err();
    assert!(matches!(e, Error::Config(_)));
}

#[tokio::test]
async fn write_queued_behind_destroy_fails_and_does_not_recreate() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    mock.state.delete_delay_ms.store(200, SeqCst);
    let (a, b) = (p.clone(), p.clone());
    let d = tokio::spawn(async move { a.destroy().await.unwrap() });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let w = tokio::spawn(async move { b.set_volume(7).await });
    d.await.unwrap();
    let e = w.await.unwrap().unwrap_err();
    assert!(matches!(e, Error::PlayerNotFound));
    let reqs = mock.requests_matching("PATCH", G);
    assert_eq!(reqs.len(), 1, "no PATCH after DELETE");
    assert_eq!(mock.requests_matching("DELETE", G).len(), 1);
}

#[tokio::test]
async fn player_after_destroy_is_fresh_and_works() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    p.destroy().await.unwrap();
    let q = c.player(GuildId(42));
    q.play(&sample_track("DEF")).await.unwrap();
    assert_eq!(&*q.track().unwrap().encoded, "DEF");
    assert!(matches!(p.pause(true).await, Err(Error::PlayerNotFound)));
}
