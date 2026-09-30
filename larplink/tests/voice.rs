mod common;
use common::*;
use larplink::{
    BoxFuture, ChannelId, GuildId, LavalinkClient, NodeConfig, Result, UserId, VoiceGateway,
    VoiceServerUpdate, VoiceState, VoiceStateUpdate,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const P7: &str = "/v4/sessions/mock-session/players/7";
fn su(ch: Option<u64>) -> VoiceStateUpdate {
    VoiceStateUpdate {
        channel_id: ch.map(ChannelId),
        session_id: "sess".into(),
    }
}
fn sv(e: Option<&str>) -> VoiceServerUpdate {
    VoiceServerUpdate {
        token: "tok".into(),
        endpoint: e.map(Into::into),
    }
}
fn voice_body() -> serde_json::Value {
    json!({"token":"tok","endpoint":"e:443","sessionId":"sess","channelId":"9"})
}

#[tokio::test]
async fn state_then_server_sends_exactly_one_patch() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    assert!(
        mock.requests_matching("PATCH", P7).is_empty(),
        "must wait for the server update"
    );
    c.voice_server_update(GuildId(7), sv(Some("e:443")))
        .await
        .unwrap();
    let r = mock.requests_matching("PATCH", P7);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].body["voice"], voice_body());
}

#[tokio::test]
async fn server_before_state_works() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_server_update(GuildId(7), sv(Some("e:443")))
        .await
        .unwrap();
    assert!(mock.requests_matching("PATCH", P7).is_empty());
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    assert_eq!(
        mock.requests_matching("PATCH", P7)[0].body["voice"],
        voice_body()
    );
}

#[tokio::test]
async fn null_endpoint_sends_nothing_and_duplicates_are_suppressed() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    c.voice_server_update(GuildId(7), sv(None)).await.unwrap();
    assert!(mock.requests_matching("PATCH", P7).is_empty());
    c.voice_server_update(GuildId(7), sv(Some("e:443")))
        .await
        .unwrap();
    c.voice_server_update(GuildId(7), sv(Some("e:443")))
        .await
        .unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7).len(), 1);
    c.voice_server_update(GuildId(7), sv(Some("other:443")))
        .await
        .unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7).len(), 2);
}

#[tokio::test]
async fn leaving_destroys_the_player() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    c.voice_server_update(GuildId(7), sv(Some("e:443")))
        .await
        .unwrap();
    c.voice_state_update(GuildId(7), su(None)).await.unwrap();
    assert_eq!(mock.requests_matching("DELETE", P7).len(), 1);
    assert!(c.get_player(GuildId(7)).is_none());
}

#[tokio::test]
async fn leaving_a_never_joined_guild_is_a_noop() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(8), su(None)).await.unwrap();
    assert!(mock.requests_matching("DELETE", "/v4/sessions").is_empty());
    assert!(c.get_player(GuildId(8)).is_none());
}

#[tokio::test]
async fn direct_voice_update_for_complete_connections() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let vs = VoiceState {
        token: "tok".into(),
        endpoint: "e:443".into(),
        session_id: "sess".into(),
        channel_id: Some(ChannelId(9)),
    };
    c.voice_update(GuildId(7), vs).await.unwrap();
    assert_eq!(
        mock.requests_matching("PATCH", P7)[0].body["voice"],
        voice_body()
    );
}

struct Fake(Mutex<Vec<String>>);
impl VoiceGateway for Fake {
    fn join(&self, g: GuildId, c: ChannelId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(format!("join {g} {c}"));
            Ok(())
        })
    }
    fn leave(&self, g: GuildId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(format!("leave {g}"));
            Ok(())
        })
    }
}

#[tokio::test]
async fn join_and_leave_go_through_the_gateway() {
    let mock = Mock::start().await;
    let fake = Arc::new(Fake(Mutex::new(vec![])));
    let c = LavalinkClient::builder(UserId(1))
        .node(NodeConfig::new(mock.host(), "pw"))
        .gateway(fake.clone())
        .build()
        .await
        .unwrap();
    c.wait_ready(Duration::from_secs(5)).await.unwrap();
    let p = c.player(GuildId(7));
    p.join(ChannelId(9)).await.unwrap();
    p.leave().await.unwrap();
    assert_eq!(*fake.0.lock().unwrap(), vec!["join 7 9", "leave 7"]);
}

#[tokio::test]
async fn voice_assembled_while_no_node_is_ready_is_sent_with_the_next_write() {
    let mock = Mock::start().await;
    let addr = mock.addr.to_string();
    let c = client(&[&mock]).await;
    mock.kill();
    eventually(Duration::from_secs(5), || !c.nodes()[0].is_ready()).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    let e = c
        .voice_server_update(GuildId(7), sv(Some("e:443")))
        .await
        .unwrap_err();
    assert!(matches!(e, larplink::Error::NoNode), "{e:?}");
    let back = Mock::start_on(&addr, false).await;
    eventually(Duration::from_secs(10), || c.nodes()[0].is_ready()).await;
    c.player(GuildId(7))
        .play(&sample_track("ABC"))
        .await
        .unwrap();
    let r = back.requests_matching("PATCH", P7);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].body["voice"], voice_body());
    assert_eq!(r[0].body["track"]["encoded"], "ABC");
    // Delivered now: an identical update is a duplicate.
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    assert_eq!(back.requests_matching("PATCH", P7).len(), 1);
}

#[tokio::test]
async fn failed_voice_patch_is_retried_by_an_identical_update() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    mock.state
        .fail_next_patches
        .store(1, std::sync::atomic::Ordering::SeqCst);
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    let e = c
        .voice_server_update(GuildId(7), sv(Some("e:443")))
        .await
        .unwrap_err();
    assert!(
        matches!(e, larplink::Error::Lavalink { status: 500, .. }),
        "{e:?}"
    );
    c.voice_server_update(GuildId(7), sv(Some("e:443")))
        .await
        .unwrap();
    let r = mock.requests_matching("PATCH", P7);
    assert_eq!(r.len(), 2);
    assert_eq!(r[1].body["voice"], voice_body());
    c.voice_server_update(GuildId(7), sv(Some("e:443")))
        .await
        .unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7).len(), 2);
}

/// Concurrent voice handlers: whatever ends up assembled last is what the
/// node got last (assembly and send happen under the player gate).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_voice_updates_leave_the_node_with_the_last_assembled_state() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    for round in 0..5 {
        let tasks: Vec<_> = (0..32)
            .map(|i| {
                let c = c.clone();
                tokio::spawn(async move {
                    let ep = format!("r{round}-e{i}:443");
                    c.voice_server_update(GuildId(7), sv(Some(&ep))).await
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        let before = mock.requests_matching("PATCH", P7).len();
        // Re-feeding the (unchanged) state half re-evaluates the assembled
        // state: it must equal what was sent last, so nothing is sent.
        c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
        assert_eq!(
            mock.requests_matching("PATCH", P7).len(),
            before,
            "round {round}: node was left with a stale voice state"
        );
    }
    assert_eq!(
        mock.state
            .max_in_flight
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}
