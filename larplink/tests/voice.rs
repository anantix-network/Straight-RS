mod common;
use common::*;
use larplink::{BoxFuture, ChannelId, GuildId, LavalinkClient, NodeConfig, Result, UserId, VoiceGateway, VoiceServerUpdate, VoiceState, VoiceStateUpdate};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const P7: &str = "/v4/sessions/mock-session/players/7";
fn su(ch: Option<u64>) -> VoiceStateUpdate { VoiceStateUpdate { channel_id: ch.map(ChannelId), session_id: "sess".into() } }
fn sv(e: Option<&str>) -> VoiceServerUpdate { VoiceServerUpdate { token: "tok".into(), endpoint: e.map(Into::into) } }
fn voice_body() -> serde_json::Value { json!({"token":"tok","endpoint":"e:443","sessionId":"sess","channelId":"9"}) }

#[tokio::test]
async fn state_then_server_sends_exactly_one_patch() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    assert!(mock.requests_matching("PATCH", P7).is_empty(), "must wait for the server update");
    c.voice_server_update(GuildId(7), sv(Some("e:443"))).await.unwrap();
    let r = mock.requests_matching("PATCH", P7);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].body["voice"], voice_body());
}

#[tokio::test]
async fn server_before_state_works() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_server_update(GuildId(7), sv(Some("e:443"))).await.unwrap();
    assert!(mock.requests_matching("PATCH", P7).is_empty());
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7)[0].body["voice"], voice_body());
}

#[tokio::test]
async fn null_endpoint_sends_nothing_and_duplicates_are_suppressed() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    c.voice_server_update(GuildId(7), sv(None)).await.unwrap();
    assert!(mock.requests_matching("PATCH", P7).is_empty());
    c.voice_server_update(GuildId(7), sv(Some("e:443"))).await.unwrap();
    c.voice_server_update(GuildId(7), sv(Some("e:443"))).await.unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7).len(), 1);
    c.voice_server_update(GuildId(7), sv(Some("other:443"))).await.unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7).len(), 2);
}

#[tokio::test]
async fn leaving_destroys_the_player() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    c.voice_server_update(GuildId(7), sv(Some("e:443"))).await.unwrap();
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
    let vs = VoiceState { token: "tok".into(), endpoint: "e:443".into(), session_id: "sess".into(), channel_id: Some(ChannelId(9)) };
    c.voice_update(GuildId(7), vs).await.unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7)[0].body["voice"], voice_body());
}

struct Fake(Mutex<Vec<String>>);
impl VoiceGateway for Fake {
    fn join(&self, g: GuildId, c: ChannelId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.0.lock().unwrap().push(format!("join {g} {c}")); Ok(()) })
    }
    fn leave(&self, g: GuildId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.0.lock().unwrap().push(format!("leave {g}")); Ok(()) })
    }
}

#[tokio::test]
async fn join_and_leave_go_through_the_gateway() {
    let mock = Mock::start().await;
    let fake = Arc::new(Fake(Mutex::new(vec![])));
    let c = LavalinkClient::builder(UserId(1)).node(NodeConfig::new(mock.host(), "pw")).gateway(fake.clone()).build().await.unwrap();
    c.wait_ready(Duration::from_secs(5)).await.unwrap();
    let p = c.player(GuildId(7));
    p.join(ChannelId(9)).await.unwrap();
    p.leave().await.unwrap();
    assert_eq!(*fake.0.lock().unwrap(), vec!["join 7 9", "leave 7"]);
}
