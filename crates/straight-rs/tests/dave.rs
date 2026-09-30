//! DAVE (Discord's E2EE voice protocol) support. The encryption itself runs inside
//! Lavalink >= 4.2.0; the client must send `channelId` and understand close code 4017.
mod common;
use common::*;
use straight_rs::{ChannelId, Event, GuildId, VoiceState};

async fn supports(major: u32, minor: u32, patch: u32) -> bool {
    let mock = Mock::start().await;
    mock.set_version(major, minor, patch);
    let c = client(&[&mock]).await;
    c.nodes()[0].supports_dave().await.unwrap()
}

#[tokio::test]
async fn dave_needs_lavalink_4_2_or_newer() {
    assert!(!supports(4, 0, 8).await);
    assert!(!supports(4, 1, 9).await);
    assert!(supports(4, 2, 0).await);
    assert!(supports(4, 10, 1).await, "minor 10 is newer than minor 2");
    assert!(supports(5, 0, 0).await);
    assert!(!supports(3, 9, 0).await);
}

#[tokio::test]
async fn websocket_closed_4017_is_recognised_as_dave_required() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let mut rx = c.events();
    mock.push_text(r#"{"op":"event","type":"WebSocketClosedEvent","guildId":"1","code":4017,"reason":"E2EE required","byRemote":true}"#);
    let e = next_event(&mut rx, |e| matches!(e, Event::WebSocketClosed { .. })).await;
    assert!(e.is_dave_required());
    mock.push_text(r#"{"op":"event","type":"WebSocketClosedEvent","guildId":"1","code":4006,"reason":"x","byRemote":true}"#);
    let e = next_event(&mut rx, |e| {
        matches!(e, Event::WebSocketClosed { code: 4006, .. })
    })
    .await;
    assert!(!e.is_dave_required());
}

#[tokio::test]
async fn every_voice_patch_carries_the_channel_id() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let vs = VoiceState {
        token: "tok".into(),
        endpoint: "e:443".into(),
        session_id: "sess".into(),
        channel_id: ChannelId(9),
    };
    c.voice_update(GuildId(7), vs).await.unwrap();
    let patches = mock.requests_matching("PATCH", "/v4/sessions/mock-session/players/7");
    assert_eq!(patches[0].body["voice"]["channelId"], "9");
}
