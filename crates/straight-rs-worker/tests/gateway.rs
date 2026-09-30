use std::time::Duration;
use straight_rs::{ChannelId, GuildId, VoiceGateway};
use straight_rs_worker::{GatewayCommand, GatewayEvent, GatewayVoiceProxy, WorkerError};
use tokio::sync::{mpsc, oneshot};

#[tokio::test]
async fn join_sends_channel_and_waits_for_acknowledgement() {
    let (tx, mut rx) = mpsc::channel(2);
    let proxy = GatewayVoiceProxy::new(tx, Duration::from_secs(1));
    let task = tokio::spawn(async move { proxy.join(GuildId(1), ChannelId(2)).await });
    match rx.recv().await.unwrap() {
        GatewayCommand::SetVoiceState {
            guild,
            channel,
            reply,
        } => {
            assert_eq!(guild, GuildId(1));
            assert_eq!(channel, Some(ChannelId(2)));
            reply.send(Ok(())).unwrap();
        }
    }
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test]
async fn leave_sends_no_channel() {
    let (tx, mut rx) = mpsc::channel(2);
    let proxy = GatewayVoiceProxy::new(tx, Duration::from_secs(1));
    let task = tokio::spawn(async move { proxy.leave(GuildId(3)).await });
    match rx.recv().await.unwrap() {
        GatewayCommand::SetVoiceState { channel, reply, .. } => {
            assert_eq!(channel, None);
            reply.send(Ok(())).unwrap();
        }
    }
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test]
async fn closed_driver_channel_returns_error() {
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let proxy = GatewayVoiceProxy::new(tx, Duration::from_secs(1));
    assert!(proxy.join(GuildId(1), ChannelId(2)).await.is_err());
}

#[test]
fn voice_event_contains_update() {
    let event = GatewayEvent::VoiceState {
        guild: GuildId(4),
        update: straight_rs::VoiceStateUpdate {
            channel_id: Some(ChannelId(5)),
            session_id: "session".into(),
        },
    };
    assert!(matches!(
        event,
        GatewayEvent::VoiceState {
            guild: GuildId(4),
            ..
        }
    ));
}

#[allow(dead_code)]
fn _typed_error(_: WorkerError) {}
#[allow(dead_code)]
fn _oneshot_type(_: oneshot::Sender<()>) {}
