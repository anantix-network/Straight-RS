#[path = "common/fake_gateway.rs"]
mod fake_gateway;

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

#[tokio::test]
async fn gateway_run_relays_voice_state_event_to_receiver() {
    use straight_rs_worker::{GatewayDriver, SecretString};

    let (commands_tx, commands_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(1);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let driver = fake_gateway::FakeGateway;
    let driver_task = tokio::spawn(async move {
        driver
            .run(
                SecretString::new("token"),
                straight_rs::UserId(9),
                commands_rx,
                events_tx,
                shutdown_rx,
            )
            .await
    });
    let event = tokio::time::timeout(Duration::from_secs(1), events_rx.recv())
        .await
        .expect("voice-state event deadline elapsed")
        .expect("driver closed event channel");
    assert!(matches!(event, GatewayEvent::Ready));
    let (reply, reply_rx) = tokio::sync::oneshot::channel();
    commands_tx
        .send(GatewayCommand::SetVoiceState {
            guild: GuildId(4),
            channel: Some(ChannelId(5)),
            reply,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), reply_rx)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), events_rx.recv())
        .await
        .expect("voice-state event deadline elapsed")
        .expect("driver closed event channel");
    assert!(
        matches!(event, GatewayEvent::VoiceState { user_id: straight_rs::UserId(9), guild: GuildId(4), update } if update.channel_id == Some(ChannelId(5)))
    );
    drop(commands_tx);
    driver_task.abort();
}

#[tokio::test]
async fn closed_driver_channel_returns_gateway_error_variant() {
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let proxy = GatewayVoiceProxy::new(tx, Duration::from_secs(1));
    let error = proxy.join(GuildId(1), ChannelId(2)).await.unwrap_err();
    assert!(matches!(error, straight_rs::Error::Gateway(_)));
}

#[tokio::test]
async fn gateway_timeout_returns_gateway_error_variant() {
    let (tx, mut rx) = mpsc::channel(1);
    let proxy = GatewayVoiceProxy::new(tx, Duration::from_millis(1));
    let task = tokio::spawn(async move { proxy.join(GuildId(1), ChannelId(2)).await });
    let command = rx.recv().await.unwrap();
    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(error, straight_rs::Error::Gateway(_)));
    drop(command);
}

#[test]
fn voice_event_contains_update() {
    let event = GatewayEvent::VoiceState {
        user_id: straight_rs::UserId(9),
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
