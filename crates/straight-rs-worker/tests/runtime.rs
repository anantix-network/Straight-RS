#[path = "common/fake_gateway.rs"]
mod fake_gateway;
#[path = "common/mock_lavalink.rs"]
mod mock_lavalink;

use std::time::Duration;
use straight_rs::{ChannelId, NodeConfig, VoiceServerUpdate, VoiceStateUpdate};
use straight_rs_model::{GuildId, UserId};
use straight_rs_worker::{
    GatewayEvent, SecretString, WorkerBuilder, WorkerConfigBuilder, WorkerError,
};
use tokio::sync::watch;
use tower::ServiceExt;

fn config(host: String) -> straight_rs_worker::WorkerConfig {
    WorkerConfigBuilder::new(
        UserId(9),
        SecretString::new("bot-token"),
        SecretString::new("api-token-that-is-at-least-thirty-two-bytes"),
        vec![NodeConfig::new(host, "pw")],
    )
    .build()
    .unwrap()
}
async fn wait_ready(worker: &straight_rs_worker::RunningWorker) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !worker.status().ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("readiness deadline elapsed");
}
async fn wait_gateway_ready(worker: &straight_rs_worker::RunningWorker, expected: bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while worker.status().gateway_ready != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("gateway readiness deadline elapsed");
}
async fn wait_lavalink_ready(worker: &straight_rs_worker::RunningWorker, expected: bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while worker.status().lavalink_ready != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Lavalink readiness deadline elapsed");
}
async fn assert_no_player_patch(mock: &mock_lavalink::MockLavalink) {
    assert!(
        !mock
            .requests()
            .iter()
            .any(|r| r.method == "PATCH" && r.path.contains("/players/")),
        "unexpected player PATCH"
    );
}
async fn player_patches(
    mock: &mock_lavalink::MockLavalink,
    count: usize,
) -> Vec<mock_lavalink::Recorded> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let patches: Vec<_> = mock
                .requests()
                .into_iter()
                .filter(|r| r.method == "PATCH" && r.path.contains("/players/"))
                .collect();
            if patches.len() >= count {
                break patches;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("player PATCH deadline elapsed")
}
fn state(channel: Option<u64>, session: &str) -> VoiceStateUpdate {
    VoiceStateUpdate {
        channel_id: channel.map(ChannelId),
        session_id: session.into(),
    }
}
fn server(token: &str) -> VoiceServerUpdate {
    VoiceServerUpdate {
        token: token.into(),
        endpoint: Some("voice.example:443".into()),
    }
}

#[tokio::test]
async fn readiness_requires_gateway_and_lavalink_ready() {
    let lavalink = mock_lavalink::MockLavalink::start().await;
    let mut worker = WorkerBuilder::new(config(lavalink.host()), fake_gateway::FakeGateway)
        .build()
        .await
        .unwrap();
    wait_ready(&worker).await;
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn readiness_waits_for_gateway_ready_event() {
    let lavalink = mock_lavalink::MockLavalink::start().await;
    let gateway = fake_gateway::ControlledGateway::withheld_ready();
    let ready = gateway.ready.clone();
    let mut worker = WorkerBuilder::new(config(lavalink.host()), gateway)
        .build()
        .await
        .unwrap();
    wait_lavalink_ready(&worker, true).await;
    wait_gateway_ready(&worker, false).await;
    assert!(!worker.status().ready);
    ready.send(GatewayEvent::Ready).unwrap();
    wait_ready(&worker).await;
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn readiness_tracks_disconnect_resume_and_gateway_stream_close() {
    let lavalink = mock_lavalink::MockLavalink::start().await;
    let gateway = fake_gateway::ControlledGateway::new(false);
    let events = gateway.events.clone();
    let mut worker = WorkerBuilder::new(config(lavalink.host()), gateway)
        .build()
        .await
        .unwrap();
    wait_ready(&worker).await;

    events.send(GatewayEvent::Disconnected).unwrap();
    wait_gateway_ready(&worker, false).await;
    assert!(!worker.status().ready);

    events.send(GatewayEvent::Ready).unwrap();
    wait_gateway_ready(&worker, true).await;
    wait_ready(&worker).await;
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn gateway_driver_exit_clears_readiness() {
    let lavalink = mock_lavalink::MockLavalink::start().await;
    let (exit, exit_rx) = watch::channel(false);
    let mut worker = WorkerBuilder::new(
        config(lavalink.host()),
        fake_gateway::ReadyThenExitGateway { exit: exit_rx },
    )
    .build()
    .await
    .unwrap();
    exit.send(true).unwrap();
    wait_gateway_ready(&worker, false).await;
    assert!(!worker.status().ready);

    let response = worker
        .router()
        .oneshot(
            axum::http::Request::builder()
                .uri("/readyz")
                .header(
                    "authorization",
                    "Bearer api-token-that-is-at-least-thirty-two-bytes",
                )
                .extension(axum::extract::ConnectInfo(
                    "127.0.0.1:54321".parse::<std::net::SocketAddr>().unwrap(),
                ))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let _ = worker.shutdown().await;
}

#[tokio::test]
async fn readiness_waits_for_lavalink_ready_handshake() {
    let lavalink = mock_lavalink::MockLavalink::start_paused().await;
    let release = lavalink.release_ready();
    let mut worker = WorkerBuilder::new(config(lavalink.host()), fake_gateway::FakeGateway)
        .build()
        .await
        .unwrap();
    wait_gateway_ready(&worker, true).await;
    wait_lavalink_ready(&worker, false).await;
    assert!(!worker.status().ready);
    release.send(true).unwrap();
    wait_ready(&worker).await;
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn non_bot_voice_state_is_ignored_and_bot_state_reaches_lavalink() {
    let lavalink = mock_lavalink::MockLavalink::start().await;
    let gateway = fake_gateway::ControlledGateway::new(false);
    let events = gateway.events.clone();
    let mut worker = WorkerBuilder::new(config(lavalink.host()), gateway)
        .build()
        .await
        .unwrap();
    wait_ready(&worker).await;
    events
        .send(GatewayEvent::VoiceState {
            user_id: UserId(10),
            guild: GuildId(44),
            update: state(Some(2), "other"),
        })
        .unwrap();
    events
        .send(GatewayEvent::VoiceServer {
            guild: GuildId(44),
            update: server("foreign-token"),
        })
        .unwrap();
    events.send(GatewayEvent::Disconnected).unwrap();
    wait_gateway_ready(&worker, false).await;
    assert_no_player_patch(&lavalink).await;
    events
        .send(GatewayEvent::VoiceState {
            user_id: UserId(9),
            guild: GuildId(44),
            update: state(Some(3), "bot-session"),
        })
        .unwrap();
    let patches = player_patches(&lavalink, 1).await;
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].body["voice"]["sessionId"], "bot-session");
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn voice_state_server_arrival_orders_both_forward_complete_state() {
    for (guild, state_first) in [(GuildId(51), true), (GuildId(52), false)] {
        let lavalink = mock_lavalink::MockLavalink::start().await;
        let gateway = fake_gateway::ControlledGateway::new(false);
        let events = gateway.events.clone();
        let mut worker = WorkerBuilder::new(config(lavalink.host()), gateway)
            .build()
            .await
            .unwrap();
        wait_ready(&worker).await;
        let state_event = GatewayEvent::VoiceState {
            user_id: UserId(9),
            guild,
            update: state(Some(8), "complete-session"),
        };
        let server_event = GatewayEvent::VoiceServer {
            guild,
            update: server("complete-token"),
        };
        if state_first {
            events.send(state_event).unwrap();
            events.send(server_event).unwrap();
        } else {
            events.send(server_event).unwrap();
            events.send(state_event).unwrap();
        }
        let patches = player_patches(&lavalink, 1).await;
        assert_eq!(patches.len(), 1);
        assert_eq!(
            patches[0].body["voice"],
            serde_json::json!({"token":"complete-token","endpoint":"voice.example:443","sessionId":"complete-session","channelId":"8"})
        );
        worker.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn serve_shutdown_signals_gateway_and_completes_owned_task_join() {
    let lavalink = mock_lavalink::MockLavalink::start().await;
    let gateway = fake_gateway::ControlledGateway::new(false);
    let stopped = gateway.stopped.clone();
    let mut worker = WorkerBuilder::new(config(lavalink.host()), gateway)
        .build()
        .await
        .unwrap();
    wait_ready(&worker).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tx, rx) = watch::channel(false);
    let serve = tokio::spawn(async move { worker.serve_on(listener, rx).await });
    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), serve)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(stopped.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn gateway_error_does_not_skip_relay_join() {
    let lavalink = mock_lavalink::MockLavalink::start().await;
    let gateway = fake_gateway::ControlledGateway::new(true);
    let stopped = gateway.stopped.clone();
    let mut worker = WorkerBuilder::new(config(lavalink.host()), gateway)
        .build()
        .await
        .unwrap();
    wait_ready(&worker).await;
    let err = worker.shutdown().await.unwrap_err();
    assert!(matches!(err, WorkerError::Gateway(_)));
    assert!(stopped.load(std::sync::atomic::Ordering::SeqCst));
}
