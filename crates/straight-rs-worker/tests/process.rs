#![cfg(feature = "test-support")]

use axum::http::StatusCode;
use std::{
    net::SocketAddr,
    process::{Command, Output},
    sync::{Arc, Mutex},
    time::Duration,
};
use straight_rs::{NodeConfig, VoiceServerUpdate, VoiceStateUpdate};
use straight_rs_model::{ChannelId, GuildId, UserId};
use straight_rs_worker::{
    GatewayCommand, GatewayDriver, GatewayEvent, GatewayFuture, SecretString, WorkerBuilder,
    WorkerConfigBuilder, WorkerError,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};

#[path = "common/mock_lavalink.rs"]
mod mock_lavalink;
use mock_lavalink::{MockLavalink, synthetic_track};

const API_TOKEN: &str = "synthetic-process-test-api-token-32-bytes";
const GUILD_ID: &str = "424242";
const CHANNEL_ID: &str = "313131";
const TRACK_IDENTIFIER: &str = "synthetic:process-test-track";

#[derive(Clone, Default)]
struct VoiceGateway {
    joins: Arc<Mutex<Vec<(GuildId, Option<ChannelId>)>>>,
}

impl GatewayDriver for VoiceGateway {
    fn run<'a>(
        &'a self,
        _token: SecretString,
        _bot_user_id: UserId,
        mut commands: mpsc::Receiver<GatewayCommand>,
        events: mpsc::Sender<GatewayEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> GatewayFuture<'a> {
        let joins = self.joins.clone();
        Box::pin(async move {
            events
                .send(GatewayEvent::Ready)
                .await
                .map_err(|_| WorkerError::GatewayClosed)?;
            loop {
                tokio::select! {
                    _ = shutdown.changed() => return Ok(()),
                    command = commands.recv() => match command {
                        Some(GatewayCommand::SetVoiceState { guild, channel, reply }) => {
                            joins.lock().unwrap().push((guild, channel));
                            let _ = reply.send(Ok(()));
                            if let Some(channel_id) = channel {
                                events.send(GatewayEvent::VoiceState {
                                    user_id: UserId(9),
                                    guild,
                                    update: VoiceStateUpdate { channel_id: Some(channel_id), session_id: "synthetic-session".into() },
                                }).await.map_err(|_| WorkerError::GatewayClosed)?;
                                events.send(GatewayEvent::VoiceServer {
                                    guild,
                                    update: VoiceServerUpdate { token: "synthetic-voice-token".into(), endpoint: Some("voice.example:443".into()) },
                                }).await.map_err(|_| WorkerError::GatewayClosed)?;
                            }
                        }
                        None => return Ok(()),
                    }
                }
            }
        })
    }
}

fn run_probe(executable: &str, base_url: &str, operation: &str) -> Output {
    Command::new(executable)
        .env("WORKER_API_BASE_URL", base_url)
        .env("WORKER_API_TOKEN", API_TOKEN)
        .env("GUILD_ID", GUILD_ID)
        .env("CHANNEL_ID", CHANNEL_ID)
        .env("TRACK_IDENTIFIER", TRACK_IDENTIFIER)
        .arg(operation)
        .output()
        .expect("child probe should start")
}

fn child_success(output: Output) -> String {
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("child output should be UTF-8")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_process_exit_keeps_worker_player_and_voice_snapshot_alive() {
    let mock = MockLavalink::start().await;
    mock.set_load_body(serde_json::json!({
        "loadType": "track",
        "data": synthetic_track("synthetic-process-test-encoded-track")
    }));

    let gateway = VoiceGateway::default();
    let joins = gateway.joins.clone();
    let config = WorkerConfigBuilder::new(
        UserId(9),
        SecretString::new("synthetic-process-test-bot-token"),
        SecretString::new(API_TOKEN),
        vec![NodeConfig::new(
            mock.host(),
            "synthetic-process-test-lavalink-token",
        )],
    )
    .bind_addr("127.0.0.1:0".parse().unwrap(), false)
    .build()
    .unwrap();
    let mut worker = WorkerBuilder::new(config, gateway).build().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !worker.status().ready {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker readiness deadline elapsed");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let serving = tokio::spawn(async move { worker.serve_on(listener, shutdown_rx).await });
    let base_url = format!("http://{address}");
    let executable = env!("CARGO_BIN_EXE_worker-client-probe");

    child_success(run_probe(executable, &base_url, "join-play"));
    let player = query_player(&base_url).await;
    assert_eq!(player.status, StatusCode::OK);
    assert_eq!(player.body["connected"], true);
    assert_eq!(player.body["voiceChannelId"], CHANNEL_ID);
    assert_eq!(player.body["track"]["identifier"], "id1");
    assert_eq!(
        joins.lock().unwrap().as_slice(),
        &[(GuildId(424242), Some(ChannelId(313131)))]
    );

    let requests_before_query = mock.requests();
    child_success(run_probe(executable, &base_url, "query"));
    let requests_after_query = mock.requests();
    assert_eq!(
        requests_after_query.len(),
        requests_before_query.len(),
        "read-only query must not mutate Lavalink"
    );
    assert!(
        !requests_after_query
            .iter()
            .any(|request| request.method == "DELETE")
    );
    assert_eq!(
        joins.lock().unwrap().len(),
        1,
        "child exit must not issue a gateway leave"
    );

    let retained = query_player(&base_url).await;
    assert_eq!(retained.status, StatusCode::OK);
    assert_eq!(retained.body["connected"], true);
    assert_eq!(retained.body["voiceChannelId"], CHANNEL_ID);
    assert_eq!(retained.body["track"]["identifier"], "id1");
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("worker server shutdown deadline elapsed")
        .unwrap()
        .unwrap();
}

struct PlayerResponse {
    status: StatusCode,
    body: serde_json::Value,
}

async fn query_player(base_url: &str) -> PlayerResponse {
    let authority = base_url.strip_prefix("http://").unwrap();
    let mut stream = tokio::net::TcpStream::connect(authority).await.unwrap();
    stream.write_all(format!("GET /v1/guilds/{GUILD_ID}/player HTTP/1.1\r\nHost: {authority}\r\nAuthorization: Bearer {API_TOKEN}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let (head, body) = response
        .split_once("\r\n\r\n")
        .expect("valid HTTP response");
    let status = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<u16>()
        .unwrap();
    PlayerResponse {
        status: StatusCode::from_u16(status).unwrap(),
        body: serde_json::from_str(body).expect("player response JSON"),
    }
}
