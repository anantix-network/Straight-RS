#[path = "common/fake_gateway.rs"]
mod fake_gateway;

use std::time::Duration;
use straight_rs::{ChannelId, NodeConfig};
use straight_rs_model::{GuildId, UserId};
use straight_rs_worker::{GatewayEvent, SecretString, WorkerBuilder, WorkerConfigBuilder};

#[tokio::test]
async fn readiness_requires_gateway_and_lavalink_ready() {
    let lavalink = mock_lavalink::MockLavalink::start().await;
    let gateway = fake_gateway::FakeGateway;
    let config = WorkerConfigBuilder::new(
        UserId(9),
        SecretString::new("bot-token"),
        SecretString::new("api-token"),
        vec![NodeConfig::new(lavalink.host(), "pw")],
    )
    .build()
    .unwrap();
    let mut worker = WorkerBuilder::new(config, gateway).build().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if worker.status().ready {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker readiness deadline elapsed");
    worker.shutdown().await.unwrap();
    let _ = (GatewayEvent::Ready, ChannelId(1), GuildId(1));
}

#[path = "common/mock_lavalink.rs"]
mod mock_lavalink;
