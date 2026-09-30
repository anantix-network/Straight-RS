#![cfg(feature = "e2e")]
//! Needs a real Lavalink v4: `docker run -p 2333:2333 -e SERVER_PORT=2333 ghcr.io/lavalink-devs/lavalink:4`
//! Run: `cargo test -p straight-rs --features e2e -- --ignored`
use std::time::Duration;
use straight_rs::{GuildId, LavalinkClient, NodeConfig, UserId};

async fn connect() -> LavalinkClient {
    let host = std::env::var("LAVALINK_HOST").unwrap_or_else(|_| "127.0.0.1:2333".into());
    let pw = std::env::var("LAVALINK_PASSWORD").unwrap_or_else(|_| "youshallnotpass".into());
    let c = LavalinkClient::builder(UserId(1))
        .node(NodeConfig::new(host, pw))
        .build()
        .await
        .unwrap();
    c.wait_ready(Duration::from_secs(10)).await.unwrap();
    c
}

#[tokio::test]
#[ignore]
async fn real_server_info_version_and_stats() {
    let c = connect().await;
    let n = &c.nodes()[0];
    assert_eq!(n.info().await.unwrap().version.major, 4);
    assert!(n.version().await.unwrap().starts_with('4'));
    assert!(n.rest().stats().await.is_ok());
}

#[tokio::test]
#[ignore]
async fn real_server_load_and_player_lifecycle() {
    let c = connect().await;
    assert!(c.load("https://example.invalid/none.mp3").await.is_ok());
    let p = c.player(GuildId(1));
    p.set_volume(50).await.unwrap();
    assert_eq!(p.volume(), 50);
    p.destroy().await.unwrap();
}
