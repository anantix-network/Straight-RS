//! Quick start (compiled by `cargo build --examples`, not run in CI).
//! Run against a local Lavalink v4: `cargo run -p larplink --example quickstart`
use larplink::{
    ChannelId, GuildId, LavalinkClient, LoadResult, NodeConfig, Strategy, UserId,
    VoiceServerUpdate, VoiceStateUpdate,
};

#[tokio::main]
async fn main() -> larplink::Result<()> {
    // In a real bot these come from your Discord library's voice events.
    let bot_id = 1u64;
    let guild_id = GuildId(2);
    let channel = ChannelId(3);
    let session_id = String::from("session");
    let token = String::from("token");
    let endpoint = String::from("example.discord.media:443");

    let client = LavalinkClient::builder(UserId(bot_id))
        .node(NodeConfig::new("127.0.0.1:2333", "youshallnotpass"))
        .strategy(Strategy::LeastPenalty)
        .build()
        .await?;
    client
        .wait_ready(std::time::Duration::from_secs(10))
        .await?;

    client
        .voice_state_update(
            guild_id,
            VoiceStateUpdate {
                channel_id: Some(channel),
                session_id,
            },
        )
        .await?;
    client
        .voice_server_update(
            guild_id,
            VoiceServerUpdate {
                token,
                endpoint: Some(endpoint),
            },
        )
        .await?;

    if let LoadResult::Search(tracks) = client.load("ytsearch:never gonna give you up").await? {
        if let Some(track) = tracks.first() {
            client.player(guild_id).play(track).await?;
        }
    }
    let mut events = client.events();
    while let Ok(event) = events.recv().await {
        println!("{event:?}");
    }
    Ok(())
}
