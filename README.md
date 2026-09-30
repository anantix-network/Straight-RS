# Straight-RS

A fast, easy-to-use **[Lavalink v4](https://lavalink.dev) client for Rust.**

Lavalink is a standalone audio server for Discord bots. Straight-RS is the
part that lives in your bot: it connects to one or more Lavalink servers,
loads tracks, controls playback, and keeps everything running when a server
restarts or drops.

- **Works with any Discord library.** Nothing is tied to serenity, twilight or
  songbird. Small optional adapters are included for all three.
- **Fast.** Reading a player's state never waits or locks. Parsing a Lavalink
  message takes well under a microsecond (see [Performance](#performance)).
- **Reliable.** Automatic reconnect, session resume, and moving players to
  another node when one goes down.
- **Complete.** All Lavalink v4 REST endpoints, filters and events, plus
  multi-node load balancing.

> Requires Rust 1.97 or newer (edition 2024) and a running Lavalink v4 server.

## Install

Straight-RS is not on crates.io yet, so add it from GitHub:

```toml
[dependencies]
straight-rs = { git = "https://github.com/anantix-network/Straight-RS" }
tokio = { version = "1", features = ["full"] }
```

Optional features:

| Feature    | What it adds                                           |
|------------|--------------------------------------------------------|
| `serenity` | Convert serenity voice events for Straight-RS          |
| `twilight` | Convert twilight voice events for Straight-RS          |
| `songbird` | Convert songbird connection info for Straight-RS       |
| `tls`      | Connect to `https://` / `wss://` Lavalink nodes        |

```toml
straight-rs = { git = "https://github.com/anantix-network/Straight-RS", features = ["twilight", "tls"] }
```

## Quick start

```rust
use straight_rs::{
    ChannelId, GuildId, LavalinkClient, LoadResult, NodeConfig, UserId,
    VoiceServerUpdate, VoiceStateUpdate,
};

#[tokio::main]
async fn main() -> straight_rs::Result<()> {
    // 1. Connect to your Lavalink server.
    let client = LavalinkClient::builder(UserId(YOUR_BOT_ID))
        .node(NodeConfig::new("127.0.0.1:2333", "youshallnotpass"))
        .build()
        .await?;
    client.wait_ready(std::time::Duration::from_secs(10)).await?;

    // 2. Tell Straight-RS which voice channel the bot joined.
    //    (These values come from your Discord library, see the next section.)
    let guild = GuildId(GUILD_ID);
    client.voice_state_update(guild, VoiceStateUpdate {
        channel_id: Some(ChannelId(CHANNEL_ID)),
        session_id,
    }).await?;
    client.voice_server_update(guild, VoiceServerUpdate {
        token,
        endpoint: Some(endpoint),
    }).await?;

    // 3. Search for a track and play it.
    if let LoadResult::Search(tracks) = client.load("ytsearch:never gonna give you up").await? {
        if let Some(track) = tracks.first() {
            client.player(guild).play(track).await?;
        }
    }
    Ok(())
}
```

A complete, compiling version is in
[`straight-rs/examples/quickstart.rs`](straight-rs/examples/quickstart.rs).

## Connecting your Discord library

Lavalink needs two pieces of information from Discord for each voice channel the
bot joins. Your Discord library receives them as events; you just pass them on:

| Discord event         | Call this                       |
|-----------------------|---------------------------------|
| `VOICE_STATE_UPDATE`  | `client.voice_state_update(...)`  |
| `VOICE_SERVER_UPDATE` | `client.voice_server_update(...)` |

- Only pass events for **your own bot's** voice state, not other users'.
- The two events can arrive in any order. Straight-RS waits until it has both,
  then sends them to Lavalink.
- When the bot leaves a channel (`channel_id: None`), Straight-RS removes the
  player from Lavalink for you.
- **songbird:** once it has a connection, pass it to `client.voice_update(...)`.
- **serenity / twilight / songbird features:** `straight_rs::adapters::*` has
  ready-made functions that turn each library's event types into the inputs above.
- **Joining and leaving channels:** implement the small `VoiceGateway` trait
  (`join` / `leave`) and pass it to `ClientBuilder::gateway`. Then
  `player.join(channel)` and `player.leave()` work.

## DAVE (end-to-end encrypted voice)

Since March 2026 Discord requires DAVE, its end-to-end encryption for voice.
The encryption itself runs inside Lavalink, so you only need:

- **Lavalink 4.2.0 or newer.** Older servers cannot speak DAVE and Discord closes
  their voice connections with code 4017. Straight-RS logs a warning when it
  connects to such a node, and `node.supports_dave().await` tells you in code.
- **The voice channel id with every voice state.** Straight-RS always sends it
  (`VoiceState::channel_id` is required), so there is nothing extra to do when
  you use `voice_state_update` / `voice_server_update` or the adapters.
- If Discord closes a voice connection with 4017 anyway, you receive
  `Event::WebSocketClosed`; `event.is_dave_required()` is true for it.

## Controlling playback

`client.player(guild)` gives you a cheap handle you can clone and share freely.

```rust
let player = client.player(guild);

player.play(&track).await?;        // start a track
player.pause(true).await?;         // pause (false to resume)
player.seek(30_000).await?;        // jump to 30 seconds (milliseconds)
player.set_volume(80).await?;      // 0 to 1000, 100 is normal
player.stop().await?;              // stop the current track
player.destroy().await?;           // remove the player completely
```

Filters (equalizer, speed, karaoke and so on):

```rust
use straight_rs::{Filters, model::Timescale};

player.set_filters(Filters {
    timescale: Some(Timescale { speed: Some(1.25), ..Default::default() }),
    ..Default::default()
}).await?;
```

Reading state is instant and never waits:

```rust
player.position();   // current position in ms, kept up to date between updates
player.track();      // the playing track, if any
player.is_paused();
player.volume();
```

Calls for the same server (guild) always run one at a time, in the order you
made them, so you never get two conflicting updates racing each other.

## Listening for events

```rust
let mut events = client.events();
while let Ok(event) = events.recv().await {
    if event.may_start_next() {
        // a song finished: play the next one from your own queue
    }
}
```

- `client.events()` covers every node; `player.events()` only one guild.
- Common events: `TrackStart`, `TrackEnd`, `TrackException`, `TrackStuck`,
  `PlayerUpdate`, `NodeConnected`, `NodeDisconnected`, `PlayerMigrated`.
  Plugin messages arrive as `Unknown`.
- The channel has a fixed size (`ClientBuilder::event_capacity`). If your code is
  too slow, `recv()` returns `RecvError::Lagged(n)` telling you how many events
  were skipped, and you can keep going.

Straight-RS does not include a queue. `TrackEnd` tells you when a song is over
(`event.may_start_next()`); what plays next is up to your bot.

## Using several Lavalink servers

Add more than one `.node(...)`. New players go to the best available node.

```rust
use straight_rs::Strategy;

let client = LavalinkClient::builder(UserId(bot_id))
    .node(NodeConfig::new("lavalink-1.example.com:2333", "password"))
    .node(NodeConfig::new("lavalink-2.example.com:2333", "password"))
    .strategy(Strategy::LeastPenalty)   // or RoundRobin, LeastPlayers, Custom(..)
    .build()
    .await?;
```

## What happens when something goes wrong

You don't have to handle reconnecting yourself.

- **Connection drops:** Straight-RS reconnects with increasing delays and asks
  Lavalink to resume the old session, so music keeps playing.
- **Lavalink restarted and forgot the session:** every player is rebuilt on the
  new session with its track, position, volume, filters and voice state.
- **A node stays down** (longer than `NodeConfig::failover_grace`, 10 seconds by
  default): its players move to another node and you get
  `Event::PlayerMigrated { guild, from, to }`.
- **No node is available:** players are kept and picked up again as soon as a
  node comes back. Calls made in the meantime return `Error::NoNode`.

## Settings and errors

`ClientBuilder::build()` returns `Error::Config` for settings that can never
work, for example no nodes, an `event_capacity` of 0, `secure = true` without the
`tls` feature, or a host or password that is not valid in an HTTP header.

`client.shutdown()` (or dropping the last copy of the client) closes all
connections. After that, calls return `Error::Closed`. Lavalink removes the
players on its own once the resume timeout passes.

## Not included

- A Discord library. Use serenity, twilight, songbird or your own.
- A track queue or playlist manager.
- Lavalink v3 or older.

## Performance

Measured with `cargo bench -p straight-rs` on the development machine:

| What                                     | Time     |
|------------------------------------------|----------|
| Parse a `playerUpdate` message           | ~0.42 µs |
| Parse a `stats` message                  | ~0.97 µs |
| Pick the best of 16 nodes                | ~14.6 ns |
| Apply a `playerUpdate` to a player       | ~202 ns  |
| Read a player's current position         | ~18 ns   |

Your numbers will differ; run the benchmarks to compare.

## Development

```sh
cargo test --workspace --all-features   # tests run against a built-in mock Lavalink
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo bench -p straight-rs              # benchmarks
```

To also test against a real server:

```sh
docker run -p 2333:2333 -e SERVER_PORT=2333 ghcr.io/lavalink-devs/lavalink:4
cargo test -p straight-rs --features e2e -- --ignored
```

The crate layout:

- `straight-rs`: the client (this is what you depend on).
- `straight-rs-model`: all Lavalink data types. Re-exported as `straight_rs::model`.

## License

MIT, see [LICENSE](LICENSE).
