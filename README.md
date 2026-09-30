# Straight-RS

A fast, easy-to-use **[Lavalink v4](https://lavalink.dev) client for Rust.**

Lavalink is a standalone audio server for Discord bots. Straight-RS connects
your Rust process to one or more Lavalink servers, loads tracks, and controls
playback. Choose whether that process is your bot or a separate playback worker;
any process failure stops its in-memory client, Gateway session, and voice
coordination. Lavalink's resume window may help with a transient reconnect, but
it does not keep a dead application process alive.

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

For the Serenity voice-event adapter:

```toml
straight-rs = { git = "https://github.com/anantix-network/Straight-RS", features = ["serenity", "tls"] }
```

The separate worker is a different crate; its `serenity` feature enables its
optional Serenity dependency and the matching `straight-rs` adapter:

```toml
straight-rs-worker = { git = "https://github.com/anantix-network/Straight-RS", features = ["serenity"] }
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
[`crates/straight-rs/examples/quickstart.rs`](crates/straight-rs/examples/quickstart.rs).

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

### Separate playback worker

The optional `straight-rs-worker` crate owns the Lavalink client, the Discord
Gateway session used for voice state/commands, and an authenticated HTTP control
API in one process. Depend on it from Git with the adapter feature you use:

```toml
straight-rs-worker = { git = "https://github.com/anantix-network/Straight-RS", features = ["serenity"] }
```

The worker owns its bot Gateway shards. **Do not also open a Gateway session for
the same bot token in the command/API application**: duplicate shard sessions
conflict. The intended topology is:

```text
Discord HTTP interactions -> Command/API app -- bearer-auth HTTP --> Playback worker
                                                                  |-- Discord Gateway shards
                                                                  |-- Lavalink client
                                                                  `-- voice coordination
```

Keep chat commands and interaction handling in another process only if that
process uses non-Gateway ingress, such as Discord HTTP interactions, then have
it call the worker's HTTP API. Worker plugins receive only the documented
sanitized playback/node events; they do not receive Discord interactions.

A minimal composition looks like this (the application supplies a
`GatewayDriver`; see the adapter status note below):

```rust,no_run
use std::net::SocketAddr;
use straight_rs::{NodeConfig, UserId};
use straight_rs_worker::{SecretString, WorkerBuilder, WorkerConfigBuilder};
use tokio::sync::watch;

async fn run(gateway: impl straight_rs_worker::GatewayDriver) -> Result<(), Box<dyn std::error::Error>> {
    let config = WorkerConfigBuilder::new(
        UserId(123456789012345678),
        SecretString::new(std::env::var("DISCORD_BOT_TOKEN")?),
        SecretString::new(std::env::var("WORKER_API_TOKEN")?), // at least 32 bytes
        vec![NodeConfig::new("127.0.0.1:2333", std::env::var("LAVALINK_PASSWORD")?)],
    )
    .bind_addr("127.0.0.1:8080".parse::<SocketAddr>()?, false)
    .build()?;

    let mut worker = WorkerBuilder::new(config, gateway).build().await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(true);
    });
    worker.serve(shutdown_rx).await?;
    Ok(())
}
```

Install the Git `serenity` feature to compile the optional Serenity dependency
and the worker's Serenity Gateway driver. The driver owns the bot Gateway
session, forwards voice-state and voice-server updates, and submits voice join
and leave commands through that session. The worker waits for Gateway readiness
before reporting ready. Build and automated adapter tests do not prove that a
deployment can authenticate or maintain a live Discord Gateway connection;
validate those credentials, intents, and network access in the target
environment.

Register plugins statically before building the worker:

```rust,no_run
let worker = WorkerBuilder::new(config, gateway)
    .optional_plugin(queue_plugin) // startup continues if this plugin fails
    // .plugin(required_plugin)     // required plugin startup failure aborts build
    .build()
    .await?;
```

Plugins are trusted in-process code, not sandboxes. They receive whitelisted
sanitized playback/node events (track `user_data` and Lavalink plugin metadata
are removed) and a restricted `WorkerContext` playback API; they cannot receive
Discord interactions through the worker plugin interface. See
[`queue-plugin`](crates/straight-rs-worker/examples/queue-plugin.rs) for the
example implementation.

The worker binds to `127.0.0.1:8080` by default and requires a non-empty API
bearer token of at least 32 bytes. Keep the API loopback-bound unless remote
access is necessary; remote binds require explicit opt-in via
`.bind_addr(address, true)` and should be protected by a private network or TLS
reverse proxy. Store bot, Lavalink, and API secrets in a secret manager or
process environment, never in source control. `/healthz` is liveness (HTTP 200
while the API responds, even if not ready); `/readyz` returns 200 only when the
Gateway and Lavalink are ready, otherwise 503. Both probes are unauthenticated;
playback/control endpoints require `Authorization: Bearer $WORKER_API_TOKEN`.

Example API calls (guild and channel IDs are decimal strings):

```sh
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl -H "Authorization: Bearer $WORKER_API_TOKEN" \\
  http://127.0.0.1:8080/v1/guilds/123456789012345678/player
curl -X POST -H "Authorization: Bearer $WORKER_API_TOKEN" \\
  -H 'Content-Type: application/json' \\
  -d '{"channel_id":"234567890123456789"}' \\
  http://127.0.0.1:8080/v1/guilds/123456789012345678/join
curl -X POST -H "Authorization: Bearer $WORKER_API_TOKEN" \\
  -H 'Content-Type: application/json' -d '{"identifier":"ytsearch:lofi"}' \\
  http://127.0.0.1:8080/v1/guilds/123456789012345678/play
```

Run the worker under a process supervisor (for example, systemd with
`Restart=on-failure`) or a container orchestrator with liveness/readiness probes
and graceful termination. A unit for your compiled application can use this
pattern (replace the executable path and service account for your deployment):

```ini
[Service]
ExecStart=/opt/my-bot/bin/playback-worker
Restart=on-failure
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=20
```

Configure container health checks to use `/healthz` for liveness and `/readyz`
for readiness; avoid restarting just because the worker is not ready during
startup. The worker, its Gateway session, its Lavalink client, and the voice
connection coordination must all remain alive in the worker process for playback
control and voice to continue. If that process exits or is killed, its Gateway
disconnects and its in-memory player/worker/plugin state is gone; a separate
command/API process cannot keep that playback alive. A new worker may reconnect
within Lavalink's configured session-resume window, but process supervision is
still required and playback continuity is not guaranteed across process death.

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

MIT OR Apache-2.0
