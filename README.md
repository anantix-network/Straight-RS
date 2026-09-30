# Straight-RS

A fast, complete [Lavalink v4](https://lavalink.dev) client for Rust. It is
library-agnostic: it talks to Lavalink over REST and WebSocket and never depends
on a particular Discord library. Optional adapters convert serenity, twilight and
songbird voice types into Straight-RS's inputs.

Workspace crates: `straight-rs` (the client) and `straight-rs-model` (all protocol
payloads, re-exported as `straight_rs::model`).

## Install

```toml
[dependencies]
straight-rs = "0.1"
# optional features: serenity | twilight | songbird | tls
# straight-rs = { version = "0.1", features = ["twilight"] }
```

`tls` enables `wss://`/`https://` nodes (rustls). The MSRV is Rust 1.80.

## Quick start

The full, compile-checked version lives in
[`straight-rs/examples/quickstart.rs`](straight-rs/examples/quickstart.rs)
(`cargo build -p straight-rs --examples`).

```rust
let client = LavalinkClient::builder(UserId(bot_id))
    .node(NodeConfig::new("127.0.0.1:2333", "youshallnotpass"))
    .strategy(Strategy::LeastPenalty)
    .build()
    .await?;
client.wait_ready(std::time::Duration::from_secs(10)).await?;

client.voice_state_update(guild_id, VoiceStateUpdate { channel_id: Some(channel), session_id }).await?;
client.voice_server_update(guild_id, VoiceServerUpdate { token, endpoint: Some(endpoint) }).await?;

if let LoadResult::Search(tracks) = client.load("ytsearch:never gonna give you up").await? {
    if let Some(track) = tracks.first() {
        client.player(guild_id).play(track).await?;
    }
}
let mut events = client.events();
```

## Wiring your Discord library

Lavalink needs the bot's own voice data. Feed the bot's voice events into the
client:

- Forward the bot's own `VOICE_STATE_UPDATE` to `client.voice_state_update(guild, VoiceStateUpdate { .. })`
  and `VOICE_SERVER_UPDATE` to `client.voice_server_update(guild, VoiceServerUpdate { .. })`.
  The two can arrive in either order; Straight-RS sends the assembled voice state to
  the node once both halves are present. A `None` channel means the bot left; a
  `None` endpoint means Discord's voice server is unavailable.
- With songbird, pass its connection info to `client.voice_update(guild, VoiceState)`.
- The `serenity`, `twilight` and `songbird` features add `straight_rs::adapters::*`
  helpers that convert those libraries' types into the inputs above.
- Implement the `VoiceGateway` trait (`join` / `leave`, which send gateway opcode 4)
  and pass it to `ClientBuilder::gateway` if you want Straight-RS to join and leave
  channels for you.

## Players

`client.player(guild)` returns a cheap, cloneable `Player` handle. Writes
(`play`, `pause`, `seek`, `set_volume`, `set_filters`, `update`, ...) for one
guild are sent one at a time, in call order. The synchronous getters
(`position`, `track`, `is_paused`, `volume`, `filters`) read the locally cached
state and never await; `snapshot()` returns the whole `PlayerSnapshot`, whose
`filters` field is an `Arc<Filters>` (shared, not copied on every update).
`player.fetch().await` asks the node for its current view of the player
(`GET /v4/sessions/{id}/players/{guild}`). `destroy()` deletes the player on its
node and forgets it locally; it completes even if the awaiting future is
dropped, and if the node is unreachable the player is deleted when the node
comes back with the same session.

## Events

`client.events()` returns a `tokio::sync::broadcast::Receiver<Event>` covering all
nodes (`NodeConnected`, `NodeDisconnected`, `Ready`, `Stats`, `PlayerUpdate`, `TrackStart`, `TrackEnd`,
`TrackException`, `TrackStuck`, `WebSocketClosed`, `PlayerMigrated`, and
`Unknown` for plugin messages). `player.events()` yields the same stream filtered
to one guild. The channel is bounded (`ClientBuilder::event_capacity`); a slow
consumer receives `RecvError::Lagged(n)` telling it how many events were dropped,
and can keep receiving afterwards.

## Resume and failover

Each node enables Lavalink session resuming with a grace period
(`NodeConfig::resume_timeout_secs`, default 60 s) and reconnects with exponential
backoff. If the session is resumed, players carry on. If the server reports
`resumed = false`, Straight-RS rebuilds every player (track, position, filters,
volume, voice state) on the new session. If a node stays down, its players are
migrated to another node according to the balancing strategy, announced by
`Event::PlayerMigrated { guild, from, to }`; players with no node available are
kept and rescued when a node returns. A write to a player whose node is down (or
that could not be migrated) moves it to a healthy node right away. Voice state
that could not be delivered (no node ready, failed request) is kept and sent
with the next write. Players moved away from a node are deleted there if that
node comes back with its old session.

## Configuration and shutdown

`ClientBuilder::build` validates the configuration and returns `Error::Config`
for settings that could never work: no nodes, `event_capacity` outside
`1..=MAX_EVENT_CAPACITY` (2^24), a zero `ping_interval`, `ping_timeout` shorter
than `ping_interval`, `secure = true` without the `tls` feature, or a host,
password or client name that is not a valid header value.

`client.shutdown()` (or dropping the last clone of the client) closes every node
connection and cancels pending failover and restore work. Nodes then report
`NodeStatus::Disconnected`, and every call on the client or on any `Player`
handle returns `Error::Closed`. Players are not destroyed on the server; Lavalink
drops them when the session's resume timeout expires.

## Out of scope

- Playing audio or speaking to Discord's voice gateway yourself (Lavalink does that).
- A Discord gateway/REST client: bring serenity, twilight, songbird, or your own.
- A track queue or playlist manager.
- Lavalink v3 and earlier.

## Performance

`cargo bench -p straight-rs` (criterion, release profile, one run on the
development machine):

| Benchmark | Time |
|---|---|
| parse `playerUpdate` | ~0.42 µs |
| parse `stats` | ~0.97 µs |
| pick over 16 nodes (`LeastPenalty`) | ~14.6 ns |
| position `interpolate` | ~1.9 ns |
| `PlayerSnapshot::position_now` | ~18 ns |
| apply a `playerUpdate` to a player (`apply_update`) | ~202 ns |

Numbers vary by machine; rerun the benches to compare on yours.

## Testing

`cargo test --workspace` runs unit and integration tests against an in-process
mock Lavalink. An opt-in suite against a real server:

```sh
docker run -p 2333:2333 -e SERVER_PORT=2333 ghcr.io/lavalink-devs/lavalink:4
cargo test -p straight-rs --features e2e -- --ignored
```

## License

MIT OR Apache-2.0
