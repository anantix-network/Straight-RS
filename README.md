# larplink

A fast, complete [Lavalink v4](https://lavalink.dev) client for Rust. It is
library-agnostic: it talks to Lavalink over REST and WebSocket and never depends
on a particular Discord library. Optional adapters convert serenity, twilight and
songbird voice types into larplink's inputs.

Workspace crates: `larplink` (the client) and `larplink-model` (all protocol
payloads, re-exported as `larplink::model`).

## Install

```toml
[dependencies]
larplink = "0.1"
# optional features: serenity | twilight | songbird | tls
# larplink = { version = "0.1", features = ["twilight"] }
```

`tls` enables `wss://`/`https://` nodes (rustls). The MSRV is Rust 1.80.

## Quick start

The full, compile-checked version lives in
[`larplink/examples/quickstart.rs`](larplink/examples/quickstart.rs)
(`cargo build -p larplink --examples`).

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
  The two can arrive in either order; larplink sends the assembled voice state to
  the node once both halves are present. A `None` channel means the bot left; a
  `None` endpoint means Discord's voice server is unavailable.
- With songbird, pass its connection info to `client.voice_update(guild, VoiceState)`.
- The `serenity`, `twilight` and `songbird` features add `larplink::adapters::*`
  helpers that convert those libraries' types into the inputs above.
- Implement the `VoiceGateway` trait (`join` / `leave`, which send gateway opcode 4)
  and pass it to `ClientBuilder::gateway` if you want larplink to join and leave
  channels for you.

## Events

`client.events()` returns a `tokio::sync::broadcast::Receiver<Event>` covering all
nodes (`NodeConnected`, `Ready`, `Stats`, `PlayerUpdate`, `TrackStart`, `TrackEnd`,
`TrackException`, `TrackStuck`, `WebSocketClosed`, `PlayerMigrated`, and
`Unknown` for plugin messages). `player.events()` yields the same stream filtered
to one guild. The channel is bounded (`ClientBuilder::event_capacity`); a slow
consumer receives `RecvError::Lagged(n)` telling it how many events were dropped,
and can keep receiving afterwards.

## Resume and failover

Each node enables Lavalink session resuming with a grace period
(`NodeConfig::resume_timeout_secs`, default 60 s) and reconnects with exponential
backoff. If the session is resumed, players carry on. If the server reports
`resumed = false`, larplink rebuilds every player (track, position, filters,
volume, voice state) on the new session. If a node stays down, its players are
migrated to another node according to the balancing strategy, announced by
`Event::PlayerMigrated { guild, from, to }`; players with no node available are
kept and rescued when a node returns.

## Out of scope

- Playing audio or speaking to Discord's voice gateway yourself (Lavalink does that).
- A Discord gateway/REST client: bring serenity, twilight, songbird, or your own.
- A track queue or playlist manager.
- Lavalink v3 and earlier.

## Performance

`cargo bench -p larplink` (criterion, release profile, one run on the
development machine):

| Benchmark | Time |
|---|---|
| parse `playerUpdate` | ~0.42 µs |
| parse `stats` | ~0.97 µs |
| pick over 16 nodes (`LeastPenalty`) | ~14.6 ns |
| position `interpolate` | ~1.9 ns |
| `PlayerSnapshot::position_now` | ~18 ns |

Numbers vary by machine; rerun the benches to compare on yours.

## Testing

`cargo test --workspace` runs unit and integration tests against an in-process
mock Lavalink. An opt-in suite against a real server:

```sh
docker run -p 2333:2333 -e SERVER_PORT=2333 ghcr.io/lavalink-devs/lavalink:4
cargo test -p larplink --features e2e -- --ignored
```

## License

MIT OR Apache-2.0
