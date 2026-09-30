# Straight-RS — Design Spec

Date: 2026-09-30

## 1. Goal and scope

`straight-rs` is a high-performance, correct, and complete **Lavalink v4 client library** written in Rust.

**Agreed decisions**
- Client library only (not a server). Targets **Lavalink v4 only** (REST + WebSocket). No v3 support.
- **Library-agnostic core**: no dependency on any Discord library. Adapters for `serenity`, `twilight`, `songbird` are optional feature flags.
- Runtime: `tokio`.
- Architecture: **actor per node + lock-free player state** (approach A).
- "Complete" = full Lavalink v4 API coverage: players, tracks (load/decode), filters, sessions/resume, node stats/info, route planner, events, plugin passthrough; plus multi-node pool and load balancing.

**Out of scope (YAGNI)**
- Queue / autoplay (users manage their own queue; `TrackEnd.may_start_next` is exposed as a helper)
- Lavalink v3 support
- The Lavalink server itself
- Adapters for libraries other than `serenity`, `twilight`, `songbird`

## 2. Workspace layout

```
straight-rs/
  Cargo.toml            (workspace)
  straight-rs-model/       types + serde for all Lavalink v4 payloads; no async runtime
  straight-rs/             client, node pool, players, events, voice adapters (feature flags)
  tests/                integration tests with a mock Lavalink server
  benches/              criterion benchmarks
```

## 3. `straight-rs-model`

- Depends only on `serde` (and `serde_json` for `Value`).
- Types: `Track`, `TrackInfo`, `PlaylistInfo`, `LoadResult` (`Track | Playlist | Search | Empty | Error`), `Player`, `PlayerState`, `VoiceState`, `Filters` (equalizer, karaoke, timescale, tremolo, vibrato, rotation, distortion, channelMix, lowPass, `pluginFilters`), `Stats`, `Info`, `RoutePlanner*`, `SessionInfo`, and events (`TrackStart`, `TrackEnd`, `TrackException`, `TrackStuck`, `WebSocketClosed`).
- Tagged enums follow the spec (`#[serde(tag = "op")]`, `#[serde(tag = "type")]`).
- Unknown/plugin fields are preserved as `serde_json::Value`.
- `Track.encoded` is `Arc<str>` so clones are cheap.
- Zero-copy parsing (`&str` / `Cow`) where practical.

## 4. `straight-rs` public API

```rust
let client = LavalinkClient::builder(user_id)
    .node(NodeConfig::new("host:2333", "password"))
    .strategy(Strategy::LeastPenalty)
    .build().await?;

client.voice_state_update(guild_id, VoiceStateUpdate { .. }).await?;
client.voice_server_update(guild_id, VoiceServerUpdate { .. }).await?;

let player = client.player(guild_id);   // cheap Arc handle
player.play(track).await?;
player.set_filters(filters).await?;
let mut events = client.events();       // broadcast::Receiver<Event>
```

- Client-level REST: `client.load(identifier)`, `client.decode(...)`.
- `Player` is a cloneable handle; `player.position()` is synchronous (interpolated from the latest snapshot).

## 5. Node, reconnect/resume, load balancing

**Node task (one per node)**
- States: `Connecting → Ready → Disconnected → Reconnecting`.
- Connects to `ws://host/v4/websocket` with headers `Authorization`, `User-Id`, `Client-Name`, and `Session-Id` when resuming.
- On `ready`: store `sessionId` and `resumed`; call `PATCH /v4/sessions/{id}` to enable `resuming=true` with a `timeout`.
- If `resumed=false` (server lost the session): rebuild players from client-held state (track, position, volume, filters, voice state, paused) and `PATCH` them back.
- Reconnect: exponential backoff with jitter (500 ms initial, 30 s max). Ping/pong watchdog detects stalled connections.

**Requests**
- REST via shared `hyper` client per node with keep-alive; WS via `tokio-tungstenite`.
- Configurable timeouts; retry only idempotent calls (GET) on transient errors.
- Per-player ordered write queue (small `mpsc` + single task) so concurrent `PATCH`es cannot overwrite each other.

**Failover**
- Node down beyond grace period → migrate its players to the best node from stored state; emit `PlayerMigrated`.
- No node available → player becomes `Orphaned`; retried when a node returns.

**Load balancing (`Strategy`)**
- `LeastPenalty` (default): players + `1.05^(100 × systemLoad) × 10 − 10` + `frameDeficit` + `frameNulled` penalties, stored as `AtomicU32` per node (lock-free selection).
- `RoundRobin`, `LeastPlayers`, `Custom(Fn)`.

**Node utilities**
- `route_planner_status()`, `free_address()`, `free_all()`, cached `info()` / `version()`.

## 6. Player state, voice adapter, events

**Player state**
- `DashMap<GuildId, Arc<PlayerInner>>`.
- Hot-read state in `ArcSwap<PlayerSnapshot>` (track, paused, volume, filters, position).
- `position()` = last reported position + `Instant::elapsed()` while not paused and connected.
- Failover state (voice state, filters, track, position) kept in a `Mutex`, locked only on write.

**Voice adapter (library-agnostic)**
- Core inputs: `VoiceStateUpdate { guild_id, channel_id, session_id }` and `VoiceServerUpdate { guild_id, token, endpoint }`.
- When both are present (and endpoint non-null) assemble `VoiceState { token, endpoint, sessionId, channelId }` and `PATCH` the node. Order of arrival must not matter.
- `channel_id = None` → destroy the player on the node and drop local state.
- `VoiceGateway` trait for join/leave (sending opcode 4 goes through the host library's gateway).
- Feature flags `serenity`, `twilight`, `songbird` contain only event-conversion code.

**Events**
- Single `Event` enum: `Ready`, `Stats`, `PlayerUpdate`, `TrackStart`, `TrackEnd { reason }`, `TrackException`, `TrackStuck`, `WebSocketClosed`, `NodeConnected`, `NodeDisconnected`, `PlayerMigrated`, `Unknown { op, payload }`.
- Client-level `broadcast`; `player.events()` filtered by guild.
- Slow receivers get `Lagged(n)`; the hot path never blocks.

**Performance**
- `PlayerUpdate` parsing borrows and updates the snapshot without extra allocation.
- criterion benches: event parsing, node selection, player position read.

## 7. Error handling

- `straight_rs::Error` (`thiserror`, `#[non_exhaustive]`): `Http`, `Ws`, `Json`, `Lavalink { status, error, message, path }`, `NoNode`, `Timeout`, `PlayerNotFound`, `Closed`.
- No panics or `unwrap` in library code. Background tasks catch errors and surface them as events or `tracing` logs.
- Reconnect and failover are internal; users observe events, not errors.

## 8. Testing

- **Unit**: serde round-trips for every model using Lavalink v4 docs samples (including unknown fields/plugins); penalty calculation, backoff, position interpolation.
- **Integration** (mock Lavalink via `axum` + WS): handshake; resume with `resumed=true` and `false`; reconnect after drop; failover; per-player `PATCH` ordering; voice state/server updates in either order; event lag.
- **Optional e2e**: real Lavalink via Docker behind `--features e2e` and `#[ignore]`.
- **Bench**: criterion (see §6).
- **CI**: `cargo fmt`, `clippy -D warnings`, tests for all feature combinations, pinned MSRV.
