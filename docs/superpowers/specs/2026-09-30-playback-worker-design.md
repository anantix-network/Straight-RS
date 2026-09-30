# Straight-RS Playback Worker — Design Spec

Date: 2026-09-30

## 1. Goal

Provide a reusable Rust crate/service that keeps the Discord Gateway shard and Lavalink player runtime alive when the command/application process is unavailable. Consumers can add the worker crate to a dedicated, long-running process and send playback commands to it from their bot application.

Success means: while the worker process, its Discord Gateway connection, Lavalink node, and Discord voice service remain healthy, stopping/restarting the separate command process does not disconnect an already-playing track. The command process can reconnect and inspect/control the existing player afterward.

This is not a promise that audio survives the worker process, its Discord shard, Lavalink, or Discord voice connection going down. Lavalink documents that when the shard's main WebSocket dies, its Lavalink audio connections die too, including session resumes. Lavalink session resuming protects a Lavalink-client WebSocket interruption only while the Discord shard remains alive. See the API implementation notes: <https://github.com/lavalink-devs/Lavalink/blob/master/docs/api/index.md>.

## 2. Current project context

- The workspace currently contains `straight-rs-model/` and `straight-rs/`; the client crate owns Lavalink node sessions, player state, reconnect/resume, and voice input APIs.
- `straight-rs::VoiceGateway` can send join/leave through a host Discord library, and existing adapters convert Serenity/Twilight voice events. They do not own a long-lived Discord Gateway connection.
- A library linked into the command bot's process cannot outlive that process. The new worker therefore has to run as a distinct OS process/service and own the bot's Gateway shard as well as the Lavalink client.
- The user requested the Rust package code be kept under `crates/`; move the existing package directories there as part of this workspace change.

## 3. Proposed workspace shape

```text
Cargo.toml
crates/
  straight-rs-model/       # existing protocol models
  straight-rs/             # existing Lavalink client
  straight-rs-worker/      # new reusable worker runtime and control API
```

Keep existing package names and public APIs stable. Update workspace member paths, local path dependencies, CI commands if path-sensitive, examples/docs links, and lockfile. The worker crate depends on `straight-rs`; the client remains usable independently with no worker dependency.

Consumers initially add `straight-rs-worker` as a Git dependency, consistent with the current README distribution model. Do not claim crates.io availability until a separate publishing decision is made.

## 4. Process and ownership model

1. The worker is started and supervised independently (systemd, container, or another process supervisor); it is not spawned as a child that dies with the command bot.
2. The worker owns the bot token, exactly one configured owner for each Discord shard, its long-lived Discord Gateway session, voice event handling, and the `LavalinkClient`.
3. The command/application process does not open a duplicate Gateway session for the same shard. It uses the worker control API for player operations.
4. The worker forwards its own bot's `VOICE_STATE_UPDATE` and `VOICE_SERVER_UPDATE` to Straight-RS, and routes join/leave operations through the owned Gateway connection.
5. Lavalink remains the audio engine. While its player is actively playing, the worker's continued Gateway and Lavalink sessions keep the voice path alive independently of the command process.

The worker crate exposes a small Gateway-driver boundary so Gateway mechanics are isolated and testable. Provide optional supported integrations for Serenity and Twilight, matching the existing project ecosystem; do not add Songbird as an audio owner for this Lavalink path. Gateway adapters must consume Gateway events and send voice-state commands through the same shard owner.

## 5. Control API

Expose a versioned HTTP/JSON API over a configurable bind address. Bind to loopback by default and require a bearer token for every request. Document reverse-proxy/TLS deployment for remote access; never provide an unauthenticated public listener.

Initial endpoints:

- `GET /healthz`: process/API health, not a claim that Discord or Lavalink is ready.
- `GET /readyz`: readiness of the Gateway owner and at least one ready Lavalink node.
- `GET /v1/guilds/{guild_id}/player`: current snapshot, voice channel, node/readiness state.
- `POST /v1/guilds/{guild_id}/join`: join a specified voice channel.
- `POST /v1/guilds/{guild_id}/play`: load/search and play the provided query or encoded track.
- `POST /v1/guilds/{guild_id}/pause`, `/resume`, `/seek`, `/volume`, `/stop`, `/leave`: explicit playback controls.

Use bounded request bodies, request deadlines, structured error responses, and graceful shutdown. Validate IDs and payloads before calling the client. No endpoint may destroy a player on command-bot disconnect or API-client timeout; only an explicit `leave`/`stop` request changes playback. Commands that can be retried should have clear idempotent semantics.

## 6. State and outage behavior

- The worker owns active player state and remains the sole command writer for its guilds. The command bot may disconnect/restart without sending `leave` or `destroy`.
- The currently loaded track continues in Lavalink when only the command process is down; when the command process returns it can query the player snapshot and resume control.
- Track queues/autoplay remain out of scope for the first version. A track already submitted to Lavalink continues, but the worker does not promise a next track after `TrackEnd` unless a later queue feature is designed.
- Lavalink-node loss is handled by the existing Straight-RS reconnect/session restore and node failover behavior, subject to Discord voice credentials remaining valid and the Gateway shard remaining connected.
- Worker or Gateway-owner termination is outside the uninterrupted-playback guarantee. On worker shutdown, do not silently send a leave/destroy as cleanup; surface the limitation clearly and allow configured graceful shutdown behavior to be explicit.
- Durable recovery across worker host/process loss is not part of this version. Do not persist Discord voice tokens or session credentials to disk.

## 7. Security and operations

- Read Discord/Lavalink credentials from environment or the user's secret manager; never log tokens, Authorization headers, or full voice credentials.
- Default control listener is `127.0.0.1`; require bearer authentication for all routes. Reject invalid/oversized input and rate-limit control requests.
- Provide graceful shutdown signals, health/readiness, structured tracing, and operator guidance for process supervision and single-shard ownership.
- Document expected topology, known single points of failure, and that horizontal replicas must not own the same Discord shard concurrently.

## 8. Testing and acceptance criteria

1. Workspace relocation preserves package names and all existing tests/examples; `cargo metadata --no-deps` resolves every member under `crates/`.
2. Unit tests prove API validation, authentication, bounded request handling, and error mapping without live Discord/Lavalink credentials.
3. Mocked integration tests prove Gateway voice updates reach Straight-RS, join/leave use the worker-owned shard adapter, control calls mutate only the requested guild player, and API-client disconnect does not stop playback or destroy player state.
4. A process-level test runs the worker separately from a mock command client, starts playback, terminates/restarts only the command client, and verifies the worker/player remains active and queryable.
5. Tests explicitly distinguish command-process failure (playback remains active) from worker/Gateway-shard failure (uninterrupted playback is not promised).
6. Verify with workspace tests, formatting, Clippy, and example build; add a documented manual smoke test using a test Discord bot and Lavalink instance.

## 9. Non-goals

- Keeping audio alive after the worker's own Discord Gateway shard or process dies.
- Replacing Lavalink or implementing Discord voice audio transport in Straight-RS.
- Queue/autoplay persistence, database-backed state, multi-tenant authorization, or public Internet exposure without TLS/reverse proxy.
- Running multiple active worker owners for the same Discord shard.
