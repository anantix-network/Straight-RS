# Straight-RS Playback Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pullable Rust worker crate that owns the Discord Gateway/Lavalink playback runtime independently of the command bot, with a secure control API and a bounded, statically registered plugin system.

**Architecture:** Move the existing packages under `crates/` without changing their package names, then add `straight-rs-worker` as a library/service package. A dedicated worker process owns the bot Gateway session and `LavalinkClient`; the application process controls it through loopback-by-default authenticated HTTP. Gateway drivers and trusted Rust plugins are adapters around the worker core; plugin callbacks run on bounded supervised tasks, outside the audio/Gateway event loops.

**Tech Stack:** Rust 2024 / MSRV 1.97; existing Tokio + Straight-RS; Axum 0.8 for HTTP; optional Serenity 0.12 and Twilight Gateway/Model 0.17 integrations; serde/serde_json, tracing, thiserror, tower utilities, and standard-library synchronization where sufficient.

**Spec:** `docs/superpowers/specs/2026-09-30-playback-worker-design.md`

## Global Constraints

- Preserve existing package names and public APIs while moving package directories under `crates/`.
- The worker runs as an independently supervised OS process and owns the Discord Gateway shard and Lavalink client; the command process must not own a duplicate session for that shard.
- The worker guarantees continuation only while its process, Discord Gateway shard, Lavalink node, and Discord voice service remain healthy.
- Control API listener defaults to `127.0.0.1`; every endpoint requires bearer authentication.
- Credentials and raw voice tokens are never logged, exposed to plugins, or persisted by the worker.
- Plugins are statically linked trusted Rust code; no dynamic library loading or plugin-owned HTTP routes in this version.
- Plugin event queues and callback durations are bounded; plugin faults must not terminate playback or other plugins.
- Do not push. Every commit uses `git commit -S` and is verified with `git verify-commit HEAD`.
- This environment has a stale `SDKROOT`; prefix Cargo/Git shell commands with `unset SDKROOT;`.

## Review Focus

1. Worker/application lifetime confusion: dropping or restarting an HTTP client must not call `leave`, `stop`, or `destroy`; test a real child command-client process exiting while the worker remains queryable.
2. Discord voice protocol event ordering and identity: ignore other users and forward both voice events even when `VOICE_SERVER_UPDATE` arrives first; test both orders and channel `None`.
3. Auth/config boundary: missing, malformed, empty, and wrong bearer tokens, non-loopback binds, oversized bodies, invalid IDs, and over-rate traffic must fail closed with stable responses.
4. Plugin overload/failure: full queue, hanging callback, startup error, and panic must produce observable unhealthy/lagged status without stalling the Lavalink/Gateway event loops.
5. Secret leakage: formatted errors, logs, debug output, API responses, and plugin events must not contain Discord/Lavalink/API credentials or raw voice tokens.

---

## File Structure

```text
Cargo.toml
crates/
  straight-rs-model/                 # existing protocol model package, moved
  straight-rs/                       # existing Lavalink client package, moved
    src/adapters/{serenity,twilight}.rs
  straight-rs-worker/
    Cargo.toml
    src/lib.rs                       # public exports
    src/config.rs                    # validated worker config + redacted secret wrappers
    src/error.rs                     # worker/API/plugin errors
    src/gateway.rs                   # GatewayDriver, gateway messages, VoiceGateway proxy
    src/runtime.rs                   # builder, startup/readiness, owned background tasks
    src/state.rs                     # worker readiness and per-guild voice channel snapshot
    src/api.rs                       # HTTP router and route handlers
    src/auth.rs                      # bearer auth and rate limit
    src/plugin.rs                    # WorkerPlugin API and bounded supervisor
    src/adapters/{mod,serenity,twilight}.rs
    examples/worker-serenity.rs      # separately run worker host example
    examples/queue-plugin.rs         # optional feature plugin example
    src/bin/worker-client-probe.rs   # test-support child process
    tests/common/{fake_gateway,mock_lavalink}.rs
    tests/{gateway,serenity_adapter,twilight_adapter,runtime,auth,api,plugins,process}.rs
.github/workflows/ci.yml
README.md

docs/superpowers/specs/2026-09-30-straight-rs-design.md  # update stale workspace paths
```

Do not modify the historical base implementation plan `docs/superpowers/plans/2026-09-30-straight-rs.md`; it documents how the original library was built. The worker spec is the authority for the new architecture.

## Execution Order and Parallel Boundaries

Run the foundation serially: Task 1 (relocation) → Task 2 (worker/Gateway public contract) → Task 3 (runtime, mock fixtures, shared module stubs). After Task 3 freezes the interfaces, launch the requested seven agents concurrently in isolated worktrees with these non-overlapping ownership boundaries:

1. Serenity adapter + Serenity adapter tests + `examples/worker-serenity.rs`.
2. Twilight adapter + Twilight adapter tests.
3. HTTP authentication/rate-limit implementation + `tests/auth.rs`.
4. HTTP route handlers/DTOs + `tests/api.rs`.
5. Plugin trait/supervisor + `tests/plugins.rs` + queue-plugin example.
6. Process regression test + test client probe binary.
7. README and CI workflow updates.

The integrator alone owns workspace/worker manifests, `lib.rs`, `runtime.rs`, `error.rs`, shared test fixtures, and final wiring/conflict resolution. The agents implement against the exact interfaces below and do not edit integrator-owned files. Their feature tests can run against the predeclared contracts/stubs; after integration, rerun every focused suite and the full workspace gates. No two agents edit the same file. If a contract mismatch appears, pause that agent's integration and resolve it centrally rather than letting seven branches redesign interfaces independently.

---

### Task 1: Move workspace packages under `crates/`

**Files:**
- Move: `straight-rs-model/` → `crates/straight-rs-model/`
- Move: `straight-rs/` → `crates/straight-rs/`
- Modify: `Cargo.toml`
- Modify: `crates/straight-rs/Cargo.toml`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-09-30-straight-rs-design.md`
- Test: existing workspace suite and package/example builds

**Interfaces:**
- Consumes: current workspace package names `straight-rs-model` and `straight-rs`.
- Produces: unchanged crate imports/package names, with manifests and sources physically under `crates/`.

- [ ] **Step 1: Record the current package graph and green baseline**

Run:

```bash
unset SDKROOT; cargo metadata --locked --no-deps --format-version 1
unset SDKROOT; cargo test --workspace --locked
```

Expected: metadata lists exactly `straight-rs-model` and `straight-rs`; existing tests pass. If the baseline is red, stop and record the pre-existing failure before moving files.

- [ ] **Step 2: Move the package trees without editing contents**

```bash
unset SDKROOT; mkdir -p crates
unset SDKROOT; git mv straight-rs-model crates/straight-rs-model
unset SDKROOT; git mv straight-rs crates/straight-rs
```

Expected: Git records two package-directory moves and no source behavior changes.

- [ ] **Step 3: Update the workspace manifest paths**

Set the root member list and local model dependency to:

```toml
[workspace]
resolver = "2"
members = ["crates/straight-rs-model", "crates/straight-rs"]

[workspace.dependencies]
straight-rs-model = { path = "crates/straight-rs-model", version = "0.1.0" }
```

Update README example links and the original design spec's workspace diagram to use `crates/straight-rs*`. Do not rename package names or Rust crate imports.

- [ ] **Step 4: Verify the relocation before proceeding**

```bash
unset SDKROOT; cargo metadata --locked --no-deps --format-version 1
unset SDKROOT; cargo test --workspace --locked
unset SDKROOT; cargo test -p straight-rs --all-features --locked
unset SDKROOT; cargo build -p straight-rs --examples --locked
```

Expected: package `manifest_path` values point under `crates/`, and all existing tests/features/examples pass.

- [ ] **Step 5: Commit the relocation**

```bash
unset SDKROOT; git add Cargo.toml Cargo.lock README.md crates docs/superpowers/specs/2026-09-30-straight-rs-design.md
unset SDKROOT; git diff --cached --check
unset SDKROOT; git commit -S -m "refactor: move Rust packages under crates"
unset SDKROOT; git verify-commit HEAD
```

---

### Task 2: Add worker config, Gateway driver boundary, and adapters

**Files:**
- Create: `crates/straight-rs-worker/Cargo.toml`
- Create: `crates/straight-rs-worker/src/{lib,config,error,gateway}.rs`
- Create: `crates/straight-rs-worker/src/adapters/{mod,serenity,twilight}.rs`
- Create: `crates/straight-rs-worker/tests/gateway.rs`
- Create: `crates/straight-rs-worker/tests/common/fake_gateway.rs`
- Create: `crates/straight-rs-worker/tests/common/mock_lavalink.rs` (implemented in Task 3)
- Modify: root `Cargo.toml` workspace members/dependencies
- Modify: `crates/straight-rs/src/config.rs` and its unit tests
- Modify: `crates/straight-rs/Cargo.toml` only if existing adapter feature forwarding is needed

**Interfaces:**
- Produces: `SecretString`, `WorkerConfig`, `WorkerBuilder`, `GatewayDriver`, `GatewayCommand`, `GatewayEvent`, and a `GatewayVoiceProxy` implementing `straight_rs::VoiceGateway`.
- `GatewayDriver::run` consumes a bot token, bounded command/event channels, and a shutdown watch receiver; implementations own the Gateway client/shard until shutdown.
- `GatewayEvent` contains only `VoiceStateUpdate`, `VoiceServerUpdate`, `Ready`, and connection-status metadata. Raw voice tokens exist only inside the worker event relay and are never sent to plugins or serialized by the control API.

- [ ] **Step 1: Scaffold the worker package and resolve its dependency graph**

Add `crates/straight-rs-worker` to root workspace members; add the workspace `straight-rs = { path = "crates/straight-rs", version = "0.1.0" }` dependency; create its manifest, `src/lib.rs`, `src/config.rs`, `src/error.rs`, and `tests/common/{fake_gateway,mock_lavalink}.rs`. Declare `serenity = { version = "0.12", default-features = false, features = ["client", "gateway", "model", "rustls_backend"], optional = true }`; declare `twilight-gateway = { version = "0.17.1", optional = true }`, `twilight-http = { version = "0.17.1", optional = true }`, and `twilight-model = { version = "0.17", optional = true }`. Define `serenity = ["dep:serenity", "straight-rs/serenity"]` and `twilight = ["dep:twilight-gateway", "dep:twilight-http", "dep:twilight-model", "straight-rs/twilight"]`. Add runtime dependencies for imported crates (`axum`, `serde`, `serde_json`, `thiserror`, `tokio`, `tracing`, `straight-rs`); add `tower`/`http-body-util` as test dependencies. Declare empty `config` and `error` modules in `lib.rs` so the targeted test reaches a missing-type compile failure rather than failing to find a module.

Run: `unset SDKROOT; cargo check -p straight-rs-worker`
Expected: PASS with the empty library and updated lockfile; this establishes a runnable test target before adding RED tests.

- [ ] **Step 2: Add failing tests for config validation and secret redaction**

In worker `src/config.rs` unit tests, assert that empty Discord token, empty API bearer token, invalid remote-bind configuration, and empty Lavalink node list return `WorkerError::Config`; assert debug formatting is redacted. In the existing Straight-RS config tests, assert that `NodeConfig`'s Debug output redacts its password; the existing derived `Debug` currently prints the Lavalink password, so replace it with a manual redacted implementation without changing the struct's public fields:

```rust
#[test]
fn secret_debug_is_redacted() {
    let secret = SecretString::new("test-token-value");
    assert_eq!(format!("{secret:?}"), "SecretString([REDACTED])");
    assert!(!format!("{secret:?}").contains("test-token-value"));
}

#[test]
fn node_config_debug_redacts_password() {
    let config = straight_rs::NodeConfig::new("localhost:2333", "lavalink-password");
    let debug = format!("{config:?}");
    assert!(!debug.contains("lavalink-password"));
    assert!(debug.contains("[REDACTED]"));
}
```

Run: `unset SDKROOT; cargo test -p straight-rs-worker --lib config::tests`
Expected: FAIL with unresolved `SecretString`/`WorkerError` symbols; the package and test target exist.

- [ ] **Step 3: Implement minimal public config and error types**

The existing project uses Serenity 0.12.5 and Twilight Model 0.17; verify adapter calls against those resolved sources. Use direct dependencies for every imported crate.

Use these public boundary types as the worker's stable contract. If a concrete Discord library requires a different internal adapter shape, keep the public command/event semantics and record the internal change in that task's handoff:

```rust
pub type WorkerResult<T> = std::result::Result<T, WorkerError>;
pub type GatewayFuture<'a> = straight_rs::BoxFuture<'a, WorkerResult<()>>;

pub enum GatewayCommand {
    SetVoiceState {
        guild: GuildId,
        channel: Option<ChannelId>,
        reply: tokio::sync::oneshot::Sender<WorkerResult<()>>,
    },
}

pub enum GatewayEvent {
    VoiceState { guild: GuildId, update: VoiceStateUpdate },
    VoiceServer { guild: GuildId, update: VoiceServerUpdate },
    Ready,
    Disconnected,
}

pub trait GatewayDriver: Send + Sync + 'static {
    fn run<'a>(
        &'a self,
        token: SecretString,
        bot_user_id: UserId,
        commands: tokio::sync::mpsc::Receiver<GatewayCommand>,
        events: tokio::sync::mpsc::Sender<GatewayEvent>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> GatewayFuture<'a>;
}
```

Implement `SecretString` with a private `Box<str>`, redacted `Debug`, and an explicit `expose_secret()` method used only by the Gateway adapter. `WorkerConfig` stores `bot_user_id`, secret token, Lavalink `NodeConfig`s, bind address, `allow_remote_bind`, API token, body limit, callback timeout, shutdown timeout, Gateway command timeout, plugin event capacity, per-IP request limit, and limiter-table capacity; validation rejects empty credentials, missing nodes, invalid ranges/capacities, and non-loopback bind unless explicitly enabled. Keep the secret accessor crate-private.

- [ ] **Step 4: Add failing fake-driver tests for voice ownership**

`tests/gateway.rs` uses the shared fake `GatewayDriver` with bounded Tokio channels. Assert `VoiceGateway::join(guild, channel)` sends one `SetVoiceState` with `Some(channel)` and waits for its oneshot acknowledgement; assert `leave(guild)` sends `None`; assert a closed driver channel returns a typed error rather than hanging. Assert a simulated `VoiceState` event is relayed to the worker event receiver. In `crates/straight-rs/src/config.rs`, replace derived `Debug` with a manual implementation that retains host/TLS/timeouts but prints `password: "[REDACTED]"`.

Run: `unset SDKROOT; cargo test -p straight-rs-worker --test gateway --locked`
Expected: FAIL because `GatewayVoiceProxy` has not yet been implemented, while the config/error/Gateway message types compile.

- [ ] **Step 5: Implement the Gateway proxy and freeze adapter contracts**

Implement `GatewayVoiceProxy` using bounded `mpsc::Sender::send` and a oneshot reply under a finite configured deadline. Add `SerenityGatewayDriver::new() -> Self` and `TwilightGatewayDriver::new() -> Self` public constructor/type contracts and empty module stubs; each implements `GatewayDriver`, owns the Gateway connection, filters using the supplied `bot_user_id`, forwards voice events to the worker, and sends opcode 4 through that same owner. Serenity implementation uses the client/gateway feature. Twilight implementation uses `twilight-http` to obtain recommended shard metadata and supervises the configured `twilight-gateway::Shard` stream. The seven-agent wave implements the two adapter source files against these frozen contracts, inspecting the locked Serenity 0.12.5 source and resolved Twilight 0.17.1 sources before method calls.

```bash
unset SDKROOT; cargo check -p straight-rs-worker --no-default-features --locked
```

- [ ] **Step 6: Run the focused tests and commit the Gateway boundary**

```bash
unset SDKROOT; cargo test -p straight-rs-worker --test gateway --locked
unset SDKROOT; cargo test -p straight-rs-worker --lib config::tests --locked
unset SDKROOT; git add Cargo.toml Cargo.lock crates/straight-rs-worker crates/straight-rs/src/config.rs
unset SDKROOT; git diff --cached --check
unset SDKROOT; git commit -S -m "feat: add playback worker gateway boundary"
unset SDKROOT; git verify-commit HEAD
```

---

### Task 3: Build the worker lifecycle and status model

**Files:**
- Create: `crates/straight-rs-worker/src/runtime.rs`
- Create: `crates/straight-rs-worker/src/state.rs`
- Modify: `crates/straight-rs-worker/src/{lib,gateway,error,api,auth,plugin}.rs` (create module stubs; integrator owns wiring)
- Modify: `crates/straight-rs/src/client.rs`
- Modify: `crates/straight-rs/tests/node.rs`
- Create: `crates/straight-rs-worker/tests/runtime.rs`

**Interfaces:**
- Produces: `ClientBuilder::build_with_events() -> Result<(LavalinkClient, broadcast::Receiver<Event>)>` (receiver subscribed before node tasks start); `WorkerBuilder::new(config, gateway)`, `WorkerBuilder::build() -> WorkerResult<RunningWorker>`, `RunningWorker::router() -> axum::Router`, `RunningWorker::serve(shutdown: watch::Receiver<bool>) -> WorkerResult<()>`, `RunningWorker::serve_on(listener: tokio::net::TcpListener, shutdown: watch::Receiver<bool>) -> WorkerResult<()>`, `RunningWorker::status() -> WorkerStatus`, and an internal per-guild voice-channel lookup for API views.
- `RunningWorker` owns `LavalinkClient`, Gateway task, event relay, and API state; no worker-owned task is detached. Task 5 adds the plugin supervisor to this owner.

- [ ] **Step 1: Write failing lifecycle tests**

Use `tests/common/fake_gateway.rs` and `tests/common/mock_lavalink.rs`; the worker crate cannot import the other package's private test fixture. Cover: `build_with_events()` captures an immediate `Ready` event emitted during node startup; Gateway voice events call `voice_state_update`/`voice_server_update`; voice-state and voice-server update order both work; the worker voice-channel snapshot records a non-null channel and removes it on channel `None`; readiness remains false until both Gateway and a Lavalink node are ready; shutdown signals Gateway and joins owned tasks within a finite deadline.

Run: `unset SDKROOT; cargo test -p straight-rs-worker --test runtime --locked`
Expected: FAIL until `RunningWorker` owns and coordinates these components.

- [ ] **Step 2: Implement startup and ownership**

Add `ClientBuilder::build_with_events()` without changing `build()` behavior: construct the broadcast channel, subscribe a receiver before spawning any node tasks, then return `(LavalinkClient, receiver)`. Implement `build()` by calling the shared internal builder and discarding the pre-subscribed receiver. `WorkerBuilder::build()` uses this method, creates the `GatewayVoiceProxy`, starts `LavalinkClient::builder(bot_user_id).node(...).gateway(proxy).build_with_events().await?`, then starts the Gateway driver and relay tasks. Keep Gateway and Lavalink readiness separate in `WorkerStatus`; do not report ready merely because HTTP is listening.

For each `GatewayEvent::VoiceState` or `VoiceServer`, forward the exact guild/update to the corresponding `LavalinkClient` method. Maintain `VoiceStateStore` from own-bot voice-state updates (`Some(channel)` inserts/replaces, `None` removes) so the HTTP player view can show the current channel without reading secret voice data. If a client event receiver reports `Lagged(n)`, increment a worker diagnostic counter and continue receiving; if closed, report degraded status and stop/reconcile rather than spin.

- [ ] **Step 3: Implement bounded shutdown**

Implement `serve()` by binding `WorkerConfig::bind_addr` and delegating to `serve_on(listener, shutdown)`. `serve_on()` passes `ConnectInfo<SocketAddr>` into Axum so the bounded rate limiter can key by peer IP. On the shutdown watch value, stop accepting API traffic, signal Gateway and event-relay tasks, wait for owned tasks until the configured deadline, then abort and join remaining tasks. Do not call `Player::leave`, `Player::destroy`, or send Discord voice-state `None` as implicit cleanup. Return task failures through `WorkerError`/status and log only redacted diagnostics. Task 5 extends this same shutdown path to plugin tasks.

- [ ] **Step 4: Verify lifecycle and commit**

```bash
unset SDKROOT; cargo test -p straight-rs-worker --test runtime --locked
unset SDKROOT; cargo test -p straight-rs-worker --test gateway --locked
unset SDKROOT; cargo test -p straight-rs --test node --locked
unset SDKROOT; git add crates/straight-rs-worker crates/straight-rs/src/client.rs crates/straight-rs/tests/node.rs
unset SDKROOT; git diff --cached --check
unset SDKROOT; git commit -S -m "feat: supervise playback worker lifecycle"
unset SDKROOT; git verify-commit HEAD
```

---

### Task 4: Add authenticated playback HTTP API

**Files:**
- Create/modify: `crates/straight-rs-worker/src/auth.rs` (auth agent)
- Create/modify: `crates/straight-rs-worker/src/api.rs` (route agent)
- Create: `crates/straight-rs-worker/tests/auth.rs` and `tests/api.rs`
- Integration-only: `Cargo.toml`, `crates/straight-rs-worker/Cargo.toml`, `src/lib.rs`, `src/runtime.rs`, `src/error.rs` (integrator owns these; API agents do not edit them)

**Interfaces:**
- `GET /healthz`, `GET /readyz`, `GET /v1/guilds/{guild_id}/player`.
- `POST /v1/guilds/{guild_id}/join` with `{ "channel_id": "..." }`.
- `POST /v1/guilds/{guild_id}/play` with `{ "identifier": "..." }`.
- `POST /v1/guilds/{guild_id}/pause` with `{ "paused": true }`; `/resume`; `/seek` with `{ "position_ms": 0 }`; `/volume` with `{ "volume": 100 }`; `/stop`; `/leave`.
- Every route requires an `Authorization` header using the Bearer scheme. Responses use `{ "error": { "code": "...", "message": "..." } }` for failures and never include raw provider/token values.
- Auth module contract: `AuthState::new(api_token: SecretString, limits: RateLimitConfig) -> AuthState` and `auth::secure(router: axum::Router, state: AuthState) -> axum::Router`; the API router owns route state, then wraps the complete router with `auth::secure`.
- API module contract: `api::router(state: Arc<WorkerApiState>, auth: AuthState) -> axum::Router`; only the integrator adds the module export and wires it to `RunningWorker::router()`.

- [ ] **Step 1: Write failing API contract tests**

Use `tower::ServiceExt::oneshot` against `RunningWorker::router()` backed by `tests/common/fake_gateway.rs` and `tests/common/mock_lavalink.rs`. Pin these cases: no/malformed/wrong bearer → `401`; valid bearer on `/healthz` → `200`; unready `/readyz` → `503`; invalid snowflake/body/range → `400`; unknown route → `404`; `play` returns structured `Empty`/`Error` outcomes without panicking; `/leave` is the only API route that sends a voice channel `None`; request-body size above the configured maximum returns `413`.

Example auth assertion:

```rust
let response = app.clone().oneshot(
    Request::get("/healthz").body(Body::empty()).unwrap(),
).await.unwrap();
assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
```

Run: `unset SDKROOT; cargo test -p straight-rs-worker --test api --locked`
Expected: FAIL until routes and auth middleware exist.

- [ ] **Step 2: Implement auth, limits, and typed request/response DTOs**

Add Axum 0.8, tower test utilities, and `subtle` as workspace dependencies. Apply an authentication middleware to the complete router before route dispatch; compare bearer bytes with `subtle::ConstantTimeEq` and never log them. Apply `DefaultBodyLimit::max(config.max_body_bytes)`, per-client-IP request rate limiting with a bounded in-memory table (reject unseen peers with `429` when the table is at capacity), and finite handler deadlines (`504` on deadline). Reject API tokens shorter than 32 bytes at config validation. Default `bind_addr` to `127.0.0.1:8080`; refuse non-loopback binds unless explicitly enabled, and still require bearer auth. In router tests, add `ConnectInfo<SocketAddr>` explicitly; production `serve_on()` uses `into_make_service_with_connect_info::<SocketAddr>()`.

Define DTOs with `#[serde(deny_unknown_fields)]` for mutating routes. Validate `volume <= 1000` and seek values before calling Straight-RS. Serialize a public `PlayerView` from `Player::snapshot()` fields and `VoiceStateStore`, computing current position with `position_now()`; never serialize `Instant`, `Track::user_data`, or voice credentials.

- [ ] **Step 3: Implement handlers against current Straight-RS methods**

Use `LavalinkClient::load(identifier)` and handle `LoadResult::{Track,Search,Playlist,Empty,Error}` deterministically; choose the first result for Search, the playlist's selected track when valid (otherwise first track), and return `404` for Empty/no playable tracks. Map provider/load errors to `502`, unavailable Lavalink nodes to `503`, invalid request values to `400`, and request deadline to `504`. Use `Player::play`, `pause`, `seek`, `set_volume`, `stop`, `join`, and `leave`. Do not install request-disconnect cleanup that calls `leave` or `destroy`; a command-client disconnect by itself must not mutate playback state.

- [ ] **Step 4: Test rate-limit and error/secret paths**

Use a test configuration with a small limit and assert the next request returns `429`, then assert the next window allows requests without fixed sleeps (enable Tokio's `test-util` feature only for worker tests and use `tokio::time::pause/advance`). Bound the per-IP limiter table size and assert a new IP cannot cause unbounded map growth. Assert `format!("{err:?}")`, response bodies, and captured tracing output do not contain test bot, API, Lavalink, or voice tokens.

- [ ] **Step 5: Run focused tests and commit**

```bash
unset SDKROOT; cargo test -p straight-rs-worker --test api --locked
unset SDKROOT; cargo test -p straight-rs-worker --test runtime --locked
unset SDKROOT; git add Cargo.toml Cargo.lock crates/straight-rs-worker
unset SDKROOT; git diff --cached --check
unset SDKROOT; git commit -S -m "feat: add authenticated playback control API"
unset SDKROOT; git verify-commit HEAD
```

---

### Task 5: Implement bounded plugin lifecycle and event hooks

**Files:**
- Create/modify: `crates/straight-rs-worker/src/plugin.rs`
- Create: `crates/straight-rs-worker/tests/plugins.rs`
- Create: `crates/straight-rs-worker/examples/queue-plugin.rs`
- Integration-only: `src/lib.rs`, `src/runtime.rs`, `src/api.rs`, `src/error.rs` (integrator owns plugin wiring and readiness integration)

**Interfaces:**

```rust
pub type PluginResult = std::result::Result<(), PluginError>;
pub type PluginFuture<'a> = straight_rs::BoxFuture<'a, PluginResult>;

pub trait WorkerPlugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn on_start(&self, context: WorkerContext) -> PluginFuture<'_>;
    fn on_event(&self, context: WorkerContext, event: WorkerEvent) -> PluginFuture<'_>;
    fn on_shutdown(&self, context: WorkerContext) -> PluginFuture<'_>;
}
```

`PluginTrack` is a sanitized projection with `encoded: Arc<str>`, `title`, `author`, `length_ms`, `is_stream`, and `source_name`; it omits `Track::user_data`, plugin metadata, and voice credentials. `WorkerEvent` is a sanitized enum with `TrackStart { guild, track: PluginTrack }`, `TrackEnd { guild, track: PluginTrack, may_start_next }`, `TrackException { guild, track: PluginTrack }`, `TrackStuck { guild, track: PluginTrack, threshold_ms }`, `NodeConnected { node }`, `NodeDisconnected { node }`, `PlayerMigrated { guild, from, to }`, `Ready`, and `Degraded`. It never wraps raw Lavalink payloads or carries `GatewayEvent::VoiceServer` or credentials. `WorkerContext` has `player(guild) -> Player`, async `load(identifier) -> WorkerResult<LoadResult>`, and `status() -> WorkerStatus`; it has no accessor for `LavalinkClient` internals or secret fields. `WorkerBuilder::plugin(plugin)` registers a required plugin; `optional_plugin(plugin)` registers an optional one.

Implement those contracts with these exact event/context shapes:

```rust
#[derive(Clone, Debug)]
pub struct PluginTrack {
    pub encoded: Arc<str>,
    pub title: Arc<str>,
    pub author: Arc<str>,
    pub length_ms: u64,
    pub is_stream: bool,
    pub source_name: Arc<str>,
}

#[derive(Clone, Debug)]
pub enum WorkerEvent {
    TrackStart { guild: GuildId, track: PluginTrack },
    TrackEnd { guild: GuildId, track: PluginTrack, may_start_next: bool },
    TrackException { guild: GuildId, track: PluginTrack },
    TrackStuck { guild: GuildId, track: PluginTrack, threshold_ms: u64 },
    NodeConnected { node: usize },
    NodeDisconnected { node: usize },
    PlayerMigrated { guild: GuildId, from: usize, to: usize },
    Ready,
    Degraded,
}

#[derive(Clone)]
pub struct WorkerContext {
    client: LavalinkClient,
    status: tokio::sync::watch::Receiver<WorkerStatus>,
}

impl WorkerContext {
    pub fn player(&self, guild: GuildId) -> Player {
        self.client.player(guild)
    }

    pub async fn load(&self, identifier: &str) -> WorkerResult<LoadResult> {
        self.client.load(identifier).await.map_err(WorkerError::from)
    }

    pub fn status(&self) -> WorkerStatus {
        self.status.borrow().clone()
    }
}
```

- [ ] **Step 1: Write failing plugin lifecycle tests**

Create recording plugins and assert: registration order determines startup order; event callbacks receive cloned events; shutdown order is reverse registration; required startup failure fails worker build; optional startup failure leaves the worker running and marks that plugin unhealthy; one panicking plugin is marked unhealthy without stopping the worker or a second plugin; a blocked callback hits its deadline; filling one plugin's bounded channel reports lag for that plugin only; the health endpoint includes plugin status but never plugin secrets.

Run: `unset SDKROOT; cargo test -p straight-rs-worker --test plugins --locked`
Expected: FAIL until plugin registry/supervisor is wired into runtime.

- [ ] **Step 2: Add static registry and callback API**

Store plugins as `Arc<dyn WorkerPlugin>` with an explicit required flag, bounded `mpsc` capacity, and callback timeout. Run each plugin in its own supervised task. Run required `on_start` hooks sequentially before reporting ready; mark optional failures unhealthy and continue. A required startup-hook error or timeout fails build only after signaling and joining all Gateway/Lavalink/event tasks already started. Dispatch public `WorkerEvent`s with `try_send`; on full queue increment plugin lag count and surface `PluginStatus::Lagged` rather than blocking Gateway/Lavalink event tasks. Convert task panic/join failure and callback timeout into `PluginStatus::Unhealthy`; do not retry a panicked callback automatically.

On shutdown, stop dispatch, call initialized plugin `on_shutdown` in reverse order with the finite shutdown deadline, then cancel/join remaining tasks. Add a regression assertion that a required startup failure leaves no running Gateway/plugin task. A plugin callback cannot change API auth, bind a listener, or access raw Discord/Lavalink credentials.

- [ ] **Step 3: Add an example queue plugin**

Implement `QueuePlugin` in `examples/queue-plugin.rs`: consume only `WorkerEvent::TrackEnd { may_start_next: true, .. }`, keep a per-guild in-memory track queue, and use `WorkerContext` to play the next queued track. Document that the example queue is ephemeral and is not bundled into the worker core; do not persist voice/session credentials.

- [ ] **Step 4: Run plugin-focused tests and commit**

```bash
unset SDKROOT; cargo test -p straight-rs-worker --test plugins --locked
unset SDKROOT; cargo test -p straight-rs-worker --test api --locked
unset SDKROOT; cargo clippy -p straight-rs-worker --all-targets --all-features -- -D warnings
unset SDKROOT; git add crates/straight-rs-worker
unset SDKROOT; git diff --cached --check
unset SDKROOT; git commit -S -m "feat: add bounded playback plugin system"
unset SDKROOT; git verify-commit HEAD
```

---

### Task 6: Prove command-process independence and document deployment

**Files:**
- Process-test agent creates: `crates/straight-rs-worker/tests/process.rs` and `src/bin/worker-client-probe.rs` (test-only target, gated with `required-features = ["test-support"]`).
- Serenity-adapter agent creates: `crates/straight-rs-worker/examples/worker-serenity.rs`.
- Docs/CI agent modifies: `README.md` and `.github/workflows/ci.yml`.
- Integration-only: `crates/straight-rs-worker/Cargo.toml` (integrator declares binary/feature targets).

**Interfaces:**
- Produces a runnable example process that owns one Serenity Gateway connection and the Lavalink client, plus a child command-client probe used only by the process test.
- README install example adds `straight-rs-worker` as a Git dependency with `serenity` feature; deployment instructions start the worker separately and point the app to its bearer-authenticated API.

- [ ] **Step 1: Write the process-isolation regression test**

Bind a mock-backed `RunningWorker` with `serve_on()` to a `127.0.0.1:0` listener in the test process, configure the fake Gateway to emit both valid voice updates on join, and spawn `worker-client-probe` as a child process with `WORKER_API_BASE_URL`, `WORKER_API_TOKEN`, `GUILD_ID`, `CHANNEL_ID`, and `TRACK_IDENTIFIER`. The child issues `POST /join`, then `POST /play`, and exits successfully; the parent asserts the mock Lavalink player is connected with the active track and the channel snapshot remains present. Spawn a second child to query the same player and verify control resumes. The test must not call `leave`, `stop`, or `destroy` during child exit.

Run: `unset SDKROOT; cargo test -p straight-rs-worker --test process --features test-support --locked`
Expected: FAIL until the probe target and end-to-end API flow exist.

- [ ] **Step 2: Add the Serenity worker host example**

Read credentials only from environment variables (`DISCORD_TOKEN`, `DISCORD_BOT_USER_ID`, `LAVALINK_HOST`, `LAVALINK_PASSWORD`, `WORKER_API_TOKEN`, optional `WORKER_BIND`). Parse numeric IDs with checked `u64` parsing; do not print environment values. Build the Serenity `GatewayDriver`, register any example plugins explicitly, then run `RunningWorker::serve` with Ctrl-C/shutdown handling. The example binary is a separate process from any command bot.

- [ ] **Step 3: Document topology, limits, and usage**

Update README with: installation from Git, minimal `WorkerBuilder` setup, separate process diagram, client HTTP calls, plugin registration example, health/readiness meanings, local bind/auth configuration, systemd/container supervision guidance, and the hard limitation that worker/Gateway-shard termination interrupts playback. Explain that because the worker owns the bot's Gateway shards, the command application must not open duplicate shard sessions; it must call the worker API and receive Discord commands through a non-Gateway ingress such as Discord HTTP interactions. State that worker plugins receive sanitized playback/node lifecycle events, not Discord interactions, in this version. Remove claims that the old in-process client alone preserves playback when the bot process dies. Update CI to test each adapter feature, worker examples, full workspace tests, fmt, and Clippy.

- [ ] **Step 4: Run all acceptance gates**

```bash
unset SDKROOT; cargo metadata --locked --no-deps --format-version 1
unset SDKROOT; cargo fmt --all -- --check
unset SDKROOT; cargo test --workspace --locked
unset SDKROOT; cargo test --workspace --all-features --locked
unset SDKROOT; cargo clippy --workspace --all-targets --all-features -- -D warnings
unset SDKROOT; cargo build -p straight-rs-worker --examples --all-features --locked
unset SDKROOT; cargo test -p straight-rs-worker --test process --features test-support --locked
```

Expected: all commands pass; metadata lists all three packages with manifests under `crates/`; process regression proves only the command client can exit while current playback stays active.

- [ ] **Step 5: Final review and commit**

```bash
unset SDKROOT; git status --short --branch
unset SDKROOT; git diff --check
unset SDKROOT; git add README.md .github/workflows/ci.yml crates/straight-rs-worker
unset SDKROOT; git diff --cached --stat
unset SDKROOT; git commit -S -m "docs: document standalone playback worker"
unset SDKROOT; git verify-commit HEAD
```

Do not push or deploy. Finish with a live manual smoke test only when a test Discord bot and test Lavalink endpoint are available; otherwise state that limitation and report automated evidence separately.
