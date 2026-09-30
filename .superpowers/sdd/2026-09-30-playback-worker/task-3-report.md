# Task 3 report — partial; lifecycle implementation blocked

## Outcome

Implemented the client-side pre-subscribed event API required by Task 3: `ClientBuilder::build_with_events()` creates a broadcast receiver before spawning node tasks and returns it with the client; existing `build()` delegates to this method and discards the receiver. Added an integration regression test proving an immediate Lavalink `Ready` event is captured.

Task 3 is **not complete**. Worker lifecycle/status/readiness, Gateway event forwarding, voice-channel snapshot, API router, bounded shutdown, and runtime integration tests were not implemented. No implicit leave/destroy behavior was introduced.

## TDD record

- RED: the new node integration test first failed to compile because `build_with_events` did not exist (after correcting the fixture's address accessor from an initially mistaken `address()` to the actual `host()`).
- GREEN: after implementation, the test initially reached the expected stream but asserted the wrong event variant (`NodeConnected`). The event model distinguishes `Ready`; corrected the test to assert `Event::Ready`, then the focused test passed.
- The broader prescribed runtime RED command could not run because the `runtime` test target and runtime module do not exist in this checkout.

## Verification

- `unset SDKROOT; cargo test -p straight-rs --test node build_with_events_captures_immediate_ready_event --locked` — passed, 1 test.
- `unset SDKROOT; cargo test -p straight-rs-worker --test runtime --locked` — blocked: no test target named `runtime` (only `gateway` is present).
- `unset SDKROOT; cargo test -p straight-rs-worker --test gateway --locked` — passed, 7 tests.
- `unset SDKROOT; cargo test -p straight-rs --test node --locked` — passed, 15 tests.
- `unset SDKROOT; cargo fmt --all -- --check` — passed.
- `unset SDKROOT; cargo clippy -p straight-rs --test node --locked -- -D warnings` — passed.
- `unset SDKROOT; git diff --check` — passed.

This environment's configured Xcode SDK path was initially invalid; commands succeeded after setting the command-local `DEVELOPER_DIR=/Library/Developer/CommandLineTools` while retaining the required `unset SDKROOT;` prefix.

## Blockers requiring task-owner resolution

1. The existing public `WorkerBuilder` is defined in `config.rs`; `WorkerBuilder::new(bot_user_id, bot_token, api_token, nodes)` and `build() -> WorkerResult<WorkerConfig>` are already part of the Task 2 API. Task 3 simultaneously requires `WorkerBuilder::new(config, gateway)` and `build() -> WorkerResult<RunningWorker>`. Rust cannot overload associated constructors or methods by arity/return type. Changing this would break the existing public API, contrary to the global constraint to preserve public APIs. The task owner must choose a compatible naming/transition strategy (for example, add a distinct runtime builder name or an explicit migration exception).
2. The required shared Lavalink fixture at `crates/straight-rs-worker/tests/common/mock_lavalink.rs` contains only the placeholder comment `// Implemented by the Lavalink runtime task.`. A runtime integration test target has not been created. The runtime work needs this fixture implemented or a decision to share/extract the existing private mock from `straight-rs/tests/common/mod.rs`.

## Files changed

- `crates/straight-rs/src/client.rs`
- `crates/straight-rs/tests/node.rs`

No push was performed. The implemented pre-subscribed client event API and test were committed as `bc7cd04` (`feat: add pre-subscribed Lavalink events`); `git verify-commit HEAD` reported a good signature for key `4BF1BEEF20534E262484E1E196591EC3A22386F5`. This was the partial Task 3 commit.

## Task 3 continuation — lifecycle implementation

Controller ruling resolved the API transition: the config-only API is now `WorkerConfigBuilder`, while `WorkerBuilder::new(config, gateway)` returns a running worker. Added runtime ownership of the Lavalink client, Gateway driver and event relay; readiness reports Gateway and Lavalink independently and only sets `ready` when both are ready. Gateway voice-state/server updates are relayed to Lavalink, and voice-state channel IDs are held in an in-memory per-guild snapshot. Event broadcast lag increments a diagnostic counter and continues; closed event sources degrade/stop the relay. `serve` binds the configured address and `serve_on` injects `ConnectInfo<SocketAddr>`; router is deliberately empty pending Task 4 authenticated routes. Shutdown signals Gateway, closes Lavalink, waits to a finite deadline, then aborts and joins timed-out owned tasks; no implicit leave/destroy is sent.

RED→GREEN: the new runtime integration test initially failed compilation due to missing WorkerConfigBuilder/WorkerBuilder and mock; implementation made the API compile and the lifecycle readiness test pass. A first shutdown implementation exposed a `JoinHandle polled after completion` failure; corrected by retaining the join result from the deadline wait. The mock Lavalink uses a deterministic in-process websocket Ready handshake.

Verified:
- `unset SDKROOT; DEVELOPER_DIR=/Library/Developer/CommandLineTools cargo test -p straight-rs-worker --locked` — passed: 9 unit, 7 gateway, 1 runtime tests.
- `unset SDKROOT; DEVELOPER_DIR=/Library/Developer/CommandLineTools cargo test -p straight-rs --test node --locked` — passed: 15 tests.
- `unset SDKROOT; DEVELOPER_DIR=/Library/Developer/CommandLineTools cargo fmt --all -- --check` — passed.
- `unset SDKROOT; DEVELOPER_DIR=/Library/Developer/CommandLineTools cargo clippy -p straight-rs-worker --all-targets --all-features --locked -- -D warnings` — passed.
- `unset SDKROOT; git diff --check` — passed.

Final signed lifecycle commit follows partial `bc7cd04`; commit hash and signature verification are recorded after commit. Remaining scope limit: Task 3 tests provide readiness/shutdown smoke coverage, but do not integration-assert voice update ordering/forwarding or voice snapshot removal; router intentionally has no routes, and plugin supervision remains Task 5. No push.
