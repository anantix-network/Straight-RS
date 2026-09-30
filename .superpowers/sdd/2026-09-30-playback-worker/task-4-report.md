# Task 4 report — authenticated playback HTTP API

## Implemented

- Added bearer authentication with `subtle::ConstantTimeEq`, sanitized JSON error envelopes, bounded per-IP limiter keyed by `ConnectInfo<SocketAddr>`, fixed-capacity rejection, and paused-time window reset behavior.
- Added API routes for health/readiness, player view, join/play/pause/resume/seek/volume/stop/leave; hand-authored DTOs omit provider and voice credentials. Added configured body limit and handler deadlines; play selects direct track, first search result, or selected/fallback playlist track.
- Wired router/auth state into `RunningWorker`, preserved loopback binding defaults, enforced minimum API token length, and updated short config fixtures.
- Added auth and API integration tests, including auth failures, rate limiting/window reset/table bound, oversized body, health, unknown route, and credential redaction.

## Verification

- `cargo test -p straight-rs-worker --all-targets --all-features --locked` — passed.
- `cargo test -p straight-rs --test node --locked` — passed.
- `cargo fmt --all -- --check` — passed.
- `cargo clippy -p straight-rs-worker --all-targets --all-features -- -D warnings` — passed.
- `git diff --check` — passed.

## Remaining acceptance gaps

The brief's full API contract matrix is not complete: API tests do not yet cover invalid snowflakes/mutation DTOs/ranges, unready `/readyz`, `LoadResult::Empty`/`Error`, explicit-only voice `None` behavior, or secret exclusion from captured tracing/debug/error paths beyond auth response redaction. Signed commit `43bf81d` was created and verified; no push was made. Do not treat this report as full Task 4 acceptance.

## Follow-up (2026-09-30)

- Fixed limiter identity to key buckets by peer IP address (ports no longer partition a client's quota).
- Changed GET player lookup to use `get_player`; absent players return a structured 404 without creating state.
- Play operation maps `Error::NoNode` to sanitized 503; other play failures remain sanitized 502.
- Sanitization now replaces client-error and server-error bodies with a structured public envelope; Bearer scheme parsing is case-insensitive.
- API test harness retains both `RunningWorker` and `MockLavalink` instead of forgetting/dropping them.
- Verified worker all-target/all-feature tests, straight-rs node tests, formatting, worker Clippy with `-D warnings`, and `git diff --check` pass.
- Follow-up signed commit: `7b7e48d`, verified with `git verify-commit HEAD`; no push.
- **Still incomplete:** additional acceptance matrix not implemented: invalid snowflake/DTO/range cases, unready `/readyz`, load Empty/Error assertions, IP same-address/different-port integration assertion, secret scans of captured errors/tracing, and explicit `/leave` versus `/stop` gateway command capture. Do not treat Task 4 acceptance as complete.

## Follow-up (2026-09-30, continuation)

- Added HTTP API coverage for nonnumeric/zero/leading-zero guild IDs, invalid channel IDs, unknown DTO fields across mutating bodies, empty play identifiers, seek beyond 24 hours, and volume above 1000. Assertions verify structured `400` envelopes.
- The new unknown-field cases exposed Axum's default `422`; middleware now normalizes `422 Unprocessable Entity` to the contract's structured `400 Bad Request`.
- Added HTTP middleware coverage proving two source ports at the same peer IP share a rate-limit quota, with table capacity above one; added API-token exclusion checks for auth error body and `AuthState` Debug.
- The explicit gateway-vs-Lavalink readiness API cases, configurable loadtracks `Empty`/`Error` responses, `/stop` versus `/leave` command capture, and public TrackView secret-field assertions remain **not implemented**. No tracing subscriber was added.
- Verification for this continuation (rerun after formatting): `cargo test -p straight-rs-worker --all-targets --all-features --locked`, `cargo test -p straight-rs --test node --locked`, `cargo fmt --all -- --check`, worker Clippy with `-D warnings`, and `git diff --check` all passed.
