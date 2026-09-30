# Task 5 report (continuation C)

Status: DONE. Commit 1e7b7a54cb94b45703a7c23130d4aeefb2a20e7f (signed, verify-commit good). Not pushed.

Changes beyond inherited work
- plugin.rs: `PluginSlot::note_dropped()` (drop count + HEALTHY->LAGGED); after marking LAGGED it re-checks `tx.capacity()==tx.max_capacity()` and CASes LAGGED->HEALTHY, closing the drain-vs-Full race. `Dispatch::send` Full arm calls it. Unit tests `full_queue_marks_lagged`, `drop_noted_after_drain_does_not_stick_lagged` (deterministic, no sleeps).
- tests/plugins.rs: lag-isolation test now waits for `fast` to be Healthy after startup burst, captures `base = dropped_of(fast)`, asserts delta zero, asserts Healthy at end.
- examples/queue-plugin.rs: QueuePlugin + QueueState (enqueue / next_after_end), TrackEnd{may_start_next:true} only, plays via Player::update(UpdateTrack{encoded}); 3 unit tests.
- runtime.rs: no changes needed (skimmed startup unwind and /healthz plugin entries; covered by tests).

Gates: worker all-targets/all-features: lib 15, api 17, auth 5, gateway 7, plugins 7, runtime 7, example 3 - all pass. straight-rs node 15 pass. examples build OK. fmt clean (after `cargo fmt --all`, which reflowed some inherited hunks). clippy -D warnings clean. git diff --check clean. plugins test x5 separate runs: 5/5 pass (35/35 tests).
RED/GREEN: lag-isolation test RED before the fix (controller fact), GREEN after; drain-race unit test was RED before the re-check patch, GREEN after.

# Task 5 FIX ROUND 1

Status: DONE. Commit babd6c8 (signed, verify-commit good; base 1e7b7a5). Not pushed.

Source changes (non-test/non-example)
- error.rs: `#[non_exhaustive]` on WorkerError; new `Lavalink(&'static str)` variant; `From<straight_rs::Error>` now maps to a static category only (no path/session/provider text).
- plugin.rs: new `PluginPlayer` (private inner Player, Debug prints guild only, allowlisted methods, `play_encoded` uses `update_with(.., true)`); `WorkerContext::player` returns it; module docs corrected; compile_fail doctests; `#[non_exhaustive]` on PluginStatus; `Outcome::Cancelled` distinct from Panicked; `mark_unhealthy(reason)` emits `tracing::warn!(plugin, reason)` on transition; `supervise` selects in-flight `call` against `stop`; `PluginHost::shutdown` signals all supervisors first, plugin phase capped at half of the remaining deadline, reverse-order `on_shutdown` capped by min(callback_timeout, remaining phase), then abort+join.
- runtime.rs: duplicate plugin names rejected at build() with WorkerError::Config before any task starts. RunningWorker::shutdown and the startup-failure unwind already used the single shared deadline, so the Gateway/relay/client stages now keep >= half of it (no code change needed there).
- lib.rs: export PluginPlayer.

Tests (tests/plugins.rs 7 -> 16, mock_lavalink: recorded query, fail_player_updates, close_websockets)
- RED before fix: hanging_on_shutdown_is_bounded_and_gateway_still_gets_grace, hanging_in_flight_event_does_not_starve_shutdown (gateway not signalled), duplicate_plugin_names_are_rejected_before_anything_starts.
- Passed immediately: required/optional hanging on_start, degraded_and_node_disconnected_reach_plugins, shutdown_twice_is_safe, plugin_player_play_encoded_patches_with_no_replace, plugin_player_errors_and_debug_carry_no_secrets (these depend on new PluginPlayer API, so they could not compile against the old code; they were written with the implementation).
- Doctests: 1 pass + 2 compile_fail (PluginPlayer has no fetch/update).
- Example: bounded queue cap 100, enqueue -> bool, play_encoded; 4 unit tests.

- plugins test x5 separate runs: 5/5 pass (16/16 each), plus the initial run.

# Task 5 FIX ROUND 3 — event-hook error health reporting

Status: implementation complete; signed commit pending.

- `tests/plugins.rs`: added `FailEvent` and `event_hook_error_marks_only_that_plugin_unhealthy_and_is_reported`. The test verifies a failed event hook makes only that plugin unhealthy, subsequent events still reach the healthy plugin, worker readiness remains true, `/healthz` shows both plugin statuses, and the plugin error text is absent.
- `plugin.rs`: event-hook failures now use the existing static `"failed"` reason through `mark_unhealthy`; no `PluginError` text is surfaced. Module docs describe sticky unhealthy status and no retry.
- RED: `unset SDKROOT; cargo test -p straight-rs-worker --test plugins event_hook_error_marks_only_that_plugin_unhealthy_and_is_reported --locked -- --exact` — 0 passed, 1 failed as expected; timed out waiting for `bad` to become unhealthy (test process exit 101).
- GREEN: same command — 1 passed, 0 failed (exit 0).
- Gates: `cargo test -p straight-rs-worker --test plugins --locked` — 17 passed; `cargo test -p straight-rs-worker --test api --locked` — 17 passed; `cargo test -p straight-rs-worker --all-targets --all-features --locked` — all suites passed (15 unit, API 17, auth 5, gateway 7, plugins 17, runtime 7, queue example 4; other targets 0 tests); `cargo fmt --all -- --check`, worker Clippy with `-D warnings`, and `git diff --check` all passed.
