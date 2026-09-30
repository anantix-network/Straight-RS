use axum::{body::Body, extract::ConnectInfo, http::Request};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};
use straight_rs::NodeConfig;
use straight_rs_model::{GuildId, UserId};
use straight_rs_worker::{
    GatewayCommand, GatewayDriver, GatewayEvent, GatewayFuture, PluginError, PluginFuture,
    PluginStatus, RunningWorker, SecretString, WorkerBuilder, WorkerConfig, WorkerConfigBuilder,
    WorkerContext, WorkerError, WorkerEvent, WorkerPlugin,
};
use tokio::sync::{mpsc, watch};
use tower::ServiceExt;
#[allow(dead_code)]
mod common {
    #[path = "../common/fake_gateway.rs"]
    pub mod fake_gateway;
    #[path = "../common/mock_lavalink.rs"]
    pub mod mock_lavalink;
}
use common::{fake_gateway::ControlledGateway, mock_lavalink::MockLavalink};

const TOKEN: &str = "synthetic-test-api-token-32-bytes-long";
type Log = Arc<Mutex<Vec<String>>>;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Normal,
    FailStart,
    PanicStart,
    PanicEvent,
    HangEvent,
    BlockEvent,
    HangStart,
    HangShutdown,
}
struct Recorder {
    name: &'static str,
    mode: Mode,
    log: Log,
    events: Log,
    release: Arc<tokio::sync::Notify>,
}
impl Recorder {
    fn new(name: &'static str, mode: Mode, log: &Log) -> (Arc<Self>, Log) {
        let events: Log = Arc::default();
        (
            Arc::new(Self {
                name,
                mode,
                log: log.clone(),
                events: events.clone(),
                release: Arc::default(),
            }),
            events,
        )
    }
}
struct Handle(Arc<Recorder>);
impl WorkerPlugin for Handle {
    fn name(&self) -> &'static str {
        self.0.name
    }
    fn on_start(&self, _c: WorkerContext) -> PluginFuture<'_> {
        Box::pin(async move {
            self.0
                .log
                .lock()
                .unwrap()
                .push(format!("{}:start", self.0.name));
            match self.0.mode {
                Mode::FailStart => Err(PluginError::new("boom-secret-text")),
                Mode::PanicStart => panic!("start panic payload"),
                Mode::HangStart => std::future::pending().await,
                _ => Ok(()),
            }
        })
    }
    fn on_event(&self, _c: WorkerContext, event: WorkerEvent) -> PluginFuture<'_> {
        Box::pin(async move {
            self.0.events.lock().unwrap().push(format!("{event:?}"));
            match self.0.mode {
                Mode::PanicEvent => panic!("event panic payload"),
                Mode::HangEvent => std::future::pending().await,
                Mode::BlockEvent => self.0.release.notified().await,
                _ => {}
            }
            Ok(())
        })
    }
    fn on_shutdown(&self, _c: WorkerContext) -> PluginFuture<'_> {
        Box::pin(async move {
            self.0
                .log
                .lock()
                .unwrap()
                .push(format!("{}:shutdown", self.0.name));
            if self.0.mode == Mode::HangShutdown {
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
}

fn config(lavalink: &MockLavalink) -> WorkerConfig {
    WorkerConfigBuilder::new(
        UserId(9),
        SecretString::new("synthetic-bot-secret"),
        SecretString::new(TOKEN),
        vec![NodeConfig::new(
            lavalink.host(),
            "synthetic-lavalink-secret",
        )],
    )
    .build()
    .unwrap()
}
async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("deadline elapsed waiting for {what}"));
}
fn status_of(worker: &RunningWorker, name: &str) -> PluginStatus {
    worker
        .plugin_health()
        .into_iter()
        .find(|p| p.name == name)
        .unwrap()
        .status
}
fn dropped_of(worker: &RunningWorker, name: &str) -> u64 {
    worker
        .plugin_health()
        .into_iter()
        .find(|p| p.name == name)
        .unwrap()
        .dropped_events
}
async fn ready_worker(
    mock: &MockLavalink,
    config: WorkerConfig,
    plugins: Vec<(Arc<Recorder>, bool)>,
) -> RunningWorker {
    ready_worker_with(mock, ControlledGateway::new(false), config, plugins).await
}
async fn ready_worker_with(
    mock: &MockLavalink,
    gateway: impl GatewayDriver,
    config: WorkerConfig,
    plugins: Vec<(Arc<Recorder>, bool)>,
) -> RunningWorker {
    let mut builder = WorkerBuilder::new(config, gateway);
    for (p, required) in plugins {
        builder = if required {
            builder.plugin(Handle(p))
        } else {
            builder.optional_plugin(Handle(p))
        };
    }
    let worker = builder.build().await.unwrap();
    let _ = mock;
    wait_for("readiness", || worker.status().ready).await;
    worker
}
fn has_event(events: &Log, needle: &'static str) -> impl FnMut() -> bool + use<> {
    let events = events.clone();
    move || events.lock().unwrap().iter().any(|e| e.contains(needle))
}

/// Gateway that reports Ready and then ends on demand (drops its event sender).
struct EndingGateway {
    end: Arc<tokio::sync::Notify>,
}
impl GatewayDriver for EndingGateway {
    fn run<'a>(
        &'a self,
        _token: SecretString,
        _bot_user_id: UserId,
        _commands: mpsc::Receiver<GatewayCommand>,
        events: mpsc::Sender<GatewayEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> GatewayFuture<'a> {
        let end = self.end.clone();
        Box::pin(async move {
            events
                .send(GatewayEvent::Ready)
                .await
                .map_err(|_| WorkerError::GatewayClosed)?;
            tokio::select! {
                _ = end.notified() => Ok(()),
                _ = shutdown.changed() => Ok(()),
            }
        })
    }
}

/// Plugin that only hands its `WorkerContext` back to the test.
struct CtxProbe(Arc<Mutex<Option<WorkerContext>>>);
impl WorkerPlugin for CtxProbe {
    fn name(&self) -> &'static str {
        "probe"
    }
    fn on_start(&self, context: WorkerContext) -> PluginFuture<'_> {
        Box::pin(async move {
            *self.0.lock().unwrap() = Some(context);
            Ok(())
        })
    }
    fn on_event(&self, _c: WorkerContext, _e: WorkerEvent) -> PluginFuture<'_> {
        Box::pin(async { Ok(()) })
    }
    fn on_shutdown(&self, _c: WorkerContext) -> PluginFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}
async fn probe_worker(mock: &MockLavalink) -> (RunningWorker, WorkerContext) {
    let slot: Arc<Mutex<Option<WorkerContext>>> = Arc::default();
    let worker = WorkerBuilder::new(config(mock), ControlledGateway::new(false))
        .plugin(CtxProbe(slot.clone()))
        .build()
        .await
        .unwrap();
    wait_for("readiness", || worker.status().ready).await;
    let context = slot.lock().unwrap().clone().expect("on_start ran");
    (worker, context)
}

#[tokio::test]
async fn startup_in_order_events_cloned_and_shutdown_reversed() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (a, a_events) = Recorder::new("a", Mode::Normal, &log);
    let (b, b_events) = Recorder::new("b", Mode::Normal, &log);
    let mut worker = ready_worker(&mock, config(&mock), vec![(a, true), (b, true)]).await;
    assert_eq!(*log.lock().unwrap(), ["a:start", "b:start"]);
    mock.push_track_end(1, "enc-1", "finished");
    wait_for("delivery", || {
        a_events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.contains("TrackEnd"))
            && b_events
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.contains("TrackEnd"))
    })
    .await;
    for events in [&a_events, &b_events] {
        let events = events.lock().unwrap();
        assert!(events.iter().any(|e| e.contains("Ready")), "{events:?}");
        assert!(
            events
                .iter()
                .any(|e| e.contains("TrackEnd") && e.contains("may_start_next: true"))
        );
    }
    worker.shutdown().await.unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        ["a:start", "b:start", "b:shutdown", "a:shutdown"]
    );
    let names: Vec<_> = worker.plugin_health().iter().map(|p| p.name).collect();
    assert_eq!(names, ["a", "b"]);
}

#[tokio::test]
async fn required_startup_failure_fails_build_and_stops_gateway() {
    for mode in [Mode::FailStart, Mode::PanicStart] {
        let mock = MockLavalink::start().await;
        let log: Log = Arc::default();
        let (a, _) = Recorder::new("a", Mode::Normal, &log);
        let (bad, _) = Recorder::new("bad", mode, &log);
        let (never, _) = Recorder::new("never", Mode::Normal, &log);
        let gateway = ControlledGateway::new(false);
        let stopped = gateway.stopped.clone();
        let result = WorkerBuilder::new(config(&mock), gateway)
            .plugin(Handle(a))
            .plugin(Handle(bad))
            .plugin(Handle(never))
            .build()
            .await;
        let err = result
            .err()
            .expect("required plugin failure must fail build");
        let text = err.to_string();
        assert!(
            !text.contains("boom-secret-text") && !text.contains("payload"),
            "{text}"
        );
        assert!(
            stopped.load(Ordering::SeqCst),
            "gateway task must be stopped"
        );
        let log = log.lock().unwrap().clone();
        assert!(!log.contains(&"never:start".to_string()), "{log:?}");
        assert!(log.contains(&"a:shutdown".to_string()), "{log:?}");
    }
}

#[tokio::test]
async fn optional_startup_failure_leaves_worker_running() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (bad, bad_events) = Recorder::new("bad", Mode::FailStart, &log);
    let (good, good_events) = Recorder::new("good", Mode::Normal, &log);
    let worker = ready_worker(&mock, config(&mock), vec![(bad, false), (good, true)]).await;
    assert_eq!(status_of(&worker, "bad"), PluginStatus::Unhealthy);
    assert_eq!(status_of(&worker, "good"), PluginStatus::Healthy);
    mock.push_track_end(1, "enc", "finished");
    wait_for("good delivery", || {
        good_events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.contains("TrackEnd"))
    })
    .await;
    assert!(bad_events.lock().unwrap().is_empty());
    let health = worker.plugin_health();
    assert!(!health[0].required && health[1].required);
}

#[tokio::test]
async fn panicking_plugin_is_unhealthy_and_isolated() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (bad, _) = Recorder::new("bad", Mode::PanicEvent, &log);
    let (good, good_events) = Recorder::new("good", Mode::Normal, &log);
    let worker = ready_worker(&mock, config(&mock), vec![(bad, true), (good, true)]).await;
    wait_for("panic", || {
        status_of(&worker, "bad") == PluginStatus::Unhealthy
    })
    .await;
    mock.push_track_end(1, "enc", "finished");
    wait_for("good delivery", || {
        good_events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.contains("TrackEnd"))
    })
    .await;
    assert_eq!(status_of(&worker, "good"), PluginStatus::Healthy);
    assert!(worker.status().ready);
}

#[tokio::test]
async fn hanging_callback_hits_deadline() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (bad, _) = Recorder::new("bad", Mode::HangEvent, &log);
    let (good, good_events) = Recorder::new("good", Mode::Normal, &log);
    let mut cfg = config(&mock);
    cfg.callback_timeout = Duration::from_millis(100);
    let worker = ready_worker(&mock, cfg, vec![(bad, true), (good, true)]).await;
    wait_for("timeout", || {
        status_of(&worker, "bad") == PluginStatus::Unhealthy
    })
    .await;
    mock.push_track_end(1, "enc", "finished");
    wait_for("good delivery", || {
        good_events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.contains("TrackEnd"))
    })
    .await;
    assert!(worker.status().ready);
}

#[tokio::test]
async fn full_queue_lags_only_that_plugin_and_never_stalls_relay() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (slow, slow_events) = Recorder::new("slow", Mode::BlockEvent, &log);
    let release = slow.release.clone();
    let (fast, fast_events) = Recorder::new("fast", Mode::Normal, &log);
    let mut cfg = config(&mock);
    cfg.plugin_event_capacity = 1;
    let worker = ready_worker(&mock, cfg, vec![(slow, true), (fast, true)]).await;
    let count = |events: &Log| {
        events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.contains("TrackEnd"))
            .count()
    };
    // `fast` must have consumed `Ready`, and each subsequent event, before the next push,
    // so its one-slot queue is never legitimately full. `slow` is blocked in its first
    // callback (`Ready`), fills its queue with the first TrackEnd and drops the rest.
    wait_for("fast Ready", || {
        fast_events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.contains("Ready"))
    })
    .await;
    // Startup events (`NodeConnected`, `Ready`) are dispatched back-to-back, so a one-slot
    // queue may legitimately have dropped one for `fast` and briefly marked it Lagged.
    // Wait for it to drain and recover, then measure drops as a delta from here.
    wait_for("fast recovers after startup burst", || {
        status_of(&worker, "fast") == PluginStatus::Healthy
    })
    .await;
    let base = dropped_of(&worker, "fast");
    for i in 0..5 {
        mock.push_track_end(1, &format!("enc-{i}"), "finished");
        wait_for("fast delivery", || count(&fast_events) == i + 1).await;
    }
    wait_for("lag", || status_of(&worker, "slow") == PluginStatus::Lagged).await;
    assert!(dropped_of(&worker, "slow") >= 1);
    assert_eq!(dropped_of(&worker, "fast"), base);
    wait_for("fast healthy at end", || {
        status_of(&worker, "fast") == PluginStatus::Healthy
    })
    .await;
    assert!(worker.status().ready, "relay/readiness must keep working");
    // Releasing the blocked plugin drains its queue and clears Lagged; the drop count is kept.
    wait_for("drain", || {
        release.notify_one();
        status_of(&worker, "slow") == PluginStatus::Healthy
    })
    .await;
    assert!(dropped_of(&worker, "slow") >= 1);
    // The recovered plugin receives new events again.
    let before = count(&slow_events);
    mock.push_track_end(1, "enc-after", "finished");
    wait_for("slow receives new event", || {
        release.notify_one();
        count(&slow_events) > before
    })
    .await;
}

#[tokio::test]
async fn healthz_reports_plugins_without_error_text_and_events_hide_user_data() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (bad, _) = Recorder::new("bad", Mode::FailStart, &log);
    let (good, good_events) = Recorder::new("good", Mode::Normal, &log);
    let worker = ready_worker(&mock, config(&mock), vec![(bad, false), (good, true)]).await;
    mock.push_track_end(1, "enc", "finished");
    wait_for("delivery", || {
        good_events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.contains("TrackEnd"))
    })
    .await;
    let debug = good_events.lock().unwrap().join("\n");
    assert!(debug.contains("title") || debug.contains("t\""), "{debug}");
    for secret in [
        "synthetic-user-secret",
        "synthetic-plugin-secret",
        "userSecret",
        "pluginSecret",
    ] {
        assert!(!debug.contains(secret), "{secret} leaked: {debug}");
    }
    let response = worker
        .router()
        .oneshot(
            Request::get("/healthz")
                .header("authorization", format!("Bearer {TOKEN}"))
                .extension(ConnectInfo(
                    "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(!text.contains("boom-secret-text"), "{text}");
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(json["status"], "ok");
    assert_eq!(
        json["plugins"],
        serde_json::json!([
            {"name": "bad", "required": false, "status": "unhealthy", "droppedEvents": 0},
            {"name": "good", "required": true, "status": "healthy", "droppedEvents": 0},
        ])
    );
}

fn shutdown_config(mock: &MockLavalink) -> WorkerConfig {
    let mut cfg = config(mock);
    // A callback timeout far larger than the whole shutdown budget: only the shared
    // deadline can bound a wedged hook.
    cfg.callback_timeout = Duration::from_secs(5);
    cfg.shutdown_timeout = Duration::from_millis(600);
    cfg
}
const SHUTDOWN_SLACK: Duration = Duration::from_millis(500);

#[tokio::test]
async fn hanging_on_shutdown_is_bounded_and_gateway_still_gets_grace() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (a, _) = Recorder::new("a", Mode::HangShutdown, &log);
    let (b, _) = Recorder::new("b", Mode::Normal, &log);
    let (c, _) = Recorder::new("c", Mode::Normal, &log);
    let gateway = ControlledGateway::new(false);
    let stopped = gateway.stopped.clone();
    let cfg = shutdown_config(&mock);
    let budget = cfg.shutdown_timeout;
    let mut worker =
        ready_worker_with(&mock, gateway, cfg, vec![(a, true), (b, true), (c, true)]).await;
    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(5), worker.shutdown())
        .await
        .expect("shutdown must be bounded");
    let elapsed = started.elapsed();
    assert!(elapsed < budget + SHUTDOWN_SLACK, "took {elapsed:?}");
    let shutdowns: Vec<_> = log
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.ends_with(":shutdown"))
        .cloned()
        .collect();
    assert_eq!(shutdowns, ["c:shutdown", "b:shutdown", "a:shutdown"]);
    assert!(
        stopped.load(Ordering::SeqCst),
        "gateway must be signalled, not aborted"
    );
    assert!(
        result.is_ok(),
        "gateway had grace to stop cleanly: {result:?}"
    );
    assert_eq!(status_of(&worker, "a"), PluginStatus::Unhealthy);
}

#[tokio::test]
async fn hanging_in_flight_event_does_not_starve_shutdown() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (hang, hang_events) = Recorder::new("hang", Mode::HangEvent, &log);
    let (good, _) = Recorder::new("good", Mode::Normal, &log);
    let gateway = ControlledGateway::new(false);
    let stopped = gateway.stopped.clone();
    let cfg = shutdown_config(&mock);
    let budget = cfg.shutdown_timeout;
    let mut worker = ready_worker_with(&mock, gateway, cfg, vec![(hang, true), (good, true)]).await;
    wait_for("hang plugin in flight", has_event(&hang_events, "Ready")).await;
    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(5), worker.shutdown())
        .await
        .expect("shutdown must be bounded");
    let elapsed = started.elapsed();
    assert!(elapsed < budget + SHUTDOWN_SLACK, "took {elapsed:?}");
    let log = log.lock().unwrap().clone();
    assert!(log.contains(&"good:shutdown".to_string()), "{log:?}");
    assert!(log.contains(&"hang:shutdown".to_string()), "{log:?}");
    assert!(stopped.load(Ordering::SeqCst));
    assert!(result.is_ok(), "{result:?}");
}

#[tokio::test]
async fn required_hanging_start_fails_build_with_plugin_error_and_stops_gateway() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (a, _) = Recorder::new("a", Mode::Normal, &log);
    let (bad, _) = Recorder::new("bad", Mode::HangStart, &log);
    let gateway = ControlledGateway::new(false);
    let stopped = gateway.stopped.clone();
    let mut cfg = config(&mock);
    cfg.callback_timeout = Duration::from_millis(150);
    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        WorkerBuilder::new(cfg, gateway)
            .plugin(Handle(a))
            .plugin(Handle(bad))
            .build(),
    )
    .await
    .expect("build must be bounded");
    assert!(started.elapsed() >= Duration::from_millis(150));
    match result.err().expect("required hanging start fails build") {
        WorkerError::Plugin { name, .. } => assert_eq!(name, "bad"),
        other => panic!("unexpected error {other:?}"),
    }
    assert!(stopped.load(Ordering::SeqCst), "gateway must be stopped");
    assert!(log.lock().unwrap().contains(&"a:shutdown".to_string()));
}

#[tokio::test]
async fn optional_hanging_start_leaves_worker_up_and_plugin_unhealthy() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (bad, _) = Recorder::new("bad", Mode::HangStart, &log);
    let (good, good_events) = Recorder::new("good", Mode::Normal, &log);
    let mut cfg = config(&mock);
    cfg.callback_timeout = Duration::from_millis(150);
    let mut worker = ready_worker(&mock, cfg, vec![(bad, false), (good, true)]).await;
    assert_eq!(status_of(&worker, "bad"), PluginStatus::Unhealthy);
    assert_eq!(status_of(&worker, "good"), PluginStatus::Healthy);
    wait_for("good Ready", has_event(&good_events, "Ready")).await;
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn degraded_and_node_disconnected_reach_plugins() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (p, events) = Recorder::new("p", Mode::Normal, &log);
    let end = Arc::new(tokio::sync::Notify::new());
    let mut worker = ready_worker_with(
        &mock,
        EndingGateway { end: end.clone() },
        config(&mock),
        vec![(p, true)],
    )
    .await;
    mock.close_websockets();
    wait_for("NodeDisconnected", has_event(&events, "NodeDisconnected")).await;
    // The gateway ending closes the event channel, which the relay reports as Degraded.
    end.notify_one();
    wait_for("Degraded", has_event(&events, "Degraded")).await;
    assert!(worker.status().degraded);
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_twice_is_safe() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (a, _) = Recorder::new("a", Mode::Normal, &log);
    let mut worker = ready_worker(&mock, config(&mock), vec![(a, true)]).await;
    worker.shutdown().await.unwrap();
    worker.shutdown().await.unwrap();
    let shutdowns = log
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.ends_with(":shutdown"))
        .count();
    assert_eq!(shutdowns, 1, "on_shutdown runs once");
}

#[tokio::test]
async fn duplicate_plugin_names_are_rejected_before_anything_starts() {
    let mock = MockLavalink::start().await;
    let log: Log = Arc::default();
    let (first, _) = Recorder::new("dup", Mode::Normal, &log);
    let (second, _) = Recorder::new("dup", Mode::Normal, &log);
    let gateway = ControlledGateway::new(false);
    let stopped = gateway.stopped.clone();
    let result = WorkerBuilder::new(config(&mock), gateway)
        .plugin(Handle(first))
        .optional_plugin(Handle(second))
        .build()
        .await;
    match result.err().expect("duplicate names must fail build") {
        WorkerError::Config(message) => assert!(message.contains("dup"), "{message}"),
        other => panic!("unexpected error {other:?}"),
    }
    assert!(log.lock().unwrap().is_empty(), "no plugin hook may run");
    assert!(!stopped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn plugin_player_play_encoded_patches_with_no_replace() {
    let mock = MockLavalink::start().await;
    let (mut worker, context) = probe_worker(&mock).await;
    let player = context.player(GuildId(1));
    player.play_encoded("enc-next").await.unwrap();
    let patch = mock
        .requests()
        .into_iter()
        .find(|r| r.method == "PATCH" && r.path.contains("/players/1"))
        .expect("player PATCH reached the node");
    assert_eq!(patch.body["track"]["encoded"], "enc-next");
    assert!(patch.query.contains("noReplace=true"), "{}", patch.query);
    assert_eq!(player.guild_id(), GuildId(1));
    let track = player.track().expect("track projected");
    assert_eq!(&*track.encoded, "enc-next");
    let shown = format!("{track:?}");
    assert!(!shown.contains("synthetic-user-secret") && !shown.contains("synthetic-plugin-secret"));
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn plugin_player_errors_and_debug_carry_no_secrets() {
    let mock = MockLavalink::start().await;
    let (mut worker, context) = probe_worker(&mock).await;
    let player = context.player(GuildId(1));
    // A successful update makes the player hold a voice-bearing response.
    player.play_encoded("enc-ok").await.unwrap();
    let debug = format!("{player:?}");
    for leaked in [
        "synthetic-voice-token",
        "synthetic-voice-endpoint",
        "synthetic-voice-session",
        "token",
        "endpoint",
    ] {
        assert!(!debug.contains(leaked), "{leaked} in {debug}");
    }
    assert!(debug.contains("PluginPlayer"), "{debug}");
    mock.fail_player_updates(true);
    let error = player.set_volume(50).await.expect_err("node answers 500");
    let error2 = player
        .play_encoded("enc-again")
        .await
        .expect_err("node answers 500");
    for error in [error, error2] {
        for text in [error.to_string(), format!("{error:?}")] {
            for leaked in [
                "synthetic-provider-secret",
                "/v4/sessions",
                "test-session",
                "Internal Server Error",
            ] {
                assert!(!text.contains(leaked), "{leaked} in {text}");
            }
        }
    }
    worker.shutdown().await.unwrap();
}
