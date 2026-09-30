use axum::{body::Body, extract::ConnectInfo, http::Request};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use straight_rs::NodeConfig;
use straight_rs_model::UserId;
use straight_rs_worker::{
    PluginError, PluginFuture, PluginStatus, RunningWorker, SecretString, WorkerBuilder,
    WorkerConfig, WorkerConfigBuilder, WorkerContext, WorkerEvent, WorkerPlugin,
};
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
    let mut builder = WorkerBuilder::new(config, ControlledGateway::new(false));
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
