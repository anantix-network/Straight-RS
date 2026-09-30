//! Bounded, supervised, statically registered worker plugins.
//!
//! Plugins are trusted in-process code registered through
//! [`WorkerBuilder::plugin`](crate::WorkerBuilder::plugin) /
//! [`optional_plugin`](crate::WorkerBuilder::optional_plugin). Each plugin has its own
//! bounded event queue and supervisor task, and every callback runs in its own spawned
//! task under `config.callback_timeout`. A panic or timeout marks only that plugin
//! [`PluginStatus::Unhealthy`] (sticky, never retried). A full queue drops the event,
//! counts it and marks that plugin [`PluginStatus::Lagged`]; the Gateway/Lavalink relay
//! never blocks.
//!
//! Plugin hooks must not block synchronously; they must yield while awaiting work. The
//! callback timeout is cooperative: timeout/cancellation is effective only while Tokio
//! can schedule and poll the hook. A hook that blocks its executor thread without yielding
//! cannot be interrupted by task timeout or abort and can starve other tasks. Hard
//! preemption requires process isolation, which is out of scope for these trusted,
//! statically linked in-process plugins.
//!
//! What a plugin can reach: the whitelisted [`WorkerEvent`]s (tracks are sanitized
//! [`PluginTrack`]s without `user_data` or Lavalink plugin metadata) and a
//! [`WorkerContext`]. `WorkerContext::player` returns a [`PluginPlayer`], a fixed
//! allowlist of playback controls that exposes no voice state, voice token, endpoint,
//! session id, raw `Player` or `LavalinkClient`. `WorkerContext::load` returns the node's
//! `LoadResult` as-is, so its tracks *do* carry `plugin_info`/`user_data` as supplied by
//! Lavalink. Errors from `PluginPlayer` and `load` carry only a static category (no REST
//! path, session id or provider text). Errors returned from `on_event` are discarded;
//! errors from `on_start` fail a required plugin's startup. Error text and panic payloads
//! are never exposed via `/healthz`.
//!
//! Plugins remain trusted in-process code (they can spawn tasks, read the process
//! environment and open sockets): `PluginPlayer` narrows the *API* handed to them, it is
//! not a sandbox.

use crate::{
    error::{WorkerError, WorkerResult},
    state::{StatusState, WorkerStatus},
};
use serde::Serialize;
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use straight_rs::{
    Event, GuildId, LavalinkClient, LoadResult, Player, Track, UpdatePlayer, UpdateTrack,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

/// Error returned by a plugin hook. The message is for the plugin's own use and is never
/// exposed by the worker's HTTP API.
#[derive(Debug, Clone)]
pub struct PluginError(String);
impl PluginError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}
impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for PluginError {}

pub type PluginResult = std::result::Result<(), PluginError>;
pub type PluginFuture<'a> = straight_rs::BoxFuture<'a, PluginResult>;

/// Trusted in-process plugin hooks. Implementations must not block synchronously; use
/// yielding async operations instead. Callback deadlines can time out/cancel a hook only
/// while Tokio can schedule and poll it. Hard preemption requires process isolation, which
/// is outside this in-process plugin contract.
pub trait WorkerPlugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn on_start(&self, context: WorkerContext) -> PluginFuture<'_>;
    fn on_event(&self, context: WorkerContext, event: WorkerEvent) -> PluginFuture<'_>;
    fn on_shutdown(&self, context: WorkerContext) -> PluginFuture<'_>;
}

/// Sanitized projection of a track: no `user_data`, no Lavalink plugin metadata.
#[derive(Clone, Debug)]
pub struct PluginTrack {
    pub encoded: Arc<str>,
    pub title: Arc<str>,
    pub author: Arc<str>,
    pub length_ms: u64,
    pub is_stream: bool,
    pub source_name: Arc<str>,
}
impl From<&Track> for PluginTrack {
    fn from(track: &Track) -> Self {
        Self {
            encoded: track.encoded.clone(),
            title: track.info.title.as_str().into(),
            author: track.info.author.as_str().into(),
            length_ms: track.info.length,
            is_stream: track.info.is_stream,
            source_name: track.info.source_name.as_str().into(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum WorkerEvent {
    TrackStart {
        guild: GuildId,
        track: PluginTrack,
    },
    TrackEnd {
        guild: GuildId,
        track: PluginTrack,
        may_start_next: bool,
    },
    TrackException {
        guild: GuildId,
        track: PluginTrack,
    },
    TrackStuck {
        guild: GuildId,
        track: PluginTrack,
        threshold_ms: u64,
    },
    NodeConnected {
        node: usize,
    },
    NodeDisconnected {
        node: usize,
    },
    PlayerMigrated {
        guild: GuildId,
        from: usize,
        to: usize,
    },
    Ready,
    Degraded,
}
impl WorkerEvent {
    /// Whitelist mapping; unknown / unlisted Lavalink events are never forwarded.
    pub(crate) fn from_lavalink(event: &Event) -> Option<Self> {
        Some(match event {
            Event::NodeConnected { node } => Self::NodeConnected { node: *node },
            Event::NodeDisconnected { node } => Self::NodeDisconnected { node: *node },
            Event::TrackStart { guild, track, .. } => Self::TrackStart {
                guild: *guild,
                track: track.as_ref().into(),
            },
            Event::TrackEnd {
                guild,
                track,
                reason,
                ..
            } => Self::TrackEnd {
                guild: *guild,
                track: track.as_ref().into(),
                may_start_next: reason.may_start_next(),
            },
            Event::TrackException { guild, track, .. } => Self::TrackException {
                guild: *guild,
                track: track.as_ref().into(),
            },
            Event::TrackStuck {
                guild,
                track,
                threshold_ms,
                ..
            } => Self::TrackStuck {
                guild: *guild,
                track: track.as_ref().into(),
                threshold_ms: *threshold_ms,
            },
            Event::PlayerMigrated { guild, from, to } => Self::PlayerMigrated {
                guild: *guild,
                from: *from,
                to: *to,
            },
            _ => return None,
        })
    }
}

/// Capability handle handed to plugins. Exposes playback and status only.
#[derive(Clone)]
pub struct WorkerContext {
    client: LavalinkClient,
    status: watch::Receiver<WorkerStatus>,
}
impl WorkerContext {
    pub(crate) fn new(client: LavalinkClient, status: watch::Receiver<WorkerStatus>) -> Self {
        Self { client, status }
    }
    /// Get-or-create the allowlisted player handle for a guild.
    pub fn player(&self, guild: GuildId) -> PluginPlayer {
        PluginPlayer {
            inner: self.client.player(guild),
        }
    }
    /// Resolve `identifier` on a node. Errors carry only a static category.
    pub async fn load(&self, identifier: &str) -> WorkerResult<LoadResult> {
        self.client
            .load(identifier)
            .await
            .map_err(WorkerError::from)
    }
    pub fn status(&self) -> WorkerStatus {
        self.status.borrow().clone()
    }
}

/// Allowlisted playback handle for one guild.
///
/// It wraps the client's `Player` privately and exposes only: `guild_id`, `position`,
/// `is_paused`, `volume`, `track` (sanitized), `play_encoded`, `stop`, `pause`, `seek`
/// and `set_volume`. There is deliberately no way to reach the voice state
/// (token/endpoint/session id), filters, events, raw updates, or to
/// destroy/join/leave. Errors carry only a static category string.
///
/// ```compile_fail,E0599
/// fn leak(player: straight_rs_worker::PluginPlayer) {
///     let _ = player.fetch();
/// }
/// ```
///
/// ```compile_fail,E0599
/// fn raw(player: straight_rs_worker::PluginPlayer) {
///     let _ = player.update(Default::default());
/// }
/// ```
///
/// ```
/// fn ok(player: straight_rs_worker::PluginPlayer) -> u16 {
///     player.volume()
/// }
/// ```
#[derive(Clone)]
pub struct PluginPlayer {
    inner: Player,
}
impl fmt::Debug for PluginPlayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginPlayer")
            .field("guild", &self.inner.guild_id())
            .finish()
    }
}
impl PluginPlayer {
    pub fn guild_id(&self) -> GuildId {
        self.inner.guild_id()
    }
    pub fn position(&self) -> u64 {
        self.inner.position()
    }
    pub fn is_paused(&self) -> bool {
        self.inner.is_paused()
    }
    pub fn volume(&self) -> u16 {
        self.inner.volume()
    }
    /// The current track, without `user_data` or plugin metadata.
    pub fn track(&self) -> Option<PluginTrack> {
        self.inner.track().as_deref().map(PluginTrack::from)
    }
    /// Start `encoded` unless a track is already playing (`noReplace=true`).
    pub async fn play_encoded(&self, encoded: &str) -> WorkerResult<()> {
        let update = UpdatePlayer {
            track: Some(UpdateTrack {
                encoded: Some(Some(encoded.into())),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.inner
            .update_with(update, true)
            .await
            .map_err(WorkerError::from)
    }
    pub async fn stop(&self) -> WorkerResult<()> {
        self.inner.stop().await.map_err(WorkerError::from)
    }
    pub async fn pause(&self, paused: bool) -> WorkerResult<()> {
        self.inner.pause(paused).await.map_err(WorkerError::from)
    }
    pub async fn seek(&self, position_ms: u64) -> WorkerResult<()> {
        self.inner
            .seek(position_ms)
            .await
            .map_err(WorkerError::from)
    }
    pub async fn set_volume(&self, volume: u16) -> WorkerResult<()> {
        self.inner
            .set_volume(volume)
            .await
            .map_err(WorkerError::from)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum PluginStatus {
    Healthy,
    Lagged,
    Unhealthy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginHealth {
    pub name: &'static str,
    pub required: bool,
    pub status: PluginStatus,
    pub dropped_events: u64,
}

const HEALTHY: u8 = 0;
const LAGGED: u8 = 1;
const UNHEALTHY: u8 = 2;

pub(crate) struct PluginSlot {
    name: &'static str,
    required: bool,
    plugin: Arc<dyn WorkerPlugin>,
    tx: mpsc::Sender<WorkerEvent>,
    status: AtomicU8,
    dropped: AtomicU64,
}
impl PluginSlot {
    fn mark_unhealthy(&self, reason: &'static str) {
        let previous = self.status.swap(UNHEALTHY, Ordering::AcqRel);
        if previous != UNHEALTHY {
            // Static reason only: never error text or panic payloads.
            tracing::warn!(plugin = self.name, reason, "plugin marked unhealthy");
        }
    }
    /// Called after `try_send` reported a full queue: counts the drop and marks the slot
    /// `Lagged`.
    fn note_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        let _ = self
            .status
            .compare_exchange(HEALTHY, LAGGED, Ordering::AcqRel, Ordering::Acquire);
        // The supervisor may have drained the queue between `try_send` failing and the
        // mark above; its clear then saw HEALTHY and did nothing. Re-check so an idle,
        // empty queue never stays Lagged. (If the supervisor drains after this check it
        // clears the mark itself.)
        if self.tx.capacity() == self.tx.max_capacity() {
            let _ =
                self.status
                    .compare_exchange(LAGGED, HEALTHY, Ordering::AcqRel, Ordering::Acquire);
        }
    }
    fn health(&self) -> PluginHealth {
        PluginHealth {
            name: self.name,
            required: self.required,
            status: match self.status.load(Ordering::Acquire) {
                HEALTHY => PluginStatus::Healthy,
                LAGGED => PluginStatus::Lagged,
                _ => PluginStatus::Unhealthy,
            },
            dropped_events: self.dropped.load(Ordering::Relaxed),
        }
    }
}

/// Non-blocking fan-out shared with the relay task and the health endpoint.
pub(crate) struct Dispatch {
    slots: Vec<Arc<PluginSlot>>,
    stopped: AtomicBool,
}
impl Dispatch {
    pub(crate) fn send(&self, event: WorkerEvent) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        for slot in &self.slots {
            match slot.tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    slot.note_dropped();
                }
                // Receiver gone: the plugin failed or never started; already Unhealthy.
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
    }
    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
    pub(crate) fn health(&self) -> Vec<PluginHealth> {
        self.slots.iter().map(|s| s.health()).collect()
    }
}

/// Publishes status snapshots and emits `Ready`/`Degraded` from the relay task.
pub(crate) struct StatusPublisher {
    tx: watch::Sender<WorkerStatus>,
    dispatch: Arc<Dispatch>,
    ready_sent: bool,
    degraded_sent: bool,
}
impl StatusPublisher {
    pub(crate) fn new(tx: watch::Sender<WorkerStatus>, dispatch: Arc<Dispatch>) -> Self {
        Self {
            tx,
            dispatch,
            ready_sent: false,
            degraded_sent: false,
        }
    }
    pub(crate) fn dispatch(&self, event: WorkerEvent) {
        self.dispatch.send(event);
    }
    pub(crate) fn publish(&mut self, state: &StatusState) {
        let snapshot = state.snapshot();
        self.tx.send_if_modified(|current| {
            if *current == snapshot {
                false
            } else {
                *current = snapshot.clone();
                true
            }
        });
        if snapshot.ready && !self.ready_sent {
            self.ready_sent = true;
            self.dispatch.send(WorkerEvent::Ready);
        }
        if snapshot.degraded && !self.degraded_sent {
            self.degraded_sent = true;
            self.dispatch.send(WorkerEvent::Degraded);
        }
    }
}

enum Hook {
    Start,
    Event(WorkerEvent),
    Shutdown,
}
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    Failed,
    Panicked,
    TimedOut,
    Cancelled,
}
impl Outcome {
    /// Static, payload-free reason used for logging and transitions to Unhealthy.
    fn reason(&self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Failed => "failed",
            Outcome::Panicked => "panicked",
            Outcome::TimedOut => "timed_out",
            Outcome::Cancelled => "cancelled",
        }
    }
}
struct AbortOnDrop(JoinHandle<PluginResult>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Run one hook in its own task under `timeout`; the task is aborted on timeout or when
/// the supervising future is dropped.
async fn call(
    plugin: Arc<dyn WorkerPlugin>,
    context: WorkerContext,
    hook: Hook,
    timeout: Duration,
) -> Outcome {
    let mut guard = AbortOnDrop(tokio::spawn(async move {
        match hook {
            Hook::Start => plugin.on_start(context).await,
            Hook::Event(event) => plugin.on_event(context, event).await,
            Hook::Shutdown => plugin.on_shutdown(context).await,
        }
    }));
    match tokio::time::timeout(timeout, &mut guard.0).await {
        Ok(Ok(Ok(()))) => Outcome::Ok,
        Ok(Ok(Err(_))) => Outcome::Failed,
        Ok(Err(e)) if e.is_cancelled() => Outcome::Cancelled,
        Ok(Err(_)) => Outcome::Panicked,
        Err(_) => Outcome::TimedOut,
    }
}

struct PluginHandle {
    slot: Arc<PluginSlot>,
    rx: Option<mpsc::Receiver<WorkerEvent>>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
    initialized: bool,
}

pub(crate) struct PluginHost {
    pub(crate) dispatch: Arc<Dispatch>,
    handles: Vec<PluginHandle>,
}

pub(crate) struct PluginRegistration {
    pub plugin: Arc<dyn WorkerPlugin>,
    pub required: bool,
}

impl PluginHost {
    pub(crate) fn new(registrations: Vec<PluginRegistration>, capacity: usize) -> Self {
        let mut slots = Vec::new();
        let mut handles = Vec::new();
        for registration in registrations {
            let (tx, rx) = mpsc::channel(capacity);
            let slot = Arc::new(PluginSlot {
                name: registration.plugin.name(),
                required: registration.required,
                plugin: registration.plugin,
                tx,
                status: AtomicU8::new(HEALTHY),
                dropped: AtomicU64::new(0),
            });
            slots.push(slot.clone());
            handles.push(PluginHandle {
                slot,
                rx: Some(rx),
                stop: watch::channel(false).0,
                task: None,
                initialized: false,
            });
        }
        Self {
            dispatch: Arc::new(Dispatch {
                slots,
                stopped: AtomicBool::new(false),
            }),
            handles,
        }
    }

    /// Sequential `on_start` in registration order. A required failure returns an error
    /// (the caller must then shut the worker down); an optional one marks it Unhealthy.
    pub(crate) async fn start(
        &mut self,
        context: &WorkerContext,
        timeout: Duration,
    ) -> WorkerResult<()> {
        for handle in &mut self.handles {
            let slot = handle.slot.clone();
            let outcome = call(slot.plugin.clone(), context.clone(), Hook::Start, timeout).await;
            if outcome != Outcome::Ok {
                slot.mark_unhealthy(outcome.reason());
                handle.rx = None;
                if slot.required {
                    return Err(WorkerError::Plugin {
                        name: slot.name,
                        reason: match outcome {
                            Outcome::Panicked => "panicked during startup",
                            Outcome::TimedOut => "startup timed out",
                            _ => "startup failed",
                        },
                    });
                }
                continue;
            }
            handle.initialized = true;
            let rx = handle.rx.take().expect("plugin started once");
            let mut stop = handle.stop.subscribe();
            let ctx = context.clone();
            handle.task = Some(tokio::spawn(supervise(
                slot,
                rx,
                ctx,
                timeout,
                async move {
                    let _ = stop.wait_for(|v| *v).await;
                },
            )));
        }
        Ok(())
    }

    pub(crate) fn health(&self) -> Vec<PluginHealth> {
        self.dispatch.health()
    }

    /// Plugin shutdown phase, bounded by half of the time left until `deadline` so the
    /// Gateway/relay/client stages always keep at least the other half.
    ///
    /// Every supervisor is signalled first (dropping and thereby aborting any in-flight
    /// callback). Then, per plugin in reverse registration order: join its supervisor and
    /// run `on_shutdown` (initialized plugins only), each hook capped by
    /// `min(timeout, remaining plugin-phase time)`. Anything still running when the phase
    /// ends is aborted and joined. Idempotent.
    pub(crate) async fn shutdown(
        &mut self,
        context: &WorkerContext,
        timeout: Duration,
        deadline: tokio::time::Instant,
    ) {
        self.dispatch.stop();
        let now = tokio::time::Instant::now();
        let phase_deadline = now + deadline.saturating_duration_since(now) / 2;
        for handle in &self.handles {
            let _ = handle.stop.send(true);
        }
        for handle in self.handles.iter_mut().rev() {
            if let Some(mut task) = handle.task.take()
                && tokio::time::timeout_at(phase_deadline, &mut task)
                    .await
                    .is_err()
            {
                task.abort();
                let _ = task.await;
                handle.slot.mark_unhealthy("timed_out");
            }
            if handle.initialized {
                handle.initialized = false;
                let remaining =
                    phase_deadline.saturating_duration_since(tokio::time::Instant::now());
                let outcome = call(
                    handle.slot.plugin.clone(),
                    context.clone(),
                    Hook::Shutdown,
                    timeout.min(remaining),
                )
                .await;
                if outcome != Outcome::Ok {
                    handle.slot.mark_unhealthy(outcome.reason());
                }
            }
        }
    }

    /// Non-async cleanup for `Drop`: aborts only.
    pub(crate) fn abort(&mut self) {
        self.dispatch.stop();
        for handle in &mut self.handles {
            if let Some(task) = handle.task.take() {
                task.abort();
            }
        }
    }
}

async fn supervise(
    slot: Arc<PluginSlot>,
    mut rx: mpsc::Receiver<WorkerEvent>,
    context: WorkerContext,
    timeout: Duration,
    stop: impl std::future::Future<Output = ()>,
) {
    tokio::pin!(stop);
    loop {
        let event = tokio::select! {
            biased;
            _ = &mut stop => return,
            event = rx.recv() => match event { Some(e) => e, None => return },
        };
        // Race the in-flight callback against `stop`: dropping the `call` future aborts
        // the callback task (AbortOnDrop), so a wedged callback cannot outlive shutdown.
        let outcome = tokio::select! {
            biased;
            _ = &mut stop => return,
            outcome = call(
                slot.plugin.clone(),
                context.clone(),
                Hook::Event(event),
                timeout,
            ) => outcome,
        };
        if matches!(
            outcome,
            Outcome::Panicked | Outcome::TimedOut | Outcome::Cancelled
        ) {
            // Sticky; never retried. Dropping `rx` makes further dispatch a no-op.
            slot.mark_unhealthy(outcome.reason());
            return;
        }
        if rx.is_empty() {
            let _ =
                slot.status
                    .compare_exchange(LAGGED, HEALTHY, Ordering::AcqRel, Ordering::Acquire);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Noop;
    impl WorkerPlugin for Noop {
        fn name(&self) -> &'static str {
            "noop"
        }
        fn on_start(&self, _: WorkerContext) -> PluginFuture<'_> {
            Box::pin(async { Ok(()) })
        }
        fn on_event(&self, _: WorkerContext, _: WorkerEvent) -> PluginFuture<'_> {
            Box::pin(async { Ok(()) })
        }
        fn on_shutdown(&self, _: WorkerContext) -> PluginFuture<'_> {
            Box::pin(async { Ok(()) })
        }
    }

    fn slot() -> (PluginSlot, mpsc::Receiver<WorkerEvent>) {
        let (tx, rx) = mpsc::channel(1);
        let slot = PluginSlot {
            name: "noop",
            required: false,
            plugin: Arc::new(Noop),
            tx,
            status: AtomicU8::new(HEALTHY),
            dropped: AtomicU64::new(0),
        };
        (slot, rx)
    }

    #[test]
    fn full_queue_marks_lagged() {
        let (slot, _rx) = slot();
        slot.tx.try_send(WorkerEvent::Ready).unwrap();
        assert!(slot.tx.try_send(WorkerEvent::Ready).is_err());
        slot.note_dropped();
        let health = slot.health();
        assert_eq!(health.status, PluginStatus::Lagged);
        assert_eq!(health.dropped_events, 1);
    }

    /// The supervisor drained the queue between `try_send` returning `Full` and the
    /// Lagged mark: the slot must not stay Lagged on an idle, empty queue.
    #[test]
    fn drop_noted_after_drain_does_not_stick_lagged() {
        let (slot, mut rx) = slot();
        slot.tx.try_send(WorkerEvent::Ready).unwrap();
        assert!(slot.tx.try_send(WorkerEvent::Ready).is_err());
        rx.try_recv().unwrap();
        slot.note_dropped();
        let health = slot.health();
        assert_eq!(health.status, PluginStatus::Healthy);
        assert_eq!(health.dropped_events, 1);
    }
}
