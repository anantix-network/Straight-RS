use crate::backoff::Backoff;
use crate::balancer::penalty;
use crate::hub::Hub;
use crate::rest::RestClient;
use crate::{Error, Event, NodeConfig, Result};
use arc_swap::ArcSwapOption;
use futures_util::{SinkExt, StreamExt};
use larplink_model::{Info, RoutePlannerStatus, SessionUpdate, Stats, WsMessage};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering::*};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::time::Instant;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    Connecting,
    Ready,
    Disconnected,
}

pub struct Node {
    pub(crate) index: usize,
    pub(crate) cfg: NodeConfig,
    rest: RestClient,
    session_id: ArcSwapOption<String>,
    status: AtomicU8,
    penalty: AtomicU32,
    players: AtomicU32,
    stats: ArcSwapOption<Stats>,
    epoch: AtomicU64,
    /// Bumped whenever the node hands out a session that does not carry our
    /// players (`resumed = false`); players remember the generation they were
    /// last written on.
    session_gen: AtomicU64,
    info: OnceCell<Info>,
    version: OnceCell<String>,
}

fn header(v: &str) -> Result<HeaderValue> {
    HeaderValue::from_str(v).map_err(|e| Error::Config(format!("invalid header value: {e}")))
}

impl Node {
    pub(crate) fn new(index: usize, cfg: NodeConfig, client_name: &str) -> Arc<Self> {
        Arc::new(Self {
            index,
            rest: RestClient::new(&cfg, client_name),
            cfg,
            session_id: ArcSwapOption::empty(),
            status: AtomicU8::new(NodeStatus::Connecting as u8),
            penalty: AtomicU32::new(0),
            players: AtomicU32::new(0),
            stats: ArcSwapOption::empty(),
            epoch: AtomicU64::new(0),
            session_gen: AtomicU64::new(0),
            info: OnceCell::new(),
            version: OnceCell::new(),
        })
    }

    pub fn index(&self) -> usize {
        self.index
    }
    pub fn status(&self) -> NodeStatus {
        match self.status.load(Acquire) {
            0 => NodeStatus::Connecting,
            1 => NodeStatus::Ready,
            _ => NodeStatus::Disconnected,
        }
    }
    fn set_status(&self, s: NodeStatus) {
        self.status.store(
            match s {
                NodeStatus::Connecting => 0,
                NodeStatus::Ready => 1,
                NodeStatus::Disconnected => 2,
            },
            Release,
        );
    }
    pub fn is_ready(&self) -> bool {
        self.status() == NodeStatus::Ready
    }
    pub fn penalty(&self) -> u32 {
        self.penalty.load(Relaxed)
    }
    pub fn players(&self) -> u32 {
        self.players.load(Relaxed)
    }
    pub fn stats(&self) -> Option<Arc<Stats>> {
        self.stats.load_full()
    }
    pub fn session_id(&self) -> Option<Arc<String>> {
        self.session_id.load_full()
    }
    pub fn rest(&self) -> &RestClient {
        &self.rest
    }
    pub(crate) fn bump_epoch(&self) -> u64 {
        self.epoch.fetch_add(1, AcqRel) + 1
    }
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch.load(Acquire)
    }
    pub(crate) fn session_gen(&self) -> u64 {
        self.session_gen.load(Acquire)
    }

    pub async fn info(&self) -> Result<&Info> {
        self.info.get_or_try_init(|| self.rest.info()).await
    }
    pub async fn version(&self) -> Result<&str> {
        self.version
            .get_or_try_init(|| self.rest.version())
            .await
            .map(String::as_str)
    }
    pub async fn route_planner_status(&self) -> Result<RoutePlannerStatus> {
        self.rest.route_planner_status().await
    }
    pub async fn free_address(&self, address: &str) -> Result<()> {
        self.rest.free_address(address).await
    }
    pub async fn free_all(&self) -> Result<()> {
        self.rest.free_all().await
    }

    /// Client closed: forget the session and report disconnected for good.
    pub(crate) fn shut_down(&self) {
        self.set_status(NodeStatus::Disconnected);
        self.session_id.store(None);
    }

    /// Connection loop: connect, read, reconnect with backoff, until shutdown.
    pub(crate) async fn run(self: Arc<Self>, hub: Arc<Hub>) {
        self.run_until_closed(&hub).await;
        self.shut_down();
    }

    async fn run_until_closed(self: &Arc<Self>, hub: &Arc<Hub>) {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        loop {
            if hub.is_closed() {
                return;
            }
            self.set_status(NodeStatus::Connecting);
            let outcome = tokio::select! {
                r = self.session(hub, &mut backoff) => r,
                _ = hub.closed() => return,
            };
            let was_ready = self.is_ready();
            self.set_status(NodeStatus::Disconnected);
            if was_ready {
                hub.node_down(self);
            }
            match outcome {
                Ok(()) => tracing::info!(node = self.index, "lavalink socket closed"),
                Err(e) => tracing::warn!(node = self.index, error = %e, "lavalink socket error"),
            }
            let delay = backoff.next_delay(rand::random::<f64>());
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = hub.closed() => return,
            }
        }
    }

    async fn session(self: &Arc<Self>, hub: &Arc<Hub>, backoff: &mut Backoff) -> Result<()> {
        let scheme = if self.cfg.secure { "wss" } else { "ws" };
        let mut req = format!("{scheme}://{}/v4/websocket", self.cfg.host).into_client_request()?;
        let h = req.headers_mut();
        h.insert("Authorization", header(&self.cfg.password)?);
        h.insert("User-Id", header(&hub.user_id.to_string())?);
        h.insert("Client-Name", header(&hub.client_name)?);
        if let Some(sid) = self.session_id() {
            h.insert("Session-Id", header(&sid)?);
        }
        let (ws, _) = tokio::time::timeout(self.cfg.request_timeout, connect_async(req))
            .await
            .map_err(|_| Error::Timeout)??;
        let (mut write, mut read) = ws.split();
        let mut ping = tokio::time::interval_at(
            Instant::now() + self.cfg.ping_interval,
            self.cfg.ping_interval,
        );
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_rx = Instant::now();
        loop {
            tokio::select! {
                frame = read.next() => match frame {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(Message::Text(t))) => {
                        last_rx = Instant::now();
                        self.on_text(hub, backoff, &t);
                    }
                    Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Ok(_)) => last_rx = Instant::now(),
                },
                _ = ping.tick() => {
                    if last_rx.elapsed() > self.cfg.ping_timeout {
                        return Err(Error::Timeout);
                    }
                    write.send(Message::Ping(Vec::new())).await?;
                }
            }
        }
    }

    fn on_text(self: &Arc<Self>, hub: &Arc<Hub>, backoff: &mut Backoff, text: &str) {
        match WsMessage::parse(text) {
            Ok(WsMessage::Ready(_)) if hub.is_closed() => {}
            Ok(WsMessage::Ready(r)) => {
                let same = self.session_id().is_some_and(|s| *s == r.session_id);
                self.session_id.store(Some(Arc::new(r.session_id.clone())));
                if !r.resumed || !same {
                    // After the new id is visible and before Ready: a writer
                    // that sees Ready sees the new generation.
                    self.session_gen.fetch_add(1, AcqRel);
                }
                self.set_status(NodeStatus::Ready);
                backoff.reset();
                self.enable_resume(hub, r.session_id.clone());
                hub.emit(Event::Ready {
                    node: self.index,
                    resumed: r.resumed,
                    session_id: r.session_id.into(),
                });
                hub.node_ready(self, r.resumed);
            }
            Ok(WsMessage::Stats(s)) => {
                self.penalty.store(penalty(&s), Relaxed);
                self.players.store(s.players, Relaxed);
                let s = Arc::new(s);
                self.stats.store(Some(s.clone()));
                hub.emit(Event::Stats {
                    node: self.index,
                    stats: s,
                });
            }
            Ok(msg) => hub.on_message(self, msg),
            Err(e) => tracing::warn!(node = self.index, error = %e, "ignoring unparseable frame"),
        }
    }

    fn enable_resume(self: &Arc<Self>, hub: &Arc<Hub>, session: String) {
        let node = self.clone();
        let hub = hub.clone();
        tokio::spawn(async move {
            hub.unless_closed(async {
                let upd = SessionUpdate {
                    resuming: Some(true),
                    timeout: Some(node.cfg.resume_timeout_secs),
                };
                if let Err(e) = node.rest.update_session(&session, &upd).await {
                    tracing::warn!(node = node.index, error = %e, "failed to enable session resuming");
                }
            })
            .await;
        });
    }
}
