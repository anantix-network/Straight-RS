use crate::balancer::Strategy;
use crate::hub::Hub;
use crate::node::Node;
use crate::{Error, Event, NodeConfig, Result, VoiceGateway};
use dashmap::DashMap;
use larplink_model::{LoadResult, Track, UserId};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch, Notify};

const DEFAULT_CLIENT_NAME: &str = concat!("larplink/", env!("CARGO_PKG_VERSION"));

struct ShutdownGuard(watch::Sender<bool>);
impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

/// Handle to the node pool. Cheap to clone. Dropping the last clone stops all node tasks.
#[derive(Clone)]
pub struct LavalinkClient {
    pub(crate) hub: Arc<Hub>,
    guard: Arc<ShutdownGuard>,
}

pub struct ClientBuilder {
    user_id: UserId,
    nodes: Vec<NodeConfig>,
    strategy: Strategy,
    client_name: String,
    event_capacity: usize,
    gateway: Option<Arc<dyn VoiceGateway>>,
}

impl ClientBuilder {
    pub fn node(mut self, cfg: NodeConfig) -> Self { self.nodes.push(cfg); self }
    pub fn strategy(mut self, s: Strategy) -> Self { self.strategy = s; self }
    pub fn client_name(mut self, n: impl Into<String>) -> Self { self.client_name = n.into(); self }
    pub fn event_capacity(mut self, n: usize) -> Self { self.event_capacity = n.max(1); self }
    pub fn gateway(mut self, g: Arc<dyn VoiceGateway>) -> Self { self.gateway = Some(g); self }

    /// Spawns one task per node; does not wait for connections (see `wait_ready`).
    pub async fn build(self) -> Result<LavalinkClient> {
        if self.nodes.is_empty() {
            return Err(Error::Config("at least one node is required".into()));
        }
        #[cfg(feature = "tls")]
        {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }
        let nodes: Vec<Arc<Node>> = self
            .nodes
            .into_iter()
            .enumerate()
            .map(|(i, cfg)| Node::new(i, cfg, &self.client_name))
            .collect();
        let (events, _) = broadcast::channel(self.event_capacity);
        let hub = Arc::new(Hub {
            user_id: self.user_id,
            client_name: self.client_name,
            nodes,
            players: DashMap::new(),
            events,
            strategy: self.strategy,
            rr: AtomicUsize::new(0),
            ready: Notify::new(),
            gateway: self.gateway,
        });
        let (tx, rx) = watch::channel(false);
        for node in &hub.nodes {
            tokio::spawn(node.clone().run(hub.clone(), rx.clone()));
        }
        Ok(LavalinkClient { hub, guard: Arc::new(ShutdownGuard(tx)) })
    }
}

impl LavalinkClient {
    pub fn builder(user_id: impl Into<UserId>) -> ClientBuilder {
        ClientBuilder {
            user_id: user_id.into(),
            nodes: Vec::new(),
            strategy: Strategy::default(),
            client_name: DEFAULT_CLIENT_NAME.to_owned(),
            event_capacity: 1024,
            gateway: None,
        }
    }

    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.hub.events.subscribe()
    }

    pub fn nodes(&self) -> &[Arc<Node>] {
        &self.hub.nodes
    }

    /// Waits until at least one node is ready.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.hub.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.hub.nodes.iter().any(|n| n.is_ready()) {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(Error::Timeout);
            }
        }
    }

    pub async fn load(&self, identifier: &str) -> Result<LoadResult> {
        self.hub.pick_node(None).ok_or(Error::NoNode)?.rest().load_tracks(identifier).await
    }

    pub async fn decode_track(&self, encoded: &str) -> Result<Track> {
        self.hub.pick_node(None).ok_or(Error::NoNode)?.rest().decode_track(encoded).await
    }

    pub async fn decode_tracks(&self, encoded: &[String]) -> Result<Vec<Track>> {
        self.hub.pick_node(None).ok_or(Error::NoNode)?.rest().decode_tracks(encoded).await
    }

    /// Stops all node tasks immediately.
    pub fn shutdown(&self) {
        let _ = self.guard.0.send(true);
    }
}
