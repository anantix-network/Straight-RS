use crate::balancer::Strategy;
use crate::hub::Hub;
use crate::node::Node;
use crate::{Error, Event, NodeConfig, Result, VoiceGateway};
use std::sync::Arc;
use std::time::Duration;
use straight_rs_model::{LoadResult, Track, UserId};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::http::HeaderValue;

const DEFAULT_CLIENT_NAME: &str = concat!("straight-rs/", env!("CARGO_PKG_VERSION"));
/// Upper bound for `ClientBuilder::event_capacity` (the broadcast channel
/// preallocates its ring buffer).
pub const MAX_EVENT_CAPACITY: usize = 1 << 24;

/// Closes the hub when the last `LavalinkClient` clone is dropped.
struct ShutdownGuard(Arc<Hub>);
impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Handle to the node pool. Cheap to clone. Dropping the last clone stops all node tasks.
#[derive(Clone)]
pub struct LavalinkClient {
    pub(crate) hub: Arc<Hub>,
    /// Held only for its `Drop`.
    _guard: Arc<ShutdownGuard>,
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
    pub fn node(mut self, cfg: NodeConfig) -> Self {
        self.nodes.push(cfg);
        self
    }
    pub fn strategy(mut self, s: Strategy) -> Self {
        self.strategy = s;
        self
    }
    pub fn client_name(mut self, n: impl Into<String>) -> Self {
        self.client_name = n.into();
        self
    }
    /// Size of the event ring buffer; must be in `1..=MAX_EVENT_CAPACITY`
    /// (checked by `build`).
    pub fn event_capacity(mut self, n: usize) -> Self {
        self.event_capacity = n;
        self
    }
    pub fn gateway(mut self, g: Arc<dyn VoiceGateway>) -> Self {
        self.gateway = Some(g);
        self
    }

    /// Spawns node tasks and returns an event receiver subscribed before they start.
    pub async fn build_with_events(self) -> Result<(LavalinkClient, broadcast::Receiver<Event>)> {
        if self.nodes.is_empty() {
            return Err(Error::Config("at least one node is required".into()));
        }
        if !(1..=MAX_EVENT_CAPACITY).contains(&self.event_capacity) {
            return Err(Error::Config(format!(
                "event_capacity must be in 1..={MAX_EVENT_CAPACITY}"
            )));
        }
        if HeaderValue::from_str(&self.client_name).is_err() {
            return Err(Error::Config(
                "client_name is not a valid header value".into(),
            ));
        }
        for n in &self.nodes {
            n.validate()?;
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
        let receiver = events.subscribe();
        let hub = Arc::new(Hub::new(
            self.user_id,
            self.client_name,
            nodes,
            events,
            self.strategy,
            self.gateway,
        ));
        for node in &hub.nodes {
            tokio::spawn(node.clone().run(hub.clone()));
        }
        Ok((
            LavalinkClient {
                _guard: Arc::new(ShutdownGuard(hub.clone())),
                hub,
            },
            receiver,
        ))
    }

    /// Spawns one task per node; does not wait for connections (see `wait_ready`).
    pub async fn build(self) -> Result<LavalinkClient> {
        self.build_with_events().await.map(|(client, _)| client)
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
            if self.hub.is_closed() {
                return Err(Error::Closed);
            }
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

    /// A ready node for a one-off REST call.
    fn any_node(&self) -> Result<Arc<Node>> {
        if self.hub.is_closed() {
            return Err(Error::Closed);
        }
        self.hub.pick_node(None).ok_or(Error::NoNode)
    }

    pub async fn load(&self, identifier: &str) -> Result<LoadResult> {
        self.any_node()?.rest().load_tracks(identifier).await
    }

    pub async fn decode_track(&self, encoded: &str) -> Result<Track> {
        self.any_node()?.rest().decode_track(encoded).await
    }

    pub async fn decode_tracks(&self, encoded: &[String]) -> Result<Vec<Track>> {
        self.any_node()?.rest().decode_tracks(encoded).await
    }

    /// Stops the client: closes every node connection, cancels pending
    /// failover/restore work, and makes every `Player` handle (and further
    /// calls on this client) fail with `Error::Closed`. Nodes report
    /// `NodeStatus::Disconnected` from here on. Dropping the last clone of the
    /// client has the same effect. Players are not destroyed on the server;
    /// Lavalink drops them when the session times out.
    pub fn shutdown(&self) {
        self.hub.close();
    }
}
