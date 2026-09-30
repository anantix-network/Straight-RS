use crate::balancer::{pick, NodeView, Strategy};
use crate::node::Node;
use crate::state::PlayerInner;
use crate::{Error, Event, Result, TrackEndReason, VoiceGateway};
use dashmap::DashMap;
use larplink_model::{GuildId, UserId, WsMessage};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::{broadcast, Notify};

pub(crate) struct Hub {
    pub(crate) user_id: UserId,
    pub(crate) client_name: String,
    pub(crate) nodes: Vec<Arc<Node>>,
    pub(crate) players: DashMap<GuildId, Arc<PlayerInner>>,
    pub(crate) events: broadcast::Sender<Event>,
    pub(crate) strategy: Strategy,
    pub(crate) rr: AtomicUsize,
    pub(crate) ready: Notify,
    pub(crate) gateway: Option<Arc<dyn VoiceGateway>>,
}

impl Hub {
    pub(crate) fn emit(&self, ev: Event) {
        // Err only means "no receivers"; that is fine.
        let _ = self.events.send(ev);
    }

    /// Best ready node, optionally excluding one.
    pub(crate) fn pick_node(&self, exclude: Option<usize>) -> Option<Arc<Node>> {
        let views: Vec<NodeView> = self
            .nodes
            .iter()
            .filter(|n| n.is_ready() && Some(n.index) != exclude)
            .map(|n| NodeView { index: n.index, penalty: n.penalty(), players: n.players() })
            .collect();
        pick(&self.strategy, &views, &self.rr).and_then(|i| self.nodes.get(i).cloned())
    }

    /// The node a player should talk to; assigns one on first use.
    pub(crate) fn node_for(&self, p: &PlayerInner) -> Result<Arc<Node>> {
        match p.node_index() {
            Some(i) => {
                let n = self.nodes.get(i).ok_or(Error::NoNode)?;
                if n.is_ready() { Ok(n.clone()) } else { Err(Error::NoNode) }
            }
            None => {
                let n = self.pick_node(None).ok_or(Error::NoNode)?;
                let i = p.assign_node(n.index);
                let n = self.nodes.get(i).ok_or(Error::NoNode)?;
                if n.is_ready() { Ok(n.clone()) } else { Err(Error::NoNode) }
            }
        }
    }

    pub(crate) fn node_ready(self: &Arc<Self>, node: &Arc<Node>, _resumed: bool) {
        node.bump_epoch();
        self.emit(Event::NodeConnected { node: node.index });
        self.ready.notify_waiters();
    }

    pub(crate) fn node_down(self: &Arc<Self>, node: &Arc<Node>) {
        node.bump_epoch();
        self.emit(Event::NodeDisconnected { node: node.index });
    }

    pub(crate) fn on_message(self: &Arc<Self>, node: &Arc<Node>, msg: WsMessage) {
        match msg {
            WsMessage::PlayerUpdate(u) => {
                if let Some(p) = self.players.get(&u.guild_id) {
                    if p.node_index().is_some_and(|i| i != node.index) {
                        return; // stale update from a node the player left
                    }
                    if p.node_index() == Some(node.index) {
                        p.apply_update(&u.state);
                    }
                }
                self.emit(Event::PlayerUpdate { node: node.index, guild: u.guild_id, state: u.state });
            }
            WsMessage::Event(ev) => {
                if let Some(p) = self.players.get(&ev.guild_id()) {
                    if p.node_index().is_some_and(|i| i != node.index) {
                        return; // stale event from a node the player left
                    }
                    if let larplink_model::Event::TrackEnd { track, reason, .. } = &ev {
                        if *reason != TrackEndReason::Replaced {
                            p.clear_track_if(&track.encoded);
                        }
                    }
                }
                self.emit(Event::from_model(node.index, ev));
            }
            WsMessage::Unknown { op, payload } => {
                self.emit(Event::Unknown { node: node.index, op, payload });
            }
            WsMessage::Ready(_) | WsMessage::Stats(_) => {}
        }
    }
}
