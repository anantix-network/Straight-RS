use crate::balancer::{pick, NodeView, Strategy};
use crate::node::Node;
use crate::state::PlayerInner;
use crate::{Error, Event, Result, TrackEndReason, VoiceGateway};
use dashmap::{DashMap, DashSet};
use larplink_model::{GuildId, UserId, WsMessage};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Notify};

pub(crate) struct Hub {
    pub(crate) user_id: UserId,
    pub(crate) client_name: String,
    pub(crate) nodes: Vec<Arc<Node>>,
    pub(crate) players: DashMap<GuildId, Arc<PlayerInner>>,
    /// (old node, guild) pairs whose player was moved away and may still exist on the old node.
    pub(crate) stale: DashSet<(usize, GuildId)>,
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

    pub(crate) fn node_ready(self: &Arc<Self>, node: &Arc<Node>, resumed: bool) {
        node.bump_epoch(); // cancels a pending failover timer
        self.emit(Event::NodeConnected { node: node.index });
        self.ready.notify_waiters();
        let hub = self.clone();
        let node = node.clone();
        tokio::spawn(async move {
            hub.cleanup_stale(&node, resumed).await;
            if !resumed {
                hub.restore_players(&node).await;
            }
            hub.rescue_orphans(&node).await;
        });
    }

    pub(crate) fn node_down(self: &Arc<Self>, node: &Arc<Node>) {
        let epoch = node.bump_epoch();
        self.emit(Event::NodeDisconnected { node: node.index });
        let hub = self.clone();
        let node = node.clone();
        tokio::spawn(async move {
            tokio::time::sleep(node.cfg.failover_grace).await;
            if node.epoch() == epoch && !node.is_ready() {
                hub.migrate_from(node.index).await;
            }
        });
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

    /// Re-create `p` on `node` from client-side state.
    pub(crate) async fn restore_to(&self, p: &Arc<PlayerInner>, node: &Arc<Node>) -> Result<()> {
        let _gate = p.gate.lock().await;
        if p.destroyed.load(Ordering::Acquire) {
            return Ok(()); // never resurrect a destroyed player
        }
        if let Some(upd) = p.restore_payload() {
            let sid = node.session_id().ok_or(Error::NoNode)?;
            let resp = node.rest().update_player(&sid, p.guild, &upd, false).await?;
            p.apply_player(&resp);
        }
        p.set_node(node.index);
        p.orphaned.store(false, Ordering::Release);
        Ok(())
    }

    fn players_where(&self, f: impl Fn(&PlayerInner) -> bool) -> Vec<Arc<PlayerInner>> {
        self.players.iter().filter(|e| f(e.value())).map(|e| e.value().clone()).collect()
    }

    /// Server lost our session: put every player of `node` back.
    pub(crate) async fn restore_players(&self, node: &Arc<Node>) {
        let idx = node.index;
        for p in self.players_where(|p| p.node_index() == Some(idx) && !p.orphaned.load(Ordering::Acquire)) {
            if let Err(e) = self.restore_to(&p, node).await {
                tracing::warn!(guild = %p.guild, error = %e, "failed to restore player after session loss");
            }
        }
    }

    /// Node stayed down past the grace period: move its players elsewhere.
    pub(crate) async fn migrate_from(self: &Arc<Self>, idx: usize) {
        for p in self.players_where(|p| p.node_index() == Some(idx)) {
            let Some(target) = self.pick_node(Some(idx)) else {
                p.orphaned.store(true, Ordering::Release);
                continue;
            };
            match self.restore_to(&p, &target).await {
                Ok(()) => {
                    self.stale.insert((idx, p.guild));
                    self.emit(Event::PlayerMigrated { guild: p.guild, from: idx, to: target.index });
                }
                Err(e) => {
                    tracing::warn!(guild = %p.guild, error = %e, "player migration failed");
                    p.orphaned.store(true, Ordering::Release);
                }
            }
        }
    }

    /// A node became ready: adopt players that had nowhere to go.
    pub(crate) async fn rescue_orphans(&self, node: &Arc<Node>) {
        for p in self.players_where(|p| p.orphaned.load(Ordering::Acquire)) {
            let from = p.node_index();
            match self.restore_to(&p, node).await {
                Ok(()) => {
                    if let Some(from) = from.filter(|f| *f != node.index) {
                        self.stale.insert((from, p.guild));
                        self.emit(Event::PlayerMigrated { guild: p.guild, from, to: node.index });
                    }
                }
                Err(e) => tracing::warn!(guild = %p.guild, error = %e, "orphan rescue failed"),
            }
        }
    }

    /// An old node is back: remove players we migrated away from it.
    pub(crate) async fn cleanup_stale(&self, node: &Arc<Node>, resumed: bool) {
        let mine: Vec<GuildId> = self.stale.iter().filter(|e| e.0 == node.index).map(|e| e.1).collect();
        for guild in mine {
            self.stale.remove(&(node.index, guild));
            if !resumed {
                continue; // the server forgot the player already
            }
            let back_here = self.players.get(&guild).is_some_and(|p| p.node_index() == Some(node.index));
            if back_here {
                continue;
            }
            if let Some(sid) = node.session_id() {
                if let Err(e) = node.rest().destroy_player(&sid, guild).await {
                    tracing::debug!(guild = %guild, error = %e, "stale player cleanup failed");
                }
            }
        }
    }
}
