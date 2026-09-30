use crate::balancer::{pick, NodeView, Strategy};
use crate::node::Node;
use crate::state::{lock, PlayerInner};
use crate::{Error, Event, Result, TrackEndReason, VoiceGateway};
use dashmap::{DashMap, DashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use straight_rs_model::{GuildId, UserId, WsMessage};
use tokio::sync::{broadcast, watch, Notify};

/// State a player must still be in (checked under its gate) for `restore_to` to act.
#[derive(Clone, Copy)]
pub(crate) enum Expect {
    /// Still orphaned.
    Rescue,
    /// Still assigned to the node being restored and not orphaned.
    Restore,
    /// Still on `from`, which is still down in the same outage `epoch`.
    Migrate { from: usize, epoch: u64 },
}

pub(crate) enum Route {
    /// The player's current node.
    Stay(Arc<Node>),
    /// A different (or re-readied) node the player must be rebuilt on.
    Move(Arc<Node>),
}

pub(crate) enum Restored {
    /// Re-created on the target; carries the node it was assigned to before.
    Moved { from: Option<usize> },
    /// Destroyed, or no longer in the expected state: nothing was sent.
    Skipped,
}

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
    /// Set once by `close`; every entry point refuses work afterwards.
    closed: AtomicBool,
    /// Flips to `true` on close; background tasks select on it.
    shutdown: watch::Sender<bool>,
}

impl Hub {
    pub(crate) fn new(
        user_id: UserId,
        client_name: String,
        nodes: Vec<Arc<Node>>,
        events: broadcast::Sender<Event>,
        strategy: Strategy,
        gateway: Option<Arc<dyn VoiceGateway>>,
    ) -> Self {
        Self {
            user_id,
            client_name,
            nodes,
            players: DashMap::new(),
            stale: DashSet::new(),
            events,
            strategy,
            rr: AtomicUsize::new(0),
            ready: Notify::new(),
            gateway,
            closed: AtomicBool::new(false),
            shutdown: watch::channel(false).0,
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Stops the client: refuses further work, stops background tasks and
    /// marks every node disconnected. Idempotent.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.shutdown.send_replace(true);
        for n in &self.nodes {
            n.shut_down();
        }
    }

    /// Resolves once the client is closed.
    pub(crate) async fn closed(&self) {
        let mut rx = self.shutdown.subscribe();
        // Err only if the sender is gone, which cannot happen while `self` lives.
        let _ = rx.wait_for(|c| *c).await;
    }

    /// Runs `fut` unless (or until) the client closes.
    pub(crate) async fn unless_closed<F: std::future::Future<Output = ()>>(&self, fut: F) {
        if self.is_closed() {
            return;
        }
        tokio::select! {
            _ = self.closed() => {}
            _ = fut => {}
        }
    }
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
            .map(|n| NodeView {
                index: n.index,
                penalty: n.penalty(),
                players: n.players(),
            })
            .collect();
        pick(&self.strategy, &views, &self.rr).and_then(|i| self.nodes.get(i).cloned())
    }

    /// The node a player should talk to; assigns one on first use.
    pub(crate) fn node_for(&self, p: &PlayerInner) -> Result<Arc<Node>> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        match p.node_index() {
            Some(i) => {
                let n = self.nodes.get(i).ok_or(Error::NoNode)?;
                if n.is_ready() {
                    Ok(n.clone())
                } else {
                    Err(Error::NoNode)
                }
            }
            None => {
                let n = self.pick_node(None).ok_or(Error::NoNode)?;
                let i = p.assign_node(n.index);
                let n = self.nodes.get(i).ok_or(Error::NoNode)?;
                if n.is_ready() {
                    Ok(n.clone())
                } else {
                    Err(Error::NoNode)
                }
            }
        }
    }

    /// Where a write for `p` must go (gate held by the caller). A player that
    /// is orphaned or whose node is down moves to the best ready node
    /// (`Route::Move`); the caller completes the move with `adopt` once the
    /// write succeeded.
    pub(crate) fn route(&self, p: &PlayerInner) -> Result<Route> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        match p.node_index() {
            Some(i) => {
                let healthy = !p.orphaned.load(Ordering::Acquire)
                    && self.nodes.get(i).is_some_and(|n| n.is_ready());
                match self.nodes.get(i) {
                    Some(n) if healthy => Ok(Route::Stay(n.clone())),
                    _ => self.pick_node(None).map(Route::Move).ok_or(Error::NoNode),
                }
            }
            None => self.node_for(p).map(Route::Stay),
        }
    }

    /// Finish moving `p` (gate held) to `node`, where it now exists.
    pub(crate) fn adopt(self: &Arc<Self>, p: &PlayerInner, node: &Node) {
        let from = p.node_index();
        p.set_node(node.index);
        p.orphaned.store(false, Ordering::Release);
        if let Some(from) = from.filter(|f| *f != node.index) {
            self.mark_stale(from, p.guild);
            self.emit(Event::PlayerMigrated {
                guild: p.guild,
                from,
                to: node.index,
            });
        }
    }

    pub(crate) fn node_ready(self: &Arc<Self>, node: &Arc<Node>, resumed: bool) {
        node.bump_epoch(); // cancels a pending failover timer
        if self.is_closed() {
            return;
        }
        self.emit(Event::NodeConnected { node: node.index });
        self.ready.notify_waiters();
        let hub = self.clone();
        let node = node.clone();
        tokio::spawn(async move {
            hub.unless_closed(async {
                hub.cleanup_stale(&node, resumed).await;
                if !resumed {
                    hub.restore_players(&node).await;
                }
                hub.rescue_orphans(&node).await;
            })
            .await;
        });
    }

    pub(crate) fn node_down(self: &Arc<Self>, node: &Arc<Node>) {
        let epoch = node.bump_epoch();
        if self.is_closed() {
            return;
        }
        self.emit(Event::NodeDisconnected { node: node.index });
        let hub = self.clone();
        let node = node.clone();
        tokio::spawn(async move {
            hub.unless_closed(async {
                tokio::time::sleep(node.cfg.failover_grace).await;
                if node.epoch() == epoch && !node.is_ready() {
                    hub.migrate_from(node.index, epoch).await;
                }
            })
            .await;
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
                self.emit(Event::PlayerUpdate {
                    node: node.index,
                    guild: u.guild_id,
                    state: u.state,
                });
            }
            WsMessage::Event(ev) => {
                if let Some(p) = self.players.get(&ev.guild_id()) {
                    if p.node_index().is_some_and(|i| i != node.index) {
                        return; // stale event from a node the player left
                    }
                    if let straight_rs_model::Event::TrackEnd { track, reason, .. } = &ev {
                        if *reason != TrackEndReason::Replaced {
                            p.clear_track_if(&track.encoded);
                        }
                    }
                }
                self.emit(Event::from_model(node.index, ev));
            }
            WsMessage::Unknown { op, payload } => {
                self.emit(Event::Unknown {
                    node: node.index,
                    op,
                    payload,
                });
            }
            WsMessage::Ready(_) | WsMessage::Stats(_) => {}
        }
    }

    /// Re-create `p` on `node` from client-side state, provided `expect` still
    /// holds once the player's gate is taken.
    pub(crate) async fn restore_to(
        &self,
        p: &Arc<PlayerInner>,
        node: &Arc<Node>,
        expect: Expect,
    ) -> Result<Restored> {
        let _gate = p.gate.lock().await;
        if self.is_closed() || !self.expected(p, node.index, expect) {
            return Ok(Restored::Skipped);
        }
        let from = p.node_index();
        let gen = node.session_gen();
        // Skip the PATCH when this session already has the player (a user
        // write rebuilt it first, or the session survived).
        if !p.written_on(node.index, gen) {
            if let Some(upd) = p.restore_payload() {
                let sid = node.session_id().ok_or(Error::NoNode)?;
                let resp = node
                    .rest()
                    .update_player(&sid, p.guild, &upd, false)
                    .await?;
                p.apply_player(resp);
                p.mark_written(node.index, gen);
                if let Some(vs) = upd.voice {
                    lock(&p.voice).mark_sent(vs);
                }
            }
        }
        p.set_node(node.index);
        p.orphaned.store(false, Ordering::Release);
        Ok(Restored::Moved { from })
    }

    /// Whether `p` (gate held by the caller) is still eligible for the flow
    /// described by `expect`. Destroyed players never are.
    fn expected(&self, p: &PlayerInner, target: usize, expect: Expect) -> bool {
        if p.destroyed.load(Ordering::Acquire) {
            return false; // never resurrect a destroyed player
        }
        let orphaned = p.orphaned.load(Ordering::Acquire);
        match expect {
            Expect::Rescue => orphaned,
            Expect::Restore => !orphaned && p.node_index() == Some(target),
            Expect::Migrate { from, epoch } => {
                p.node_index() == Some(from)
                    && self
                        .nodes
                        .get(from)
                        .is_some_and(|n| n.epoch() == epoch && !n.is_ready())
            }
        }
    }

    /// Mark `p` orphaned, unless the migration precondition stopped holding.
    async fn orphan(&self, p: &PlayerInner, from: usize, epoch: u64) {
        let _gate = p.gate.lock().await;
        if self.expected(p, from, Expect::Migrate { from, epoch }) {
            p.orphaned.store(true, Ordering::Release);
        }
    }

    fn players_where(&self, f: impl Fn(&PlayerInner) -> bool) -> Vec<Arc<PlayerInner>> {
        self.players
            .iter()
            .filter(|e| f(e.value()))
            .map(|e| e.value().clone())
            .collect()
    }

    /// Server lost our session: put every player of `node` back.
    pub(crate) async fn restore_players(&self, node: &Arc<Node>) {
        if self.is_closed() {
            return;
        }
        let idx = node.index;
        for p in self
            .players_where(|p| p.node_index() == Some(idx) && !p.orphaned.load(Ordering::Acquire))
        {
            if let Err(e) = self.restore_to(&p, node, Expect::Restore).await {
                tracing::warn!(guild = %p.guild, error = %e, "failed to restore player after session loss");
            }
        }
    }

    /// Node stayed down past the grace period: move its players elsewhere.
    pub(crate) async fn migrate_from(self: &Arc<Self>, idx: usize, epoch: u64) {
        for p in self.players_where(|p| p.node_index() == Some(idx)) {
            if self.is_closed() {
                return;
            }
            match self.nodes.get(idx) {
                Some(n) if n.epoch() == epoch && !n.is_ready() => {}
                _ => return, // node came back (or flapped): stop moving players
            }
            let Some(target) = self.pick_node(Some(idx)) else {
                self.orphan(&p, idx, epoch).await;
                continue;
            };
            match self
                .restore_to(&p, &target, Expect::Migrate { from: idx, epoch })
                .await
            {
                Ok(Restored::Skipped) => {}
                Ok(Restored::Moved { .. }) => {
                    self.mark_stale(idx, p.guild);
                    self.emit(Event::PlayerMigrated {
                        guild: p.guild,
                        from: idx,
                        to: target.index,
                    });
                }
                Err(e) => {
                    tracing::warn!(guild = %p.guild, error = %e, "player migration failed");
                    self.orphan(&p, idx, epoch).await;
                }
            }
        }
    }

    /// A node became ready: adopt players that had nowhere to go.
    pub(crate) async fn rescue_orphans(self: &Arc<Self>, node: &Arc<Node>) {
        for p in self.players_where(|p| p.orphaned.load(Ordering::Acquire)) {
            if self.is_closed() {
                return;
            }
            match self.restore_to(&p, node, Expect::Rescue).await {
                Ok(Restored::Skipped) => {}
                Ok(Restored::Moved { from }) => {
                    if let Some(from) = from.filter(|f| *f != node.index) {
                        self.mark_stale(from, p.guild);
                        self.emit(Event::PlayerMigrated {
                            guild: p.guild,
                            from,
                            to: node.index,
                        });
                    }
                }
                Err(e) => tracing::warn!(guild = %p.guild, error = %e, "orphan rescue failed"),
            }
        }
    }

    /// `guild` may still exist on node `idx` (moved away, or destroyed while
    /// that node was unreachable): delete it there once the node is back with
    /// the same session. If the node is already ready (it came back while the
    /// move was in flight), clean up right away.
    pub(crate) fn mark_stale(self: &Arc<Self>, idx: usize, guild: GuildId) {
        self.stale.insert((idx, guild));
        if !self.nodes.get(idx).is_some_and(|n| n.is_ready()) {
            return; // cleanup_stale runs when it returns
        }
        let hub = self.clone();
        tokio::spawn(async move {
            hub.unless_closed(async {
                // Whoever removes the pair does the cleanup (no double DELETE).
                if hub.stale.remove(&(idx, guild)).is_some() {
                    if let Some(node) = hub.nodes.get(idx) {
                        hub.cleanup_one(node, guild).await;
                    }
                }
            })
            .await;
        });
    }

    /// An old node is back: remove players we migrated away from it.
    pub(crate) async fn cleanup_stale(&self, node: &Arc<Node>, resumed: bool) {
        let mine: Vec<GuildId> = self
            .stale
            .iter()
            .filter(|e| e.0 == node.index)
            .map(|e| e.1)
            .collect();
        for guild in mine {
            if self.stale.remove(&(node.index, guild)).is_none() {
                continue; // someone else is cleaning it up
            }
            if !resumed {
                continue; // the server forgot the player already
            }
            self.cleanup_one(node, guild).await;
        }
    }

    /// DELETE `guild` on `node` unless the guild's player lives there now.
    /// Checked and sent under that player's gate, so it cannot interleave with
    /// a write (or a move back to `node`).
    async fn cleanup_one(&self, node: &Arc<Node>, guild: GuildId) {
        for _ in 0..8 {
            let p = self.players.get(&guild).map(|e| e.value().clone());
            let _gate = match &p {
                Some(p) => Some(p.gate.lock().await),
                None => None,
            };
            let now = self.players.get(&guild).map(|e| e.value().clone());
            let unchanged = match (&p, &now) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            };
            if !unchanged {
                continue; // the guild got a new player while we waited: recheck
            }
            let back_here = p.as_ref().is_some_and(|p| {
                !p.destroyed.load(Ordering::Acquire) && p.node_index() == Some(node.index)
            });
            if back_here || self.is_closed() {
                return;
            }
            match node.session_id() {
                Some(sid) if node.is_ready() => {
                    if let Err(e) = node.rest().destroy_player(&sid, guild).await {
                        tracing::debug!(guild = %guild, error = %e, "stale player cleanup failed");
                    }
                }
                // Down again: retry when it comes back.
                _ => {
                    self.stale.insert((node.index, guild));
                }
            }
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use straight_rs_model::VoiceState;

    fn hub() -> Arc<Hub> {
        let nodes = (0..2)
            .map(|i| Node::new(i, NodeConfig::new("127.0.0.1:1", "pw"), "test"))
            .collect();
        Arc::new(Hub::new(
            UserId(1),
            "test".into(),
            nodes,
            broadcast::channel(16).0,
            Strategy::default(),
            None,
        ))
    }

    /// Give `p` a restore payload. The test nodes have no session, so any
    /// restore that got past its precondition would fail with `NoNode`.
    fn with_payload(p: &PlayerInner) {
        let vs = VoiceState {
            token: "t".into(),
            endpoint: "e".into(),
            session_id: "s".into(),
            channel_id: None,
        };
        lock(&p.voice).set(&vs);
    }

    #[tokio::test]
    async fn second_rescue_of_the_same_orphan_is_skipped() {
        let h = hub();
        let p = Arc::new(PlayerInner::new(GuildId(1)));
        p.set_node(0);
        p.orphaned.store(true, Ordering::Release);
        // First rescue (no payload -> no REST call) adopts it on node 1.
        let r = h.restore_to(&p, &h.nodes[1], Expect::Rescue).await;
        assert!(matches!(r, Ok(Restored::Moved { from: Some(0) })));
        assert_eq!(p.node_index(), Some(1));
        // A concurrent rescue on node 0 that snapshotted the orphan earlier.
        with_payload(&p);
        let r = h.restore_to(&p, &h.nodes[0], Expect::Rescue).await;
        assert!(matches!(r, Ok(Restored::Skipped)));
        assert_eq!(p.node_index(), Some(1));
    }

    #[tokio::test]
    async fn restore_and_migrate_recheck_assignment_and_epoch() {
        let h = hub();
        let p = Arc::new(PlayerInner::new(GuildId(1)));
        with_payload(&p);
        p.set_node(1); // already migrated away from node 0
        let r = h.restore_to(&p, &h.nodes[0], Expect::Restore).await;
        assert!(matches!(r, Ok(Restored::Skipped)));
        let r = h
            .restore_to(
                &p,
                &h.nodes[0],
                Expect::Migrate {
                    from: 0,
                    epoch: h.nodes[0].epoch(),
                },
            )
            .await;
        assert!(matches!(r, Ok(Restored::Skipped)));
        p.set_node(0);
        let stale_epoch = h.nodes[0].epoch();
        h.nodes[0].bump_epoch(); // node 0 flapped: a newer outage
        let r = h
            .restore_to(
                &p,
                &h.nodes[1],
                Expect::Migrate {
                    from: 0,
                    epoch: stale_epoch,
                },
            )
            .await;
        assert!(matches!(r, Ok(Restored::Skipped)));
        // Precondition holds -> it gets as far as the REST call (no session here).
        let r = h
            .restore_to(
                &p,
                &h.nodes[1],
                Expect::Migrate {
                    from: 0,
                    epoch: h.nodes[0].epoch(),
                },
            )
            .await;
        assert!(matches!(r, Err(Error::NoNode)));
    }

    #[tokio::test]
    async fn destroyed_player_is_always_skipped() {
        let h = hub();
        let p = Arc::new(PlayerInner::new(GuildId(1)));
        with_payload(&p);
        p.set_node(0);
        p.orphaned.store(true, Ordering::Release);
        p.destroyed.store(true, Ordering::Release);
        let r = h.restore_to(&p, &h.nodes[1], Expect::Rescue).await;
        assert!(matches!(r, Ok(Restored::Skipped)));
        assert_eq!(p.node_index(), Some(0));
    }
}
