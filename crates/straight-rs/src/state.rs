use crate::position::interpolate;
use crate::voice::VoiceAssembler;
use arc_swap::{ArcSwap, Guard};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use straight_rs_model::{
    self as model, Filters, GuildId, PlayerState, Track, UpdatePlayer, UpdateTrack,
};

const NO_NODE: usize = usize::MAX;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone, Debug)]
pub struct PlayerSnapshot {
    pub track: Option<Arc<Track>>,
    pub paused: bool,
    pub volume: u16,
    /// Shared: cloning a snapshot (every `playerUpdate`) does not copy filters.
    pub filters: Arc<Filters>,
    /// Position in ms as last reported by the node (see `position_now`).
    pub position: u64,
    pub connected: bool,
    pub ping: i64,
    pub updated_at: Instant,
}

impl Default for PlayerSnapshot {
    fn default() -> Self {
        Self {
            track: None,
            paused: false,
            volume: 100,
            filters: Arc::default(),
            position: 0,
            connected: false,
            ping: -1,
            updated_at: Instant::now(),
        }
    }
}

impl PlayerSnapshot {
    /// Interpolated current position in ms.
    pub fn position_now(&self) -> u64 {
        let playing = self.track.is_some() && self.connected && !self.paused;
        let length = self
            .track
            .as_ref()
            .filter(|t| !t.info.is_stream)
            .map(|t| t.info.length);
        interpolate(self.position, playing, self.updated_at.elapsed(), length)
    }
}

pub(crate) struct PlayerInner {
    pub(crate) guild: GuildId,
    node: AtomicUsize,
    pub(crate) orphaned: AtomicBool,
    /// Set by `Player::destroy`; further writes fail with `PlayerNotFound`.
    pub(crate) destroyed: AtomicBool,
    snapshot: ArcSwap<PlayerSnapshot>,
    pub(crate) voice: Mutex<VoiceAssembler>,
    /// Fair FIFO gate: at most one in-flight write per guild, in call order.
    pub(crate) gate: tokio::sync::Mutex<()>,
    /// (node, session generation) of the last successful write; only
    /// touched under the gate.
    written: Mutex<Option<(usize, u64)>>,
}

impl PlayerInner {
    pub(crate) fn new(guild: GuildId) -> Self {
        Self {
            guild,
            node: AtomicUsize::new(NO_NODE),
            orphaned: AtomicBool::new(false),
            destroyed: AtomicBool::new(false),
            snapshot: ArcSwap::from_pointee(PlayerSnapshot::default()),
            voice: Mutex::new(VoiceAssembler::default()),
            gate: tokio::sync::Mutex::new(()),
            written: Mutex::new(None),
        }
    }

    /// Whether the node's current session already has this player's state.
    pub(crate) fn written_on(&self, node: usize, generation: u64) -> bool {
        *lock(&self.written) == Some((node, generation))
    }

    pub(crate) fn mark_written(&self, node: usize, generation: u64) {
        *lock(&self.written) = Some((node, generation));
    }

    /// Completes a user write bound for `node`'s session `generation`: if the player
    /// was last written on another node or session (lost session, move), the
    /// restore state is merged in (the caller's fields win), and a voice state
    /// the node does not have yet rides along.
    pub(crate) fn prepare(&self, upd: UpdatePlayer, node: usize, generation: u64) -> UpdatePlayer {
        let written = *lock(&self.written);
        let mut upd = match (written, self.restore_payload()) {
            (Some(w), Some(base)) if w != (node, generation) => merge(upd, base),
            _ => upd,
        };
        if upd.voice.is_none() {
            upd.voice = lock(&self.voice).pending();
        }
        upd
    }

    pub(crate) fn node_index(&self) -> Option<usize> {
        match self.node.load(Ordering::Acquire) {
            NO_NODE => None,
            i => Some(i),
        }
    }

    /// Assign a node if none is assigned yet; returns the node index in effect.
    pub(crate) fn assign_node(&self, idx: usize) -> usize {
        match self
            .node
            .compare_exchange(NO_NODE, idx, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => idx,
            Err(cur) => cur,
        }
    }

    pub(crate) fn set_node(&self, idx: usize) {
        self.node.store(idx, Ordering::Release);
    }

    pub(crate) fn snapshot(&self) -> Arc<PlayerSnapshot> {
        self.snapshot.load_full()
    }

    /// Cheap borrow of the current snapshot for synchronous getters.
    pub(crate) fn load(&self) -> Guard<Arc<PlayerSnapshot>> {
        self.snapshot.load()
    }

    pub(crate) fn apply_update(&self, st: &PlayerState) {
        self.snapshot.rcu(|cur| {
            let mut n = PlayerSnapshot::clone(cur);
            n.position = st.position;
            n.connected = st.connected;
            n.ping = st.ping;
            n.updated_at = Instant::now();
            n
        });
    }

    pub(crate) fn apply_player(&self, p: model::Player) {
        self.snapshot.store(Arc::new(PlayerSnapshot {
            track: p.track.map(Arc::new),
            paused: p.paused,
            volume: p.volume,
            filters: Arc::new(p.filters),
            position: p.state.position,
            connected: p.state.connected,
            ping: p.state.ping,
            updated_at: Instant::now(),
        }));
    }

    pub(crate) fn clear_track_if(&self, encoded: &str) {
        self.snapshot.rcu(|cur| {
            let mut n = PlayerSnapshot::clone(cur);
            if n.track.as_ref().is_some_and(|t| &*t.encoded == encoded) {
                n.track = None;
                n.position = 0;
            }
            n
        });
    }

    /// Everything needed to recreate this player on another node.
    pub(crate) fn restore_payload(&self) -> Option<UpdatePlayer> {
        let s = self.snapshot();
        let voice = lock(&self.voice).latest();
        if s.track.is_none() && voice.is_none() {
            return None;
        }
        let mut upd = UpdatePlayer::default();
        if let Some(t) = &s.track {
            upd.track = Some(UpdateTrack {
                encoded: Some(Some(t.encoded.clone())),
                ..Default::default()
            });
            if !t.info.is_stream {
                upd.position = Some(s.position_now());
            }
        }
        upd.paused = Some(s.paused);
        upd.volume = Some(s.volume);
        upd.filters = Some(Filters::clone(&s.filters));
        upd.voice = voice;
        Some(upd)
    }
}

/// `user` over `base`, field by field. A new track in `user` also drops the
/// old track's position/end time from `base`.
fn merge(user: UpdatePlayer, base: UpdatePlayer) -> UpdatePlayer {
    let (position, end_time) = if user.track.is_some() {
        (user.position, user.end_time)
    } else {
        (
            user.position.or(base.position),
            user.end_time.or(base.end_time),
        )
    };
    UpdatePlayer {
        track: user.track.or(base.track),
        position,
        end_time,
        volume: user.volume.or(base.volume),
        paused: user.paused.or(base.paused),
        filters: user.filters.or(base.filters),
        voice: user.voice.or(base.voice),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use straight_rs_model::VoiceState;
    fn track(enc: &str, len: u64, stream: bool) -> straight_rs_model::Track {
        serde_json::from_value(serde_json::json!({"encoded":enc,"info":{"identifier":"i","isSeekable":true,"author":"a","length":len,"isStream":stream,"position":0,"title":"t","uri":null,"artworkUrl":null,"isrc":null,"sourceName":"s"}})).unwrap()
    }
    fn model_player(
        t: Option<straight_rs_model::Track>,
        pos: u64,
        paused: bool,
    ) -> straight_rs_model::Player {
        serde_json::from_value(serde_json::json!({"guildId":"1","track":t,"volume":80,"paused":paused,
          "state":{"time":0,"position":pos,"connected":true,"ping":4},"voice":{"token":"","endpoint":"","sessionId":""},"filters":{"volume":0.5}})).unwrap()
    }
    #[test]
    fn assign_node_first_wins() {
        let p = PlayerInner::new(GuildId(1));
        assert_eq!(p.node_index(), None);
        assert_eq!(p.assign_node(2), 2);
        assert_eq!(p.assign_node(5), 2);
        p.set_node(5);
        assert_eq!(p.node_index(), Some(5));
    }
    #[test]
    fn apply_player_and_update() {
        let p = PlayerInner::new(GuildId(1));
        p.apply_player(model_player(Some(track("A", 10_000, false)), 100, false));
        let s = p.snapshot();
        assert_eq!(
            (s.volume, s.paused, s.position, s.ping),
            (80, false, 100, 4)
        );
        assert_eq!(s.filters.volume, Some(0.5));
        p.apply_update(&straight_rs_model::PlayerState {
            time: 0,
            position: 500,
            connected: false,
            ping: 9,
        });
        let s = p.snapshot();
        assert_eq!((s.position, s.connected, s.ping), (500, false, 9));
        assert!(s.track.is_some(), "playerUpdate must not touch the track");
    }
    #[test]
    fn position_now_respects_pause_disconnect_and_no_track() {
        let p = PlayerInner::new(GuildId(1));
        p.apply_player(model_player(Some(track("A", 10_000, false)), 1_000, true));
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert_eq!(p.snapshot().position_now(), 1_000);
        p.apply_player(model_player(None, 0, false));
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert_eq!(p.snapshot().position_now(), 0);
        p.apply_player(model_player(Some(track("A", 10_000, false)), 1_000, false));
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(p.snapshot().position_now() >= 1_030);
    }
    #[test]
    fn clear_track_only_when_encoded_matches() {
        let p = PlayerInner::new(GuildId(1));
        p.apply_player(model_player(Some(track("A", 1, false)), 0, false));
        p.clear_track_if("B");
        assert!(p.snapshot().track.is_some());
        p.clear_track_if("A");
        assert!(p.snapshot().track.is_none());
    }
    #[test]
    fn restore_payload_contains_track_position_filters_voice() {
        let p = PlayerInner::new(GuildId(1));
        assert!(p.restore_payload().is_none());
        p.apply_player(model_player(Some(track("A", 100_000, false)), 2_000, true));
        lock(&p.voice).set(&VoiceState {
            token: "t".into(),
            endpoint: "e".into(),
            session_id: "s".into(),
            channel_id: None,
        });
        let u = p.restore_payload().unwrap();
        assert_eq!(u.track.unwrap().encoded, Some(Some("A".into())));
        assert!(u.position.unwrap() >= 2_000);
        assert_eq!(u.paused, Some(true));
        assert_eq!(u.volume, Some(80));
        assert_eq!(u.voice.unwrap().token, "t");
    }
    #[test]
    fn restore_payload_uses_the_latest_voice_not_the_last_sent_one() {
        let p = PlayerInner::new(GuildId(1));
        let mut vs = VoiceState {
            token: "t".into(),
            endpoint: "old".into(),
            session_id: "s".into(),
            channel_id: None,
        };
        lock(&p.voice).set(&vs);
        lock(&p.voice).mark_sent(vs.clone());
        vs.endpoint = "new".into(); // region change that was never delivered
        lock(&p.voice).set(&vs);
        assert_eq!(p.restore_payload().unwrap().voice.unwrap().endpoint, "new");
    }
    #[test]
    fn prepare_merges_restore_state_only_after_a_session_change() {
        let p = PlayerInner::new(GuildId(1));
        p.apply_player(model_player(Some(track("A", 100_000, false)), 2_000, false));
        let vol = UpdatePlayer {
            volume: Some(5),
            ..Default::default()
        };
        // Never written: nothing to restore.
        assert_eq!(p.prepare(vol.clone(), 0, 1), vol);
        p.mark_written(0, 1);
        assert_eq!(p.prepare(vol.clone(), 0, 1), vol);
        // New session on the same node: restore fields, user's volume wins.
        let u = p.prepare(vol.clone(), 0, 2);
        assert_eq!(u.volume, Some(5));
        assert_eq!(u.track.unwrap().encoded, Some(Some("A".into())));
        assert!(u.position.unwrap() >= 2_000);
        assert_eq!(u.paused, Some(false));
        // Moving to another node: a new track drops the old position.
        let play = UpdatePlayer {
            track: Some(UpdateTrack {
                encoded: Some(Some("B".into())),
                ..Default::default()
            }),
            ..Default::default()
        };
        let u = p.prepare(play, 1, 1);
        assert_eq!(u.track.unwrap().encoded, Some(Some("B".into())));
        assert_eq!(u.position, None);
        assert_eq!(u.volume, Some(80));
    }
    #[test]
    fn restore_payload_skips_position_for_streams() {
        let p = PlayerInner::new(GuildId(1));
        p.apply_player(model_player(Some(track("S", 0, true)), 2_000, false));
        assert!(p.restore_payload().unwrap().position.is_none());
    }
}
