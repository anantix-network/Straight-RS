use crate::position::interpolate;
use crate::voice::VoiceAssembler;
use arc_swap::ArcSwap;
use larplink_model::{self as model, Filters, GuildId, PlayerState, Track, UpdatePlayer, UpdateTrack};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

const NO_NODE: usize = usize::MAX;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone, Debug)]
pub struct PlayerSnapshot {
    pub track: Option<Arc<Track>>,
    pub paused: bool,
    pub volume: u16,
    pub filters: Filters,
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
            filters: Filters::default(),
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
        let length = self.track.as_ref().filter(|t| !t.info.is_stream).map(|t| t.info.length);
        interpolate(self.position, playing, self.updated_at.elapsed(), length)
    }
}

pub(crate) struct PlayerInner {
    pub(crate) guild: GuildId,
    node: AtomicUsize,
    pub(crate) orphaned: AtomicBool,
    snapshot: ArcSwap<PlayerSnapshot>,
    pub(crate) voice: Mutex<VoiceAssembler>,
    /// Fair FIFO gate: at most one in-flight write per guild, in call order.
    pub(crate) gate: tokio::sync::Mutex<()>,
}

impl PlayerInner {
    pub(crate) fn new(guild: GuildId) -> Self {
        Self {
            guild,
            node: AtomicUsize::new(NO_NODE),
            orphaned: AtomicBool::new(false),
            snapshot: ArcSwap::from_pointee(PlayerSnapshot::default()),
            voice: Mutex::new(VoiceAssembler::default()),
            gate: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) fn node_index(&self) -> Option<usize> {
        match self.node.load(Ordering::Acquire) {
            NO_NODE => None,
            i => Some(i),
        }
    }

    /// Assign a node if none is assigned yet; returns the node index in effect.
    pub(crate) fn assign_node(&self, idx: usize) -> usize {
        match self.node.compare_exchange(NO_NODE, idx, Ordering::AcqRel, Ordering::Acquire) {
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

    pub(crate) fn apply_player(&self, p: &model::Player) {
        self.snapshot.store(Arc::new(PlayerSnapshot {
            track: p.track.clone().map(Arc::new),
            paused: p.paused,
            volume: p.volume,
            filters: p.filters.clone(),
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
        let voice = lock(&self.voice).current();
        if s.track.is_none() && voice.is_none() {
            return None;
        }
        let mut upd = UpdatePlayer::default();
        if let Some(t) = &s.track {
            upd.track = Some(UpdateTrack { encoded: Some(Some(t.encoded.clone())), ..Default::default() });
            if !t.info.is_stream {
                upd.position = Some(s.position_now());
            }
        }
        upd.paused = Some(s.paused);
        upd.volume = Some(s.volume);
        upd.filters = Some(s.filters.clone());
        upd.voice = voice;
        Some(upd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use larplink_model::VoiceState;
    fn track(enc: &str, len: u64, stream: bool) -> larplink_model::Track {
        serde_json::from_value(serde_json::json!({"encoded":enc,"info":{"identifier":"i","isSeekable":true,"author":"a","length":len,"isStream":stream,"position":0,"title":"t","uri":null,"artworkUrl":null,"isrc":null,"sourceName":"s"}})).unwrap()
    }
    fn model_player(t: Option<larplink_model::Track>, pos: u64, paused: bool) -> larplink_model::Player {
        serde_json::from_value(serde_json::json!({"guildId":"1","track":t,"volume":80,"paused":paused,
          "state":{"time":0,"position":pos,"connected":true,"ping":4},"voice":{"token":"","endpoint":"","sessionId":""},"filters":{"volume":0.5}})).unwrap()
    }
    #[test] fn assign_node_first_wins() {
        let p = PlayerInner::new(GuildId(1));
        assert_eq!(p.node_index(), None);
        assert_eq!(p.assign_node(2), 2);
        assert_eq!(p.assign_node(5), 2);
        p.set_node(5);
        assert_eq!(p.node_index(), Some(5));
    }
    #[test] fn apply_player_and_update() {
        let p = PlayerInner::new(GuildId(1));
        p.apply_player(&model_player(Some(track("A", 10_000, false)), 100, false));
        let s = p.snapshot();
        assert_eq!((s.volume, s.paused, s.position, s.ping), (80, false, 100, 4));
        assert_eq!(s.filters.volume, Some(0.5));
        p.apply_update(&larplink_model::PlayerState { time: 0, position: 500, connected: false, ping: 9 });
        let s = p.snapshot();
        assert_eq!((s.position, s.connected, s.ping), (500, false, 9));
        assert!(s.track.is_some(), "playerUpdate must not touch the track");
    }
    #[test] fn position_now_respects_pause_disconnect_and_no_track() {
        let p = PlayerInner::new(GuildId(1));
        p.apply_player(&model_player(Some(track("A", 10_000, false)), 1_000, true));
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert_eq!(p.snapshot().position_now(), 1_000);
        p.apply_player(&model_player(None, 0, false));
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert_eq!(p.snapshot().position_now(), 0);
        p.apply_player(&model_player(Some(track("A", 10_000, false)), 1_000, false));
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(p.snapshot().position_now() >= 1_030);
    }
    #[test] fn clear_track_only_when_encoded_matches() {
        let p = PlayerInner::new(GuildId(1));
        p.apply_player(&model_player(Some(track("A", 1, false)), 0, false));
        p.clear_track_if("B");
        assert!(p.snapshot().track.is_some());
        p.clear_track_if("A");
        assert!(p.snapshot().track.is_none());
    }
    #[test] fn restore_payload_contains_track_position_filters_voice() {
        let p = PlayerInner::new(GuildId(1));
        assert!(p.restore_payload().is_none());
        p.apply_player(&model_player(Some(track("A", 100_000, false)), 2_000, true));
        lock(&p.voice).mark_sent(VoiceState { token: "t".into(), endpoint: "e".into(), session_id: "s".into(), channel_id: None });
        let u = p.restore_payload().unwrap();
        assert_eq!(u.track.unwrap().encoded, Some(Some("A".into())));
        assert!(u.position.unwrap() >= 2_000);
        assert_eq!(u.paused, Some(true));
        assert_eq!(u.volume, Some(80));
        assert_eq!(u.voice.unwrap().token, "t");
    }
    #[test] fn restore_payload_skips_position_for_streams() {
        let p = PlayerInner::new(GuildId(1));
        p.apply_player(&model_player(Some(track("S", 0, true)), 2_000, false));
        assert!(p.restore_payload().unwrap().position.is_none());
    }
}
