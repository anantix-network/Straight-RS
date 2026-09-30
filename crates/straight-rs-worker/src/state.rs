use std::collections::HashMap;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use straight_rs::{ChannelId, GuildId};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerStatus {
    pub gateway_ready: bool,
    pub lavalink_ready: bool,
    pub ready: bool,
    pub degraded: bool,
    pub lagged_events: u64,
}

#[derive(Clone, Default)]
pub struct VoiceStateStore(Arc<RwLock<HashMap<GuildId, ChannelId>>>);
impl VoiceStateStore {
    pub fn update(&self, guild: GuildId, channel: Option<ChannelId>) {
        let mut states = self.0.write().expect("voice state lock poisoned");
        if let Some(channel) = channel {
            states.insert(guild, channel);
        } else {
            states.remove(&guild);
        }
    }
    pub fn channel(&self, guild: GuildId) -> Option<ChannelId> {
        self.0
            .read()
            .expect("voice state lock poisoned")
            .get(&guild)
            .copied()
    }
}

#[derive(Clone)]
pub(crate) struct StatusState {
    pub gateway_ready: Arc<AtomicBool>,
    pub degraded: Arc<AtomicBool>,
    pub lagged: Arc<AtomicU64>,
    pub lavalink: straight_rs::LavalinkClient,
}
impl StatusState {
    pub fn snapshot(&self) -> WorkerStatus {
        let gateway_ready = self.gateway_ready.load(Ordering::Acquire);
        let lavalink_ready = self.lavalink.nodes().iter().any(|node| node.is_ready());
        let degraded = self.degraded.load(Ordering::Acquire);
        WorkerStatus {
            gateway_ready,
            lavalink_ready,
            ready: gateway_ready && lavalink_ready && !degraded,
            degraded,
            lagged_events: self.lagged.load(Ordering::Relaxed),
        }
    }
}
