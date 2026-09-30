//! Example plugin: a per-guild "play next" queue.
//!
//! The queue is ephemeral (in-memory, lost on restart), is deliberately **not** bundled
//! into the worker core, and never stores voice/session credentials: it only keeps the
//! sanitized [`PluginTrack`]s handed to it and reacts to
//! `WorkerEvent::TrackEnd { may_start_next: true, .. }`.
//!
//! Registering it (statically, before the worker is built):
//!
//! ```ignore
//! let queue = QueuePlugin::new();
//! let handle = queue.handle(); // enqueue from your own code via `handle`
//! let worker = WorkerBuilder::new(config, gateway)
//!     .optional_plugin(queue) // or `.plugin(queue)` to make startup depend on it
//!     .build()
//!     .await?;
//! handle.lock().unwrap().enqueue(guild, track);
//! ```
//!
//! This example does not start a worker (that needs real Discord/Lavalink credentials);
//! it only constructs the plugin and prints usage.

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};
use straight_rs::{UpdatePlayer, UpdateTrack};
use straight_rs_model::GuildId;
use straight_rs_worker::{
    PluginError, PluginFuture, PluginTrack, WorkerContext, WorkerEvent, WorkerPlugin,
};

/// Pure queue logic: a FIFO of tracks per guild.
#[derive(Default)]
pub struct QueueState {
    queues: HashMap<GuildId, VecDeque<PluginTrack>>,
}

impl QueueState {
    pub fn enqueue(&mut self, guild: GuildId, track: PluginTrack) {
        self.queues.entry(guild).or_default().push_back(track);
    }

    /// The next track for `guild`, only when the finished track allows starting a new one.
    pub fn next_after_end(&mut self, guild: GuildId, may_start_next: bool) -> Option<PluginTrack> {
        if !may_start_next {
            return None;
        }
        let queue = self.queues.get_mut(&guild)?;
        let next = queue.pop_front();
        if queue.is_empty() {
            self.queues.remove(&guild);
        }
        next
    }
}

pub type SharedQueue = Arc<Mutex<QueueState>>;

pub struct QueuePlugin {
    state: SharedQueue,
}

impl QueuePlugin {
    pub fn new() -> Self {
        Self {
            state: SharedQueue::default(),
        }
    }

    /// Shared handle for enqueueing tracks. Never hold the lock across an `.await`.
    pub fn handle(&self) -> SharedQueue {
        self.state.clone()
    }
}

impl Default for QueuePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerPlugin for QueuePlugin {
    fn name(&self) -> &'static str {
        "queue"
    }

    fn on_start(&self, _context: WorkerContext) -> PluginFuture<'_> {
        Box::pin(async { Ok(()) })
    }

    fn on_event(&self, context: WorkerContext, event: WorkerEvent) -> PluginFuture<'_> {
        Box::pin(async move {
            let WorkerEvent::TrackEnd {
                guild,
                may_start_next,
                ..
            } = event
            else {
                return Ok(());
            };
            // The guard is a temporary dropped before the `.await` below.
            let next = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .next_after_end(guild, may_start_next);
            let Some(track) = next else {
                return Ok(());
            };
            context
                .player(guild)
                .update(UpdatePlayer {
                    track: Some(UpdateTrack {
                        encoded: Some(Some(track.encoded)),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .await
                .map_err(|_| PluginError::new("failed to start next queued track"))
        })
    }

    fn on_shutdown(&self, _context: WorkerContext) -> PluginFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

fn main() {
    let plugin = QueuePlugin::new();
    println!(
        "constructed plugin `{}`; register it with WorkerBuilder::plugin(..) or \
         optional_plugin(..) before build(). See the module docs.",
        plugin.name()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track(encoded: &str) -> PluginTrack {
        PluginTrack {
            encoded: encoded.into(),
            title: "t".into(),
            author: "a".into(),
            length_ms: 1,
            is_stream: false,
            source_name: "s".into(),
        }
    }

    #[test]
    fn ignores_end_that_may_not_start_next() {
        let mut state = QueueState::default();
        state.enqueue(GuildId(1), track("one"));
        assert!(state.next_after_end(GuildId(1), false).is_none());
        // Nothing was consumed.
        assert_eq!(
            &*state.next_after_end(GuildId(1), true).unwrap().encoded,
            "one"
        );
    }

    #[test]
    fn pops_next_for_that_guild_only_in_fifo_order() {
        let mut state = QueueState::default();
        state.enqueue(GuildId(1), track("a1"));
        state.enqueue(GuildId(1), track("a2"));
        state.enqueue(GuildId(2), track("b1"));
        assert_eq!(
            &*state.next_after_end(GuildId(1), true).unwrap().encoded,
            "a1"
        );
        assert_eq!(
            &*state.next_after_end(GuildId(1), true).unwrap().encoded,
            "a2"
        );
        assert_eq!(
            &*state.next_after_end(GuildId(2), true).unwrap().encoded,
            "b1"
        );
    }

    #[test]
    fn empty_or_unknown_queue_yields_none() {
        let mut state = QueueState::default();
        assert!(state.next_after_end(GuildId(1), true).is_none());
        state.enqueue(GuildId(1), track("x"));
        assert!(state.next_after_end(GuildId(2), true).is_none());
        assert!(state.next_after_end(GuildId(1), true).is_some());
        assert!(state.next_after_end(GuildId(1), true).is_none());
    }
}
