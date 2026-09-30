use crate::TrackEndReason;
use serde_json::Value;
use std::sync::Arc;
use straight_rs_model::{self as model, Exception, GuildId, PlayerState, Stats, Track};

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
    NodeConnected {
        node: usize,
    },
    NodeDisconnected {
        node: usize,
    },
    Ready {
        node: usize,
        resumed: bool,
        session_id: Arc<str>,
    },
    Stats {
        node: usize,
        stats: Arc<Stats>,
    },
    PlayerUpdate {
        node: usize,
        guild: GuildId,
        state: PlayerState,
    },
    TrackStart {
        node: usize,
        guild: GuildId,
        track: Arc<Track>,
    },
    TrackEnd {
        node: usize,
        guild: GuildId,
        track: Arc<Track>,
        reason: TrackEndReason,
    },
    TrackException {
        node: usize,
        guild: GuildId,
        track: Arc<Track>,
        exception: Arc<Exception>,
    },
    TrackStuck {
        node: usize,
        guild: GuildId,
        track: Arc<Track>,
        threshold_ms: u64,
    },
    WebSocketClosed {
        node: usize,
        guild: GuildId,
        code: u16,
        reason: String,
        by_remote: bool,
    },
    PlayerMigrated {
        guild: GuildId,
        from: usize,
        to: usize,
    },
    Unknown {
        node: usize,
        op: String,
        payload: Value,
    },
}

impl Event {
    pub fn guild(&self) -> Option<GuildId> {
        match self {
            Self::PlayerUpdate { guild, .. }
            | Self::TrackStart { guild, .. }
            | Self::TrackEnd { guild, .. }
            | Self::TrackException { guild, .. }
            | Self::TrackStuck { guild, .. }
            | Self::WebSocketClosed { guild, .. }
            | Self::PlayerMigrated { guild, .. } => Some(*guild),
            _ => None,
        }
    }

    /// True for `TrackEnd` events after which a queue may start the next track.
    pub fn may_start_next(&self) -> bool {
        matches!(self, Self::TrackEnd { reason, .. } if reason.may_start_next())
    }

    pub(crate) fn from_model(node: usize, ev: model::Event) -> Self {
        match ev {
            model::Event::TrackStart { guild_id, track } => Self::TrackStart {
                node,
                guild: guild_id,
                track: Arc::new(track),
            },
            model::Event::TrackEnd {
                guild_id,
                track,
                reason,
            } => Self::TrackEnd {
                node,
                guild: guild_id,
                track: Arc::new(track),
                reason,
            },
            model::Event::TrackException {
                guild_id,
                track,
                exception,
            } => Self::TrackException {
                node,
                guild: guild_id,
                track: Arc::new(track),
                exception: Arc::new(exception),
            },
            model::Event::TrackStuck {
                guild_id,
                track,
                threshold_ms,
            } => Self::TrackStuck {
                node,
                guild: guild_id,
                track: Arc::new(track),
                threshold_ms,
            },
            model::Event::WebSocketClosed {
                guild_id,
                code,
                reason,
                by_remote,
            } => Self::WebSocketClosed {
                node,
                guild: guild_id,
                code,
                reason,
                by_remote,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TrackEndReason;
    use straight_rs_model::GuildId;
    #[test]
    fn from_model_maps_fields() {
        let track: straight_rs_model::Track = serde_json::from_str(r#"{"encoded":"E","info":{"identifier":"i","isSeekable":true,"author":"a","length":1,"isStream":false,"position":0,"title":"t","uri":null,"artworkUrl":null,"isrc":null,"sourceName":"s"}}"#).unwrap();
        let e = Event::from_model(
            3,
            straight_rs_model::Event::TrackEnd {
                guild_id: GuildId(7),
                track,
                reason: TrackEndReason::Finished,
            },
        );
        assert_eq!(e.guild(), Some(GuildId(7)));
        assert!(e.may_start_next());
        assert!(matches!(e, Event::TrackEnd { node: 3, .. }));
        assert_eq!(Event::NodeDisconnected { node: 0 }.guild(), None);
    }
}
