use crate::{Exception, GuildId, PlayerState, Stats, Track};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ready {
    pub resumed: bool,
    pub session_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlayerUpdate {
    pub guild_id: GuildId,
    pub state: PlayerState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TrackEndReason {
    Finished,
    LoadFailed,
    Stopped,
    Replaced,
    Cleanup,
}

impl TrackEndReason {
    /// Whether the next track in a queue may be started.
    pub fn may_start_next(self) -> bool {
        matches!(self, Self::Finished | Self::LoadFailed)
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    #[serde(rename = "TrackStartEvent", rename_all = "camelCase")]
    TrackStart { guild_id: GuildId, track: Track },
    #[serde(rename = "TrackEndEvent", rename_all = "camelCase")]
    TrackEnd { guild_id: GuildId, track: Track, reason: TrackEndReason },
    #[serde(rename = "TrackExceptionEvent", rename_all = "camelCase")]
    TrackException { guild_id: GuildId, track: Track, exception: Exception },
    #[serde(rename = "TrackStuckEvent", rename_all = "camelCase")]
    TrackStuck { guild_id: GuildId, track: Track, threshold_ms: u64 },
    #[serde(rename = "WebSocketClosedEvent", rename_all = "camelCase")]
    WebSocketClosed { guild_id: GuildId, code: u16, reason: String, by_remote: bool },
}

impl Event {
    pub fn guild_id(&self) -> GuildId {
        match self {
            Self::TrackStart { guild_id, .. }
            | Self::TrackEnd { guild_id, .. }
            | Self::TrackException { guild_id, .. }
            | Self::TrackStuck { guild_id, .. }
            | Self::WebSocketClosed { guild_id, .. } => *guild_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum WsMessage {
    Ready(Ready),
    PlayerUpdate(PlayerUpdate),
    Stats(Stats),
    Event(Event),
    /// Unknown op / unknown event type / malformed known message (plugins).
    Unknown { op: String, payload: Value },
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Known {
    Ready(Ready),
    PlayerUpdate(PlayerUpdate),
    Stats(Stats),
    Event(Event),
}

impl WsMessage {
    /// Errors only when `text` is not valid JSON.
    pub fn parse(text: &str) -> Result<Self, serde_json::Error> {
        if let Ok(known) = serde_json::from_str::<Known>(text) {
            return Ok(match known {
                Known::Ready(v) => Self::Ready(v),
                Known::PlayerUpdate(v) => Self::PlayerUpdate(v),
                Known::Stats(v) => Self::Stats(v),
                Known::Event(v) => Self::Event(v),
            });
        }
        let payload: Value = serde_json::from_str(text)?;
        let op = payload.get("op").and_then(Value::as_str).unwrap_or("").to_owned();
        Ok(Self::Unknown { op, payload })
    }
}

/// Lavalink REST error body.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct RestError {
    #[serde(default)]
    pub status: u16,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub path: String,
}
