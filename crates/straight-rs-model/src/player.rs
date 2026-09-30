use crate::{ChannelId, Filters, GuildId, Track};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Player {
    pub guild_id: GuildId,
    pub track: Option<Track>,
    pub volume: u16,
    pub paused: bool,
    pub state: PlayerState,
    pub voice: PlayerVoice,
    pub filters: Filters,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PlayerState {
    pub time: u64,
    pub position: u64,
    pub connected: bool,
    pub ping: i64,
}

/// Voice connection sent to Lavalink. `channel_id` is required since Lavalink 4.2.0,
/// which uses it for DAVE (Discord's end-to-end encrypted voice); without it Discord
/// voice connections cannot be established.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoiceState {
    pub token: String,
    pub endpoint: String,
    pub session_id: String,
    pub channel_id: ChannelId,
}

/// The voice connection as reported back by Lavalink in a [`Player`]. Older servers
/// (and players that never joined a channel) leave `channel_id` out.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlayerVoice {
    pub token: String,
    pub endpoint: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<ChannelId>,
}

/// Body of `PATCH /v4/sessions/{id}/players/{guild}`.
/// `Some(None)` on a nested option serializes as JSON `null` (used to stop / clear).
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdatePlayer {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track: Option<UpdateTrack>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Filters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<VoiceState>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTrack {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoded: Option<Option<Arc<str>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_data: Option<Value>,
}
