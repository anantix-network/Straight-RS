use crate::exception::Exception;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Track {
    pub encoded: Arc<str>,
    pub info: TrackInfo,
    #[serde(default)]
    pub plugin_info: Value,
    #[serde(default)]
    pub user_data: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackInfo {
    pub identifier: String,
    pub is_seekable: bool,
    pub author: String,
    pub length: u64,
    pub is_stream: bool,
    pub position: u64,
    pub title: String,
    pub uri: Option<String>,
    pub artwork_url: Option<String>,
    pub isrc: Option<String>,
    pub source_name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaylistInfo {
    pub name: String,
    pub selected_track: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Playlist {
    pub info: PlaylistInfo,
    #[serde(default)]
    pub plugin_info: Value,
    pub tracks: Vec<Track>,
}

/// Payload of `loadType: "empty"` (always `{}`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NoMatches {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "loadType", content = "data", rename_all = "camelCase")]
pub enum LoadResult {
    Track(Track),
    Playlist(Playlist),
    Search(Vec<Track>),
    Empty(NoMatches),
    Error(Exception),
}
