use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub players: u32,
    pub playing_players: u32,
    pub uptime: u64,
    pub memory: Memory,
    pub cpu: Cpu,
    #[serde(default)]
    pub frame_stats: Option<FrameStats>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Memory {
    pub free: u64,
    pub used: u64,
    pub allocated: u64,
    pub reservable: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cpu {
    pub cores: u32,
    pub system_load: f64,
    pub lavalink_load: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameStats {
    pub sent: i64,
    pub nulled: i64,
    pub deficit: i64,
}
