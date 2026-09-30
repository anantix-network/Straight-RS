//! larplink — a fast, complete Lavalink v4 client.
pub mod balancer;
pub(crate) mod backoff;
mod config;
mod error;
pub mod position;
pub mod rest;

pub use balancer::{NodeView, Strategy};
pub use config::NodeConfig;
pub use error::{Error, Result};
pub use rest::RestClient;
pub use larplink_model as model;
pub use larplink_model::{
    ChannelId, Exception, Filters, GuildId, Info, LoadResult, Severity, Stats, Track,
    TrackEndReason, TrackInfo, UpdatePlayer, UpdateTrack, UserId, VoiceState,
};

mod event;
mod gateway;
pub(crate) mod state;
mod voice;

pub use event::Event;
pub use gateway::{BoxFuture, VoiceGateway};
pub use state::PlayerSnapshot;
pub use voice::{VoiceOutcome, VoiceServerUpdate, VoiceStateUpdate};

mod client;
mod hub;
mod node;

pub use client::{ClientBuilder, LavalinkClient};
pub use node::{Node, NodeStatus};

mod player;

pub use player::{Player, PlayerEvents};
pub mod adapters;
