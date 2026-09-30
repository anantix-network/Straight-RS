//! larplink — a fast, complete Lavalink v4 client.
pub(crate) mod backoff;
pub mod balancer;
mod config;
mod error;
pub mod position;
pub mod rest;

pub use balancer::{NodeView, Strategy};
pub use config::NodeConfig;
pub use error::{Error, Result};
pub use larplink_model as model;
pub use larplink_model::{
    ChannelId, Exception, Filters, GuildId, Info, LoadResult, Severity, Stats, Track,
    TrackEndReason, TrackInfo, UpdatePlayer, UpdateTrack, UserId, VoiceState,
};
pub use rest::RestClient;

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

pub use client::{ClientBuilder, LavalinkClient, MAX_EVENT_CAPACITY};
pub use node::{Node, NodeStatus};

mod player;

pub use player::{Player, PlayerEvents};
pub mod adapters;
