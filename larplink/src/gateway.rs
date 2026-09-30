use crate::Result;
use larplink_model::{ChannelId, GuildId};
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Implemented by the host Discord library so larplink can join/leave voice
/// channels (sends gateway opcode 4).
pub trait VoiceGateway: Send + Sync + 'static {
    fn join(&self, guild: GuildId, channel: ChannelId) -> BoxFuture<'_, Result<()>>;
    fn leave(&self, guild: GuildId) -> BoxFuture<'_, Result<()>>;
}
