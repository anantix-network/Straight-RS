use crate::{
    config::SecretString,
    error::{WorkerError, WorkerResult},
};
use std::{future::Future, pin::Pin, time::Duration};
use straight_rs::{ChannelId, GuildId, VoiceGateway, VoiceServerUpdate, VoiceStateUpdate};
use straight_rs_model::UserId;
use tokio::sync::{mpsc, oneshot, watch};

pub type GatewayFuture<'a> = Pin<Box<dyn Future<Output = WorkerResult<()>> + Send + 'a>>;

pub enum GatewayCommand {
    SetVoiceState {
        guild: GuildId,
        channel: Option<ChannelId>,
        reply: oneshot::Sender<WorkerResult<()>>,
    },
}
pub enum GatewayEvent {
    VoiceState {
        user_id: UserId,
        guild: GuildId,
        update: VoiceStateUpdate,
    },
    VoiceServer {
        guild: GuildId,
        update: VoiceServerUpdate,
    },
    Ready,
    Disconnected,
}

pub trait GatewayDriver: Send + Sync + 'static {
    fn run<'a>(
        &'a self,
        token: SecretString,
        bot_user_id: UserId,
        commands: mpsc::Receiver<GatewayCommand>,
        events: mpsc::Sender<GatewayEvent>,
        shutdown: watch::Receiver<bool>,
    ) -> GatewayFuture<'a>;
}

pub struct GatewayVoiceProxy {
    commands: mpsc::Sender<GatewayCommand>,
    timeout: Duration,
}
impl GatewayVoiceProxy {
    pub fn new(commands: mpsc::Sender<GatewayCommand>, timeout: Duration) -> Self {
        Self { commands, timeout }
    }
    async fn set_voice(&self, guild: GuildId, channel: Option<ChannelId>) -> WorkerResult<()> {
        let (reply, response) = oneshot::channel();
        tokio::time::timeout(
            self.timeout,
            self.commands.send(GatewayCommand::SetVoiceState {
                guild,
                channel,
                reply,
            }),
        )
        .await
        .map_err(|_| WorkerError::GatewayTimeout)?
        .map_err(|_| WorkerError::GatewayClosed)?;
        tokio::time::timeout(self.timeout, response)
            .await
            .map_err(|_| WorkerError::GatewayTimeout)?
            .map_err(|_| WorkerError::GatewayClosed)?
    }
}
impl VoiceGateway for GatewayVoiceProxy {
    fn join(
        &self,
        guild: GuildId,
        channel: ChannelId,
    ) -> straight_rs::BoxFuture<'_, straight_rs::Result<()>> {
        Box::pin(async move {
            self.set_voice(guild, Some(channel))
                .await
                .map_err(|e| straight_rs::Error::Gateway(Box::new(e)))
        })
    }
    fn leave(&self, guild: GuildId) -> straight_rs::BoxFuture<'_, straight_rs::Result<()>> {
        Box::pin(async move {
            self.set_voice(guild, None)
                .await
                .map_err(|e| straight_rs::Error::Gateway(Box::new(e)))
        })
    }
}
