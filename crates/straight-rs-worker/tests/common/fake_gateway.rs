use straight_rs_model::{ChannelId, GuildId, UserId};
use straight_rs::{VoiceServerUpdate, VoiceStateUpdate};
use straight_rs_worker::{GatewayCommand, GatewayDriver, GatewayEvent, GatewayFuture, SecretString, WorkerError, WorkerResult};
use tokio::sync::{mpsc, oneshot, watch};

pub struct FakeGateway;
impl GatewayDriver for FakeGateway {
 fn run<'a>(&'a self, _token: SecretString, _bot_user_id: UserId, mut commands: mpsc::Receiver<GatewayCommand>, events: mpsc::Sender<GatewayEvent>, mut shutdown: watch::Receiver<bool>) -> GatewayFuture<'a> {
  Box::pin(async move { tokio::select! { _ = shutdown.changed() => Ok(()), command = commands.recv() => { if let Some(GatewayCommand::SetVoiceState { reply, .. }) = command { let _ = reply.send(Ok(())); } let _ = events.send(GatewayEvent::Ready).await; Ok(()) } } })
 }
}

#[allow(dead_code)]
pub fn voice_state(guild: GuildId, channel: Option<ChannelId>) -> GatewayEvent { GatewayEvent::VoiceState { guild, update: VoiceStateUpdate { channel_id: channel, session_id: "session".into() } } }
#[allow(dead_code)]
pub fn server(guild: GuildId) -> GatewayEvent { GatewayEvent::VoiceServer { guild, update: VoiceServerUpdate { token: "secret-token".into(), endpoint: Some("voice.example:443".into()) } } }
#[allow(dead_code)]
pub fn reply() -> (oneshot::Sender<WorkerResult<()>>, oneshot::Receiver<WorkerResult<()>>) { oneshot::channel::<WorkerResult<()>>() }
#[allow(dead_code)]
fn _error_type(_: WorkerError) {}
