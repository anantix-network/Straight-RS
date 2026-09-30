use straight_rs::{VoiceServerUpdate, VoiceStateUpdate};
use straight_rs_model::{ChannelId, GuildId, UserId};
use straight_rs_worker::{
    GatewayCommand, GatewayDriver, GatewayEvent, GatewayFuture, SecretString, WorkerError,
    WorkerResult,
};
use tokio::sync::{mpsc, oneshot, watch};

pub struct FakeGateway;

#[allow(dead_code)]
pub struct ControlledGateway {
    pub events: mpsc::UnboundedSender<GatewayEvent>,
    receiver: std::sync::Mutex<Option<mpsc::UnboundedReceiver<GatewayEvent>>>,
    pub stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub fail_on_shutdown: bool,
    pub initial_ready: bool,
    pub ready: mpsc::UnboundedSender<GatewayEvent>,
}
#[allow(dead_code)]
impl ControlledGateway {
    pub fn new(fail_on_shutdown: bool) -> Self {
        Self::with_initial_ready(fail_on_shutdown, true)
    }
    pub fn withheld_ready() -> Self {
        Self::with_initial_ready(false, false)
    }
    fn with_initial_ready(fail_on_shutdown: bool, initial_ready: bool) -> Self {
        let (events, receiver) = mpsc::unbounded_channel();
        Self {
            ready: events.clone(),
            events,
            receiver: std::sync::Mutex::new(Some(receiver)),
            stopped: std::sync::Arc::default(),
            fail_on_shutdown,
            initial_ready,
        }
    }
}
impl GatewayDriver for ControlledGateway {
    fn run<'a>(
        &'a self,
        _token: SecretString,
        _bot_user_id: UserId,
        _commands: mpsc::Receiver<GatewayCommand>,
        events: mpsc::Sender<GatewayEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> GatewayFuture<'a> {
        let mut source = self
            .receiver
            .lock()
            .unwrap()
            .take()
            .expect("gateway driver runs once");
        let stopped = self.stopped.clone();
        let initial_ready = self.initial_ready;
        Box::pin(async move {
            if initial_ready {
                events
                    .send(GatewayEvent::Ready)
                    .await
                    .map_err(|_| WorkerError::GatewayClosed)?;
            }
            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        stopped.store(true, std::sync::atomic::Ordering::SeqCst);
                        return if self.fail_on_shutdown { Err(WorkerError::Gateway("injected gateway failure".into())) } else { Ok(()) };
                    },
                    event = source.recv() => match event {
                        Some(event) => events.send(event).await.map_err(|_| WorkerError::GatewayClosed)?,
                        None => return Ok(()),
                    }
                }
            }
        })
    }
}

pub type VoiceCalls = std::sync::Arc<std::sync::Mutex<Vec<(GuildId, Option<ChannelId>)>>>;
#[allow(dead_code)]
#[derive(Clone, Default)]
pub struct RecordingGateway {
    pub calls: VoiceCalls,
}
impl GatewayDriver for RecordingGateway {
    fn run<'a>(
        &'a self,
        _token: SecretString,
        _bot_user_id: UserId,
        mut commands: mpsc::Receiver<GatewayCommand>,
        events: mpsc::Sender<GatewayEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> GatewayFuture<'a> {
        let calls = self.calls.clone();
        Box::pin(async move {
            events
                .send(GatewayEvent::Ready)
                .await
                .map_err(|_| WorkerError::GatewayClosed)?;
            loop {
                tokio::select! {
                 _ = shutdown.changed() => return Ok(()),
                 command = commands.recv() => match command {
                  Some(GatewayCommand::SetVoiceState { guild, channel, reply }) => {
                   calls.lock().unwrap().push((guild, channel));
                   let _ = reply.send(Ok(()));
                  }
                  None => return Ok(()),
                 }
                }
            }
        })
    }
}

impl GatewayDriver for FakeGateway {
    fn run<'a>(
        &'a self,
        _token: SecretString,
        _bot_user_id: UserId,
        mut commands: mpsc::Receiver<GatewayCommand>,
        events: mpsc::Sender<GatewayEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) -> GatewayFuture<'a> {
        Box::pin(async move {
            events
                .send(GatewayEvent::Ready)
                .await
                .map_err(|_| WorkerError::GatewayClosed)?;
            loop {
                tokio::select! {
                 _ = shutdown.changed() => return Ok(()),
                 command = commands.recv() => match command {
                  Some(GatewayCommand::SetVoiceState { guild, channel, reply }) => {
                   let _ = reply.send(Ok(()));
                   events.send(voice_state(guild, channel)).await.map_err(|_| WorkerError::GatewayClosed)?;
                  }
                  None => return Ok(()),
                 }
                }
            }
        })
    }
}

pub fn voice_state(guild: GuildId, channel: Option<ChannelId>) -> GatewayEvent {
    GatewayEvent::VoiceState {
        user_id: UserId(9),
        guild,
        update: VoiceStateUpdate {
            channel_id: channel,
            session_id: "session".into(),
        },
    }
}
#[allow(dead_code)]
pub fn server(guild: GuildId) -> GatewayEvent {
    GatewayEvent::VoiceServer {
        guild,
        update: VoiceServerUpdate {
            token: "secret-token".into(),
            endpoint: Some("voice.example:443".into()),
        },
    }
}
#[allow(dead_code)]
pub fn reply() -> (
    oneshot::Sender<WorkerResult<()>>,
    oneshot::Receiver<WorkerResult<()>>,
) {
    oneshot::channel::<WorkerResult<()>>()
}
#[allow(dead_code)]
fn _error_type(_: WorkerError) {}
