use crate::{
    config::SecretString,
    error::{WorkerError, WorkerResult},
    gateway::{GatewayCommand, GatewayDriver, GatewayEvent, GatewayFuture},
};
use serenity::{
    async_trait,
    client::{Context, EventHandler},
    gateway::ShardMessenger,
    model::{
        event::{ResumedEvent, VoiceServerUpdateEvent},
        gateway::{GatewayIntents, Ready},
        voice::VoiceState,
    },
};
use std::{future::Future, sync::Arc};
use straight_rs::{ChannelId, GuildId, VoiceServerUpdate, VoiceStateUpdate};
use straight_rs_model::UserId;
use tokio::sync::{Mutex, mpsc, watch};

pub struct SerenityGatewayDriver;
impl SerenityGatewayDriver {
    pub fn new() -> Self {
        Self
    }
}
impl Default for SerenityGatewayDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
struct Handler {
    bot_user_id: UserId,
    events: mpsc::Sender<GatewayEvent>,
    messenger: Arc<Mutex<Option<ShardMessenger>>>,
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, _: Ready) {
        *self.messenger.lock().await = Some(ctx.shard.clone());
        let _ = self.events.send(GatewayEvent::Ready).await;
    }
    async fn resume(&self, ctx: Context, _: ResumedEvent) {
        *self.messenger.lock().await = Some(ctx.shard.clone());
        let _ = self.events.send(GatewayEvent::Ready).await;
    }
    async fn voice_state_update(&self, _: Context, _: Option<VoiceState>, new: VoiceState) {
        if let Some((guild, update)) = map_voice_state(&new, self.bot_user_id) {
            let _ = self
                .events
                .send(GatewayEvent::VoiceState {
                    user_id: self.bot_user_id,
                    guild,
                    update,
                })
                .await;
        }
    }
    async fn voice_server_update(&self, _: Context, event: VoiceServerUpdateEvent) {
        if let Some((guild, update)) = map_voice_server(&event) {
            let _ = self
                .events
                .send(GatewayEvent::VoiceServer { guild, update })
                .await;
        }
    }
}

fn map_voice_state(vs: &VoiceState, bot: UserId) -> Option<(GuildId, VoiceStateUpdate)> {
    if vs.user_id.get() != bot.0 {
        return None;
    }
    Some((
        GuildId(vs.guild_id?.get()),
        VoiceStateUpdate {
            channel_id: vs.channel_id.map(|channel| ChannelId(channel.get())),
            session_id: vs.session_id.clone(),
        },
    ))
}
fn map_voice_server(event: &VoiceServerUpdateEvent) -> Option<(GuildId, VoiceServerUpdate)> {
    Some((
        GuildId(event.guild_id?.get()),
        VoiceServerUpdate {
            token: event.token.clone(),
            endpoint: event.endpoint.clone(),
        },
    ))
}
fn voice_state_payload(guild: GuildId, channel: Option<ChannelId>) -> serde_json::Value {
    serde_json::json!({"op": 4, "d": {
        "guild_id": guild.0.to_string(),
        "channel_id": channel.map(|id| id.0.to_string()),
        "self_mute": false, "self_deaf": false,
    }})
}

async fn command_loop<F, Fut>(
    mut commands: mpsc::Receiver<GatewayCommand>,
    mut shutdown: watch::Receiver<bool>,
    mut send_voice_state: F,
) -> WorkerResult<()>
where
    F: FnMut(GuildId, Option<ChannelId>) -> Fut,
    Fut: Future<Output = WorkerResult<()>>,
{
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return Ok(()); },
            command = commands.recv() => match command {
                Some(GatewayCommand::SetVoiceState { guild, channel, reply }) => {
                    let _ = reply.send(send_voice_state(guild, channel).await);
                }
                None => return Ok(()),
            }
        }
    }
}

impl GatewayDriver for SerenityGatewayDriver {
    fn run<'a>(
        &'a self,
        token: SecretString,
        bot_user_id: UserId,
        commands: mpsc::Receiver<GatewayCommand>,
        events: mpsc::Sender<GatewayEvent>,
        shutdown: watch::Receiver<bool>,
    ) -> GatewayFuture<'a> {
        Box::pin(async move {
            let messenger = Arc::new(Mutex::new(None));
            let handler = Handler {
                bot_user_id,
                events,
                messenger: messenger.clone(),
            };
            let mut client = serenity::Client::builder(
                token.expose_secret(),
                GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES,
            )
            .event_handler(handler)
            .await
            .map_err(|error| WorkerError::Gateway(error.to_string()))?;
            let command_task = command_loop(commands, shutdown, move |guild, channel| {
                let messenger = messenger.clone();
                async move {
                    let messenger = messenger.lock().await;
                    let shard = messenger.as_ref().ok_or_else(|| {
                        WorkerError::Gateway("Serenity Gateway is not ready".into())
                    })?;
                    shard.websocket_message(voice_state_payload(guild, channel).to_string().into());
                    Ok(())
                }
            });
            tokio::pin!(command_task);
            tokio::select! {
                result = client.start() => {
                    let _ = client.shard_manager.shutdown_all().await;
                    match result {
                        Ok(()) => Err(WorkerError::Gateway("Serenity Gateway stopped unexpectedly".into())),
                        Err(error) => Err(WorkerError::Gateway(error.to_string())),
                    }
                }
                result = &mut command_task => {
                    let _ = client.shard_manager.shutdown_all().await;
                    result
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn voice_state(user: u64, channel: Option<u64>) -> VoiceState {
        serde_json::from_value(serde_json::json!({
            "channel_id": channel.map(|c| c.to_string()), "deaf": false, "guild_id": "100", "mute": false,
            "self_deaf": false, "self_mute": false, "self_video": false, "session_id": "sess",
            "suppress": false, "user_id": user.to_string(), "request_to_speak_timestamp": null
        })).unwrap()
    }
    #[test]
    fn maps_only_configured_bot_voice_state_and_allows_leave() {
        let (guild, update) = map_voice_state(&voice_state(1, Some(9)), UserId(1)).unwrap();
        assert_eq!(
            (guild, update.channel_id, update.session_id.as_str()),
            (GuildId(100), Some(ChannelId(9)), "sess")
        );
        assert!(map_voice_state(&voice_state(2, Some(9)), UserId(1)).is_none());
        assert_eq!(
            map_voice_state(&voice_state(1, None), UserId(1))
                .unwrap()
                .1
                .channel_id,
            None
        );
    }
    #[test]
    fn maps_voice_server_and_ignores_missing_guild() {
        let event: VoiceServerUpdateEvent = serde_json::from_value(
            serde_json::json!({"token":"tok","guild_id":"100","endpoint":"e:443"}),
        )
        .unwrap();
        let (guild, update) = map_voice_server(&event).unwrap();
        assert_eq!(
            (guild, update.token.as_str(), update.endpoint.as_deref()),
            (GuildId(100), "tok", Some("e:443"))
        );
        let missing: VoiceServerUpdateEvent = serde_json::from_value(
            serde_json::json!({"token":"tok","guild_id":null,"endpoint":null}),
        )
        .unwrap();
        assert!(map_voice_server(&missing).is_none());
    }
    #[test]
    fn opcode_four_payload_supports_join_and_leave() {
        let join = voice_state_payload(GuildId(10), Some(ChannelId(20)));
        assert_eq!(join["op"], 4);
        assert_eq!(join["d"]["guild_id"], "10");
        assert_eq!(join["d"]["channel_id"], "20");
        assert_eq!(join["d"]["self_mute"], false);
        assert_eq!(join["d"]["self_deaf"], false);
        assert!(voice_state_payload(GuildId(10), None)["d"]["channel_id"].is_null());
    }
    #[tokio::test]
    async fn command_loop_replies_and_exits_on_shutdown() {
        let (commands_tx, commands_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (reply, response) = tokio::sync::oneshot::channel();
        commands_tx
            .send(GatewayCommand::SetVoiceState {
                guild: GuildId(10),
                channel: None,
                reply,
            })
            .await
            .unwrap();
        shutdown_tx.send(true).unwrap();
        let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sent_by_loop = sent.clone();
        tokio::time::timeout(
            Duration::from_millis(100),
            command_loop(commands_rx, shutdown_rx, move |_, _| {
                sent_by_loop.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Ok(()) }
            }),
        )
        .await
        .expect("shutdown must be bounded")
        .unwrap();
        assert_eq!(sent.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(response.await.is_err());
    }
}
