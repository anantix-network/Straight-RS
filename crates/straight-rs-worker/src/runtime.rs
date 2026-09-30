use crate::{
    config::WorkerConfig,
    error::{WorkerError, WorkerResult},
    gateway::{GatewayDriver, GatewayEvent, GatewayVoiceProxy},
    state::{StatusState, VoiceStateStore, WorkerStatus},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use straight_rs::{Event, LavalinkClient};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    task::JoinHandle,
};

pub struct WorkerBuilder<D> {
    config: WorkerConfig,
    gateway: D,
}
impl<D: GatewayDriver> WorkerBuilder<D> {
    pub fn new(config: WorkerConfig, gateway: D) -> Self {
        Self { config, gateway }
    }
    pub async fn build(self) -> WorkerResult<RunningWorker> {
        let WorkerConfig {
            bot_user_id,
            bot_token,
            nodes,
            bind_addr,
            gateway_command_timeout,
            shutdown_timeout,
            api_token,
            per_ip_request_limit,
            limiter_table_capacity,
            body_limit,
            callback_timeout,
            ..
        } = self.config;
        let (commands_tx, commands_rx) = mpsc::channel(64);
        let (gateway_tx, mut gateway_rx) = mpsc::channel(256);
        let (gateway_shutdown_tx, gateway_shutdown_rx) = watch::channel(false);
        let proxy = Arc::new(GatewayVoiceProxy::new(commands_tx, gateway_command_timeout));
        let mut builder = LavalinkClient::builder(bot_user_id).gateway(proxy);
        for node in nodes {
            builder = builder.node(node);
        }
        let (client, mut events) = builder
            .build_with_events()
            .await
            .map_err(|e| WorkerError::Gateway(e.to_string()))?;
        let gateway = Arc::new(self.gateway);
        let gateway_task = tokio::spawn(async move {
            gateway
                .run(
                    bot_token,
                    bot_user_id,
                    commands_rx,
                    gateway_tx,
                    gateway_shutdown_rx,
                )
                .await
        });
        let gateway_ready = Arc::new(AtomicBool::new(false));
        let degraded = Arc::new(AtomicBool::new(false));
        let lagged = Arc::new(AtomicU64::new(0));
        let voice = VoiceStateStore::default();
        let relay_voice = voice.clone();
        let relay_client = client.clone();
        let relay_ready = gateway_ready.clone();
        let relay_degraded = degraded.clone();
        let relay_lagged = lagged.clone();
        let relay_bot_id = bot_user_id;
        let relay_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    event = gateway_rx.recv() => match event {
                        Some(GatewayEvent::Ready) => relay_ready.store(true, Ordering::Release),
                        Some(GatewayEvent::Disconnected) => relay_ready.store(false, Ordering::Release),
                        Some(GatewayEvent::VoiceState { user_id, guild, update }) if user_id == relay_bot_id => { relay_voice.update(guild, update.channel_id); if relay_client.voice_state_update(guild, update).await.is_err() { relay_degraded.store(true, Ordering::Release); } }
                        Some(GatewayEvent::VoiceState { .. }) => {}
                        Some(GatewayEvent::VoiceServer { guild, update }) => { if relay_client.voice_server_update(guild, update).await.is_err() { relay_degraded.store(true, Ordering::Release); } }
                        None => { relay_degraded.store(true, Ordering::Release); break; }
                    },
                    event = events.recv() => match event {
                        Ok(Event::Ready { .. }) => {}, Ok(_) => {},
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => { relay_lagged.fetch_add(n, Ordering::Relaxed); },
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => { relay_degraded.store(true, Ordering::Release); break; }
                    }
                }
            }
        });
        Ok(RunningWorker {
            client: Some(client.clone()),
            status: StatusState {
                gateway_ready,
                degraded,
                lagged,
                lavalink: client,
            },
            voice,
            api_token,
            per_ip_request_limit,
            limiter_table_capacity,
            body_limit,
            callback_timeout,
            gateway_shutdown: Some(gateway_shutdown_tx),
            gateway_task: Some(gateway_task),
            relay_task: Some(relay_task),
            shutdown_timeout,
            bind_addr,
        })
    }
}

pub struct RunningWorker {
    client: Option<LavalinkClient>,
    status: StatusState,
    #[allow(dead_code)]
    voice: VoiceStateStore,
    api_token: crate::config::SecretString,
    per_ip_request_limit: usize,
    limiter_table_capacity: usize,
    body_limit: usize,
    callback_timeout: std::time::Duration,
    gateway_shutdown: Option<watch::Sender<bool>>,
    gateway_task: Option<JoinHandle<WorkerResult<()>>>,
    relay_task: Option<JoinHandle<()>>,
    shutdown_timeout: std::time::Duration,
    bind_addr: std::net::SocketAddr,
}
impl RunningWorker {
    pub fn router(&self) -> axum::Router {
        let client = self.status.lavalink.clone();
        let status = self.status.clone();
        let state = Arc::new(crate::api::WorkerApiState {
            client,
            voice: self.voice.clone(),
            status: Arc::new(move || status.snapshot()),
            body_limit: self.body_limit,
            deadline: self.callback_timeout,
        });
        let auth = crate::auth::AuthState::new(
            crate::config::SecretString::new(self.api_token.expose_secret()),
            crate::auth::RateLimitConfig {
                requests: self.per_ip_request_limit,
                window: std::time::Duration::from_secs(60),
                table_capacity: self.limiter_table_capacity,
            },
        );
        crate::api::router(state, auth)
    }
    pub fn status(&self) -> WorkerStatus {
        self.status.snapshot()
    }
    #[allow(dead_code)]
    pub(crate) fn voice_channel(
        &self,
        guild: straight_rs::GuildId,
    ) -> Option<straight_rs::ChannelId> {
        self.voice.channel(guild)
    }
    pub async fn serve(&mut self, shutdown: watch::Receiver<bool>) -> WorkerResult<()> {
        let listener = TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| WorkerError::Gateway(e.to_string()))?;
        self.serve_on(listener, shutdown).await
    }
    pub async fn serve_on(
        &mut self,
        listener: TcpListener,
        mut shutdown: watch::Receiver<bool>,
    ) -> WorkerResult<()> {
        axum::serve(
            listener,
            self.router()
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = shutdown.wait_for(|v| *v).await;
        })
        .await
        .map_err(|e| WorkerError::Gateway(e.to_string()))?;
        self.shutdown().await
    }
    pub async fn shutdown(&mut self) -> WorkerResult<()> {
        if let Some(tx) = self.gateway_shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(client) = self.client.take() {
            client.shutdown();
        }
        let deadline = tokio::time::Instant::now() + self.shutdown_timeout;
        let mut failure = None;
        if let Some(mut task) = self.gateway_task.take() {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => failure = Some(e),
                Ok(Err(e)) => failure = Some(WorkerError::Gateway(e.to_string())),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    failure = Some(WorkerError::Gateway(
                        "gateway shutdown deadline elapsed".into(),
                    ));
                }
            }
        }
        if let Some(mut task) = self.relay_task.take() {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if failure.is_none() {
                        failure = Some(WorkerError::Gateway(e.to_string()));
                    }
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    if failure.is_none() {
                        failure = Some(WorkerError::Gateway(
                            "event relay shutdown deadline elapsed".into(),
                        ));
                    }
                }
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
impl Drop for RunningWorker {
    fn drop(&mut self) {
        if let Some(tx) = self.gateway_shutdown.take() {
            let _ = tx.send(true);
        }
        if let Some(client) = self.client.take() {
            client.shutdown();
        }
        if let Some(task) = self.gateway_task.take() {
            task.abort();
        }
        if let Some(task) = self.relay_task.take() {
            task.abort();
        }
    }
}
