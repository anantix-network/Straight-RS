use crate::{
    auth::{self, AuthState},
    state::{VoiceStateStore, WorkerStatus},
};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use straight_rs::{GuildId, LavalinkClient, LoadResult};

#[derive(Clone)]
pub struct WorkerApiState {
    pub client: LavalinkClient,
    pub voice: VoiceStateStore,
    pub status: Arc<dyn Fn() -> WorkerStatus + Send + Sync>,
    pub body_limit: usize,
    pub deadline: Duration,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlayerView {
    guild_id: String,
    track: Option<TrackView>,
    paused: bool,
    volume: u16,
    position_ms: u64,
    connected: bool,
    ping: i64,
    voice_channel_id: Option<String>,
    node_index: Option<usize>,
    ready: bool,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackView {
    identifier: String,
    title: String,
    author: String,
    length_ms: u64,
    is_stream: bool,
    uri: Option<String>,
    source_name: String,
}
impl PlayerView {
    fn make(g: GuildId, s: &WorkerApiState) -> Self {
        let p = s.client.player(g);
        let snap = p.snapshot();
        let track = snap.track.as_ref().map(|t| TrackView {
            identifier: t.info.identifier.clone(),
            title: t.info.title.clone(),
            author: t.info.author.clone(),
            length_ms: t.info.length,
            is_stream: t.info.is_stream,
            uri: t.info.uri.clone(),
            source_name: t.info.source_name.clone(),
        });
        Self {
            guild_id: g.0.to_string(),
            track,
            paused: snap.paused,
            volume: snap.volume,
            position_ms: snap.position_now(),
            connected: snap.connected,
            ping: snap.ping,
            voice_channel_id: s.voice.channel(g).map(|c| c.0.to_string()),
            node_index: p.node_index(),
            ready: (s.status)().ready,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Join {
    channel_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Play {
    identifier: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Pause {
    paused: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Seek {
    position_ms: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Volume {
    volume: u16,
}
#[derive(Serialize)]
struct Health {
    status: &'static str,
}
fn err(status: StatusCode, code: &'static str, message: &'static str) -> axum::response::Response {
    auth::error_response(status, code, message)
}
fn parse<T: std::str::FromStr>(s: &str) -> Option<T> {
    if s.is_empty() || s.len() > 20 || !s.bytes().all(|b| b.is_ascii_digit()) || s.starts_with('0')
    {
        None
    } else {
        s.parse().ok()
    }
}
fn invalid() -> axum::response::Response {
    err(
        StatusCode::BAD_REQUEST,
        "invalid_request",
        "Request is invalid.",
    )
}
fn not_ready() -> axum::response::Response {
    err(
        StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        "Playback service is unavailable.",
    )
}
async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}
async fn ready(State(s): State<Arc<WorkerApiState>>) -> axum::response::Response {
    if (s.status)().ready {
        Json(Health { status: "ready" }).into_response()
    } else {
        err(
            StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            "Worker is not ready.",
        )
    }
}
async fn get_player(
    State(s): State<Arc<WorkerApiState>>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let Some(g) = parse::<u64>(&id).map(GuildId) else {
        return invalid();
    };
    Json(PlayerView::make(g, &s)).into_response()
}
async fn act(
    s: Arc<WorkerApiState>,
    f: impl std::future::Future<Output = straight_rs::Result<()>>,
) -> axum::response::Response {
    match tokio::time::timeout(s.deadline, f).await {
        Err(_) => err(
            StatusCode::GATEWAY_TIMEOUT,
            "deadline",
            "Operation deadline elapsed.",
        ),
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err(straight_rs::Error::NoNode)) => not_ready(),
        Ok(Err(_)) => err(
            StatusCode::BAD_GATEWAY,
            "operation_failed",
            "Playback operation failed.",
        ),
    }
}
async fn join(
    State(s): State<Arc<WorkerApiState>>,
    Path(id): Path<String>,
    Json(b): Json<Join>,
) -> axum::response::Response {
    let (Some(g), Some(c)) = (
        parse::<u64>(&id).map(GuildId),
        parse::<u64>(&b.channel_id).map(straight_rs::ChannelId),
    ) else {
        return invalid();
    };
    let p = s.client.player(g);
    act(s.clone(), async move { p.join(c).await }).await
}
async fn play(
    State(s): State<Arc<WorkerApiState>>,
    Path(id): Path<String>,
    Json(b): Json<Play>,
) -> axum::response::Response {
    let Some(g) = parse::<u64>(&id).map(GuildId) else {
        return invalid();
    };
    if b.identifier.trim().is_empty() {
        return invalid();
    }
    if !(s.status)().lavalink_ready {
        return not_ready();
    }
    let c = s.client.clone();
    let deadline = s.deadline;
    match tokio::time::timeout(deadline, async move {
        match c.load(&b.identifier).await {
            Err(straight_rs::Error::NoNode) => Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "Playback service is unavailable.",
            )),
            Err(_) => Err((
                StatusCode::BAD_GATEWAY,
                "load_failed",
                "Playback request failed.",
            )),
            Ok(LoadResult::Error(_)) => Err((
                StatusCode::BAD_GATEWAY,
                "load_failed",
                "Playback request failed.",
            )),
            Ok(LoadResult::Empty(_)) => Err((
                StatusCode::NOT_FOUND,
                "not_found",
                "No playable track was found.",
            )),
            Ok(LoadResult::Track(t)) => c.player(g).play(&t).await.map_err(|_| {
                (
                    StatusCode::BAD_GATEWAY,
                    "playback_failed",
                    "Playback request failed.",
                )
            }),
            Ok(LoadResult::Search(ts)) => match ts.first() {
                Some(t) => c.player(g).play(t).await.map_err(|_| {
                    (
                        StatusCode::BAD_GATEWAY,
                        "playback_failed",
                        "Playback request failed.",
                    )
                }),
                None => Err((
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "No playable track was found.",
                )),
            },
            Ok(LoadResult::Playlist(p)) => {
                let idx = usize::try_from(p.info.selected_track)
                    .ok()
                    .filter(|i| *i < p.tracks.len())
                    .unwrap_or(0);
                match p.tracks.get(idx) {
                    Some(t) => c.player(g).play(t).await.map_err(|_| {
                        (
                            StatusCode::BAD_GATEWAY,
                            "playback_failed",
                            "Playback request failed.",
                        )
                    }),
                    None => Err((
                        StatusCode::NOT_FOUND,
                        "not_found",
                        "No playable track was found.",
                    )),
                }
            }
        }
    })
    .await
    {
        Err(_) => err(
            StatusCode::GATEWAY_TIMEOUT,
            "deadline",
            "Operation deadline elapsed.",
        ),
        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
        Ok(Err((st, code, msg))) => err(st, code, msg),
    }
}
async fn pause(
    State(s): State<Arc<WorkerApiState>>,
    Path(id): Path<String>,
    Json(b): Json<Pause>,
) -> axum::response::Response {
    let Some(g) = parse::<u64>(&id).map(GuildId) else {
        return invalid();
    };
    let p = s.client.player(g);
    act(s.clone(), async move { p.pause(b.paused).await }).await
}
async fn resume(
    State(s): State<Arc<WorkerApiState>>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let Some(g) = parse::<u64>(&id).map(GuildId) else {
        return invalid();
    };
    let p = s.client.player(g);
    act(s.clone(), async move { p.pause(false).await }).await
}
async fn seek(
    State(s): State<Arc<WorkerApiState>>,
    Path(id): Path<String>,
    Json(b): Json<Seek>,
) -> axum::response::Response {
    let Some(g) = parse::<u64>(&id).map(GuildId) else {
        return invalid();
    };
    if b.position_ms > 86_400_000 {
        return invalid();
    }
    let p = s.client.player(g);
    act(s.clone(), async move { p.seek(b.position_ms).await }).await
}
async fn volume(
    State(s): State<Arc<WorkerApiState>>,
    Path(id): Path<String>,
    Json(b): Json<Volume>,
) -> axum::response::Response {
    let Some(g) = parse::<u64>(&id).map(GuildId) else {
        return invalid();
    };
    if b.volume > 1000 {
        return invalid();
    }
    let p = s.client.player(g);
    act(s.clone(), async move { p.set_volume(b.volume).await }).await
}
async fn stop(
    State(s): State<Arc<WorkerApiState>>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let Some(g) = parse::<u64>(&id).map(GuildId) else {
        return invalid();
    };
    let p = s.client.player(g);
    act(s.clone(), async move { p.stop().await }).await
}
async fn leave(
    State(s): State<Arc<WorkerApiState>>,
    Path(id): Path<String>,
) -> axum::response::Response {
    let Some(g) = parse::<u64>(&id).map(GuildId) else {
        return invalid();
    };
    let p = s.client.player(g);
    act(s.clone(), async move { p.leave().await }).await
}
pub fn router(state: Arc<WorkerApiState>, auth_state: AuthState) -> Router {
    let limit = state.body_limit;
    let r = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/v1/guilds/{guild_id}/player", get(get_player))
        .route("/v1/guilds/{guild_id}/join", post(join))
        .route("/v1/guilds/{guild_id}/play", post(play))
        .route("/v1/guilds/{guild_id}/pause", post(pause))
        .route("/v1/guilds/{guild_id}/resume", post(resume))
        .route("/v1/guilds/{guild_id}/seek", post(seek))
        .route("/v1/guilds/{guild_id}/volume", post(volume))
        .route("/v1/guilds/{guild_id}/stop", post(stop))
        .route("/v1/guilds/{guild_id}/leave", post(leave))
        .with_state(state)
        .layer(axum::extract::DefaultBodyLimit::max(limit));
    auth::secure(r, auth_state)
}
