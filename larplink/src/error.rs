use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] hyper::Error),
    #[error("http client error: {0}")]
    HttpClient(#[from] hyper_util::client::legacy::Error),
    #[error("websocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("lavalink error {status} {error}: {message} ({path})")]
    Lavalink {
        status: u16,
        error: String,
        message: String,
        path: String,
    },
    #[error("no lavalink node available")]
    NoNode,
    #[error("request timed out")]
    Timeout,
    #[error("player not found")]
    PlayerNotFound,
    #[error("client closed")]
    Closed,
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("voice gateway error: {0}")]
    Gateway(#[source] Box<dyn std::error::Error + Send + Sync>),
}
