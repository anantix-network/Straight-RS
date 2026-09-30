use thiserror::Error;

pub type WorkerResult<T> = Result<T, WorkerError>;

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("invalid worker configuration: {0}")]
    Config(String),
    #[error("gateway command channel is closed")]
    GatewayClosed,
    #[error("gateway command timed out")]
    GatewayTimeout,
    #[error("gateway operation failed: {0}")]
    Gateway(String),
    #[error("required plugin `{name}` failed: {reason}")]
    Plugin {
        name: &'static str,
        reason: &'static str,
    },
    #[error("lavalink operation failed: {0}")]
    Lavalink(String),
}
impl From<straight_rs::Error> for WorkerError {
    fn from(e: straight_rs::Error) -> Self {
        Self::Lavalink(e.to_string())
    }
}
