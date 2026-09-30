use thiserror::Error;

pub type WorkerResult<T> = Result<T, WorkerError>;

#[derive(Debug, Error)]
#[non_exhaustive]
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
    /// A Lavalink operation failed. Carries only a static category: never a REST path,
    /// session id or provider-supplied text.
    #[error("lavalink operation failed: {0}")]
    Lavalink(&'static str),
}
impl From<straight_rs::Error> for WorkerError {
    fn from(e: straight_rs::Error) -> Self {
        use straight_rs::Error;
        Self::Lavalink(match e {
            Error::NoNode => "no node available",
            Error::Timeout => "timeout",
            Error::PlayerNotFound => "player not found",
            Error::Closed => "closed",
            _ => "lavalink request failed",
        })
    }
}
