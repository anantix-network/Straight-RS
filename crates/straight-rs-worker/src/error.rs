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
}
