use crate::{Error, Result};
use std::time::Duration;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Uri};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// `host:port`
    pub host: String,
    pub password: String,
    /// Use https/wss (needs the `tls` feature).
    pub secure: bool,
    pub request_timeout: Duration,
    /// Seconds the server keeps a session after the socket drops.
    pub resume_timeout_secs: u64,
    /// How long a node may stay down before its players migrate.
    pub failover_grace: Duration,
    pub ping_interval: Duration,
    pub ping_timeout: Duration,
}

impl NodeConfig {
    pub fn new(host: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            password: password.into(),
            secure: false,
            request_timeout: Duration::from_secs(10),
            resume_timeout_secs: 60,
            failover_grace: Duration::from_secs(10),
            ping_interval: Duration::from_secs(15),
            ping_timeout: Duration::from_secs(45),
        }
    }

    pub fn with_tls(mut self, secure: bool) -> Self {
        self.secure = secure;
        self
    }

    /// Rejects settings that could never work (or would panic later).
    pub(crate) fn validate(&self) -> Result<()> {
        let bad = |m: String| Err(Error::Config(format!("node {}: {m}", self.host)));
        if Uri::try_from(format!("http://{}/", self.host)).is_err() || self.host.is_empty() {
            return bad("host must be a valid `host:port`".into());
        }
        if HeaderValue::from_str(&self.password).is_err() {
            return bad("password is not a valid header value".into());
        }
        if self.secure && !cfg!(feature = "tls") {
            return bad("secure = true needs the `tls` feature".into());
        }
        if self.ping_interval.is_zero()
            || std::time::Instant::now()
                .checked_add(self.ping_interval)
                .is_none()
        {
            return bad("ping_interval must be non-zero and finite".into());
        }
        if self.ping_timeout < self.ping_interval {
            return bad("ping_timeout must be >= ping_interval".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults() {
        let c = NodeConfig::new("localhost:2333", "pw");
        assert!(!c.secure);
        assert_eq!(c.resume_timeout_secs, 60);
        assert!(c.with_tls(true).secure);
    }
}
