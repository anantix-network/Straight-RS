use std::time::Duration;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults() {
        let c = NodeConfig::new("localhost:2333", "pw");
        assert!(!c.secure);
        assert_eq!(c.resume_timeout_secs, 60);
        assert_eq!(c.with_tls(true).secure, true);
    }
}
