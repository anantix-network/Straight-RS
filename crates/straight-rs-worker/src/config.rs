use crate::error::{WorkerError, WorkerResult};
use std::{fmt, net::SocketAddr, time::Duration};
use straight_rs::NodeConfig;
use straight_rs_model::UserId;

pub struct WorkerConfig {
    pub bot_user_id: UserId,
    pub(crate) bot_token: SecretString,
    pub nodes: Vec<NodeConfig>,
    pub bind_addr: SocketAddr,
    pub allow_remote_bind: bool,
    pub(crate) api_token: SecretString,
    pub body_limit: usize,
    pub callback_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub gateway_command_timeout: Duration,
    pub plugin_event_capacity: usize,
    pub per_ip_request_limit: usize,
    pub limiter_table_capacity: usize,
}

impl fmt::Debug for WorkerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerConfig")
            .field("bot_user_id", &self.bot_user_id)
            .field("bot_token", &"[REDACTED]")
            .field("nodes", &self.nodes)
            .field("bind_addr", &self.bind_addr)
            .field("allow_remote_bind", &self.allow_remote_bind)
            .field("api_token", &"[REDACTED]")
            .field("body_limit", &self.body_limit)
            .field("callback_timeout", &self.callback_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("gateway_command_timeout", &self.gateway_command_timeout)
            .field("plugin_event_capacity", &self.plugin_event_capacity)
            .field("per_ip_request_limit", &self.per_ip_request_limit)
            .field("limiter_table_capacity", &self.limiter_table_capacity)
            .finish()
    }
}

pub struct WorkerConfigBuilder {
    config: WorkerConfig,
}

impl WorkerConfigBuilder {
    pub fn new(
        bot_user_id: UserId,
        bot_token: SecretString,
        api_token: SecretString,
        nodes: Vec<NodeConfig>,
    ) -> Self {
        Self {
            config: WorkerConfig {
                bot_user_id,
                bot_token,
                nodes,
                bind_addr: "127.0.0.1:8080".parse().unwrap(),
                allow_remote_bind: false,
                api_token,
                body_limit: 1024 * 1024,
                callback_timeout: Duration::from_secs(10),
                shutdown_timeout: Duration::from_secs(10),
                gateway_command_timeout: Duration::from_secs(5),
                plugin_event_capacity: 256,
                per_ip_request_limit: 60,
                limiter_table_capacity: 4096,
            },
        }
    }
    pub fn bind_addr(mut self, value: SocketAddr, allow_remote: bool) -> Self {
        self.config.bind_addr = value;
        self.config.allow_remote_bind = allow_remote;
        self
    }
    pub fn build(self) -> WorkerResult<WorkerConfig> {
        self.config.validate()?;
        Ok(self.config)
    }
}

impl WorkerConfig {
    fn validate(&self) -> WorkerResult<()> {
        if self.bot_token.expose_secret().is_empty() || self.api_token.expose_secret().is_empty() {
            return Err(WorkerError::Config("credentials must not be empty".into()));
        }
        if self.api_token.expose_secret().len() < 32 {
            return Err(WorkerError::Config(
                "API token must be at least 32 bytes".into(),
            ));
        }
        if self.nodes.is_empty() {
            return Err(WorkerError::Config(
                "at least one Lavalink node is required".into(),
            ));
        }
        if !self.allow_remote_bind && !self.bind_addr.ip().is_loopback() {
            return Err(WorkerError::Config(
                "remote bind requires explicit opt-in".into(),
            ));
        }
        if self.body_limit == 0
            || self.callback_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
            || self.gateway_command_timeout.is_zero()
            || self.plugin_event_capacity == 0
            || self.per_ip_request_limit == 0
            || self.limiter_table_capacity == 0
        {
            return Err(WorkerError::Config(
                "limits and timeouts must be non-zero".into(),
            ));
        }
        Ok(())
    }
}

pub struct SecretString(Box<str>);
impl SecretString {
    pub fn new(value: impl Into<Box<str>>) -> Self {
        Self(value.into())
    }
    pub(crate) fn expose_secret(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_builder() -> WorkerConfigBuilder {
        WorkerConfigBuilder::new(
            UserId(1),
            SecretString::new("bot-token"),
            SecretString::new("api-token-that-is-at-least-thirty-two-bytes"),
            vec![NodeConfig::new("localhost:2333", "password")],
        )
    }

    #[test]
    fn rejects_empty_bot_token() {
        let builder = WorkerConfigBuilder::new(
            UserId(1),
            SecretString::new(""),
            SecretString::new("api-token-that-is-at-least-thirty-two-bytes"),
            vec![NodeConfig::new("localhost:2333", "password")],
        );
        assert!(matches!(builder.build(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn rejects_empty_api_token() {
        let builder = WorkerConfigBuilder::new(
            UserId(1),
            SecretString::new("bot-token"),
            SecretString::new(""),
            vec![NodeConfig::new("localhost:2333", "password")],
        );
        assert!(matches!(builder.build(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn rejects_short_api_token() {
        let builder = WorkerConfigBuilder::new(
            UserId(1),
            SecretString::new("bot-token"),
            SecretString::new("short"),
            vec![NodeConfig::new("localhost:2333", "password")],
        );
        assert!(matches!(builder.build(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn rejects_empty_nodes() {
        let builder = WorkerConfigBuilder::new(
            UserId(1),
            SecretString::new("bot-token"),
            SecretString::new("api-token-that-is-at-least-thirty-two-bytes"),
            vec![],
        );
        assert!(matches!(builder.build(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn rejects_remote_bind_without_opt_in() {
        let builder = valid_builder().bind_addr("0.0.0.0:8080".parse().unwrap(), false);
        assert!(matches!(builder.build(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn rejects_zero_limiter_capacity() {
        let mut config = valid_builder().build().unwrap();
        config.limiter_table_capacity = 0;
        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn rejects_zero_limits_and_timeouts() {
        let mut config = valid_builder().build().unwrap();
        config.body_limit = 0;
        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
        config.body_limit = 1;
        config.callback_timeout = Duration::ZERO;
        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
        config.callback_timeout = Duration::from_secs(1);
        config.shutdown_timeout = Duration::ZERO;
        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
        config.shutdown_timeout = Duration::from_secs(1);
        config.gateway_command_timeout = Duration::ZERO;
        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
        config.gateway_command_timeout = Duration::from_secs(1);
        config.plugin_event_capacity = 0;
        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
        config.plugin_event_capacity = 1;
        config.per_ip_request_limit = 0;
        assert!(matches!(config.validate(), Err(WorkerError::Config(_))));
    }

    #[test]
    fn secret_debug_is_redacted() {
        let secret = SecretString::new("test-token-value");
        assert_eq!(format!("{secret:?}"), "SecretString([REDACTED])");
        assert!(!format!("{secret:?}").contains("test-token-value"));
    }
}
