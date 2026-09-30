#[cfg(feature = "serenity")]
pub mod serenity;
pub mod twilight;

#[cfg(test)]
mod tests {
    use crate::adapters::twilight;
    use crate::config::SecretString;
    use crate::error::WorkerError;
    use crate::gateway::GatewayDriver;
    use straight_rs_model::UserId;
    use tokio::sync::{mpsc, watch};

    #[tokio::test]
    async fn twilight_stub_fails_with_typed_gateway_error() {
        let (_commands_tx, commands_rx) = mpsc::channel(1);
        let (events_tx, _events_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let result = twilight::TwilightGatewayDriver::new()
            .run(
                SecretString::new("token"),
                UserId(1),
                commands_rx,
                events_tx,
                shutdown_rx,
            )
            .await;
        assert!(
            matches!(result, Err(WorkerError::Gateway(message)) if message.contains("not implemented"))
        );
    }
}
