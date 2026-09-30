use crate::{
    config::SecretString,
    error::WorkerError,
    gateway::{GatewayCommand, GatewayDriver, GatewayEvent, GatewayFuture},
};
use straight_rs_model::UserId;
use tokio::sync::{mpsc, watch};

pub struct TwilightGatewayDriver;
impl TwilightGatewayDriver {
    pub fn new() -> Self {
        Self
    }
}
impl Default for TwilightGatewayDriver {
    fn default() -> Self {
        Self::new()
    }
}
impl GatewayDriver for TwilightGatewayDriver {
    fn run<'a>(
        &'a self,
        _token: SecretString,
        _bot_user_id: UserId,
        _commands: mpsc::Receiver<GatewayCommand>,
        _events: mpsc::Sender<GatewayEvent>,
        _shutdown: watch::Receiver<bool>,
    ) -> GatewayFuture<'a> {
        Box::pin(async {
            Err(WorkerError::Gateway(
                "Twilight adapter is not implemented".into(),
            ))
        })
    }
}
