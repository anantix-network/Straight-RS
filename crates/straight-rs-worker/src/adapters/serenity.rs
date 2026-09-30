use crate::{
    config::SecretString,
    gateway::{GatewayCommand, GatewayDriver, GatewayEvent, GatewayFuture},
};
use straight_rs_model::UserId;
use tokio::sync::{mpsc, watch};

pub struct SerenityGatewayDriver;
impl SerenityGatewayDriver {
    pub fn new() -> Self {
        Self
    }
}
impl Default for SerenityGatewayDriver {
    fn default() -> Self {
        Self::new()
    }
}
impl GatewayDriver for SerenityGatewayDriver {
    fn run<'a>(
        &'a self,
        _token: SecretString,
        _bot_user_id: UserId,
        _commands: mpsc::Receiver<GatewayCommand>,
        _events: mpsc::Sender<GatewayEvent>,
        _shutdown: watch::Receiver<bool>,
    ) -> GatewayFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}
