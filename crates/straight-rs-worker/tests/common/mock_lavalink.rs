use axum::{
    Router,
    extract::ws::{Message, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
};
use std::net::SocketAddr;

pub struct MockLavalink {
    addr: SocketAddr,
    _task: tokio::task::JoinHandle<()>,
}
impl MockLavalink {
    pub async fn start() -> Self {
        async fn ws(ws: WebSocketUpgrade) -> impl IntoResponse {
            ws.on_upgrade(|mut socket| async move {
                let ready = r#"{"op":"ready","resumed":false,"sessionId":"test-session"}"#;
                let _ = socket.send(Message::Text(ready.into())).await;
                while socket.recv().await.is_some() {}
            })
        }
        let app = Router::new().route("/v4/websocket", get(ws));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, _task: task }
    }
    pub fn host(&self) -> String {
        self.addr.to_string()
    }
}
