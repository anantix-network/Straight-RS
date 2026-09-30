use axum::{
    Router,
    body::Bytes,
    extract::ws::{Message, WebSocketUpgrade},
    http::{Method, StatusCode},
    response::IntoResponse,
    routing::get,
};
use serde_json::Value;
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    pub body: Value,
}

pub struct MockLavalink {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<Recorded>>>,
    _task: tokio::task::JoinHandle<()>,
    ready_release: tokio::sync::watch::Sender<bool>,
}
impl MockLavalink {
    pub async fn start() -> Self {
        Self::start_inner(false).await
    }
    pub async fn start_paused() -> Self {
        Self::start_inner(true).await
    }
    async fn start_inner(paused: bool) -> Self {
        async fn ws_handler(
            upgrade: WebSocketUpgrade,
            mut release: tokio::sync::watch::Receiver<bool>,
        ) -> impl IntoResponse {
            upgrade.on_upgrade(move |mut socket| async move {
                if !*release.borrow() {
                    let _ = release.wait_for(|released| *released).await;
                }
                let ready = r#"{"op":"ready","resumed":false,"sessionId":"test-session"}"#;
                let _ = socket.send(Message::Text(ready.into())).await;
                while socket.recv().await.is_some() {}
            })
        }
        let (ready_release, ready_rx) = tokio::sync::watch::channel(!paused);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let app = Router::new().route("/v4/websocket", get(move |upgrade: WebSocketUpgrade| ws_handler(upgrade, ready_rx.clone()))).fallback(move |method: Method, uri: axum::http::Uri, body: Bytes| {
            let recorded = recorded.clone();
            async move {
                let value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                recorded.lock().unwrap().push(Recorded { method: method.to_string(), path: uri.path().to_owned(), body: value });
                if method == Method::PATCH { axum::Json(serde_json::json!({"guildId": "1", "track": null, "volume": 100, "paused": false, "state": {"time": 0, "position": 0, "connected": true, "ping": 1}, "voice": {"token": "", "endpoint": "", "sessionId": ""}, "filters": {}})).into_response() } else { StatusCode::NO_CONTENT.into_response() }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            addr,
            requests,
            _task: task,
            ready_release,
        }
    }
    pub fn release_ready(&self) -> tokio::sync::watch::Sender<bool> {
        self.ready_release.clone()
    }

    pub fn host(&self) -> String {
        self.addr.to_string()
    }
    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }
}
