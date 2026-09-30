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
    #[allow(dead_code)]
    load_body: Arc<Mutex<Value>>,
    load_count: Arc<std::sync::atomic::AtomicUsize>,
    frames: tokio::sync::broadcast::Sender<String>,
}

pub fn synthetic_track(encoded: &str) -> Value {
    serde_json::json!({
        "encoded": encoded,
        "info": {"identifier": "id1", "isSeekable": true, "author": "a", "length": 1000, "isStream": false, "position": 0, "title": "t", "uri": "https://example.test/t", "artworkUrl": null, "isrc": null, "sourceName": "test"},
        "pluginInfo": {"pluginSecret": "synthetic-plugin-secret"},
        "userData": {"userSecret": "synthetic-user-secret"}
    })
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
            mut frames: tokio::sync::broadcast::Receiver<String>,
        ) -> impl IntoResponse {
            upgrade.on_upgrade(move |mut socket| async move {
                if !*release.borrow() {
                    let _ = release.wait_for(|released| *released).await;
                }
                let ready = r#"{"op":"ready","resumed":false,"sessionId":"test-session"}"#;
                let _ = socket.send(Message::Text(ready.into())).await;
                loop {
                    tokio::select! {
                        incoming = socket.recv() => if incoming.is_none() { break },
                        frame = frames.recv() => match frame {
                            Ok(text) => { let _ = socket.send(Message::Text(text.into())).await; }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(_) => break,
                        },
                    }
                }
            })
        }
        let (frames_tx, _) = tokio::sync::broadcast::channel::<String>(64);
        let frames_handle = frames_tx.clone();
        let (ready_release, ready_rx) = tokio::sync::watch::channel(!paused);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let load_body = Arc::new(Mutex::new(Value::Null));
        let load_handle = load_body.clone();
        let load_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let load_counter = load_count.clone();
        let app = Router::new().route("/v4/websocket", get(move |upgrade: WebSocketUpgrade| ws_handler(upgrade, ready_rx.clone(), frames_tx.subscribe()))).fallback(move |method: Method, uri: axum::http::Uri, body: Bytes| {
            let recorded = recorded.clone();
            async move {
                let value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                recorded.lock().unwrap().push(Recorded { method: method.to_string(), path: uri.path().to_owned(), body: value.clone() });
                if method == Method::PATCH {
                    let track = value["track"]["encoded"].as_str().map(synthetic_track);
                    axum::Json(serde_json::json!({"guildId": "1", "track": track, "volume": 100, "paused": false, "state": {"time": 0, "position": 0, "connected": true, "ping": 1}, "voice": {"token": "synthetic-voice-token", "endpoint": "synthetic-voice-endpoint.example:443", "sessionId": "synthetic-voice-session"}, "filters": {}})).into_response()
                } else { StatusCode::NO_CONTENT.into_response() }
            }
        });
        let app = app.route(
            "/v4/loadtracks",
            get(move || {
                load_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = load_body.lock().unwrap().clone();
                async move { axum::Json(body) }
            }),
        );
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
            load_body: load_handle,
            load_count,
            frames: frames_handle,
        }
    }
    /// Push a synthetic websocket text frame to every connected client.
    #[allow(dead_code)]
    pub fn push_frame(&self, text: String) {
        let _ = self.frames.send(text);
    }
    #[allow(dead_code)]
    pub fn push_track_end(&self, guild: u64, encoded: &str, reason: &str) {
        self.push_frame(
            serde_json::json!({"op":"event","type":"TrackEndEvent","guildId":guild.to_string(),"track":synthetic_track(encoded),"reason":reason}).to_string(),
        );
    }
    #[allow(dead_code)]
    pub fn load_requests(&self) -> usize {
        self.load_count.load(std::sync::atomic::Ordering::SeqCst)
    }
    #[allow(dead_code)]
    pub fn set_load_body(&self, body: Value) {
        *self.load_body.lock().unwrap() = body;
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
