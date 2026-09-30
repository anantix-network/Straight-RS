#![allow(dead_code)]
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    pub query: String,
    pub body: Value,
}

#[derive(Clone, Debug)]
enum Push {
    Text(String),
    Close,
}

pub struct MockState {
    pub requests: Mutex<Vec<Recorded>>,
    pub ws_headers: Mutex<Vec<HeaderMap>>,
    pub ws_connections: AtomicUsize,
    pub resumed: AtomicBool,
    pub session_id: String,
    pub load_response: Mutex<Option<(u16, String)>>,
    pub fail_next_gets: AtomicU32,
    pub patch_delay_ms: AtomicU32,
    pub delete_delay_ms: AtomicU32,
    pub in_flight: AtomicU32,
    pub max_in_flight: AtomicU32,
    players: Mutex<HashMap<String, Value>>,
    push: broadcast::Sender<Push>,
}

pub struct Mock {
    pub addr: SocketAddr,
    pub state: Arc<MockState>,
    server: tokio::task::JoinHandle<()>,
    kill: tokio::sync::watch::Sender<bool>,
}

pub fn track_json(encoded: &str) -> Value {
    json!({"encoded": encoded, "info": {"identifier": encoded, "isSeekable": true, "author": "a",
        "length": 200000, "isStream": false, "position": 0, "title": "t", "uri": null,
        "artworkUrl": null, "isrc": null, "sourceName": "mock"}, "pluginInfo": {}, "userData": {}})
}

impl Mock {
    pub async fn start() -> Mock {
        Self::start_on("127.0.0.1:0", false).await
    }

    pub async fn start_on(addr: &str, resumed: bool) -> Mock {
        let (push, _) = broadcast::channel(64);
        let state = Arc::new(MockState {
            requests: Mutex::new(vec![]),
            ws_headers: Mutex::new(vec![]),
            ws_connections: AtomicUsize::new(0),
            resumed: AtomicBool::new(resumed),
            session_id: "mock-session".into(),
            load_response: Mutex::new(None),
            fail_next_gets: AtomicU32::new(0),
            patch_delay_ms: AtomicU32::new(0),
            delete_delay_ms: AtomicU32::new(0),
            in_flight: AtomicU32::new(0),
            max_in_flight: AtomicU32::new(0),
            players: Mutex::new(HashMap::new()),
            push,
        });
        let app = Router::new()
            .route("/v4/websocket", get(ws_handler))
            .fallback(rest_handler)
            .with_state(state.clone());
        let listener = loop {
            match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => break l,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };
        let addr = listener.local_addr().unwrap();
        let (kill, kill_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(accept_loop(listener, app, kill_rx));
        Mock { addr, state, server, kill }
    }

    pub fn host(&self) -> String {
        self.addr.to_string()
    }
    pub fn push_text(&self, s: impl Into<String>) {
        let _ = self.state.push.send(Push::Text(s.into()));
    }
    pub fn close_ws(&self) {
        let _ = self.state.push.send(Push::Close);
    }
    /// Stops accepting connections and drops the websocket.
    pub fn kill(&self) {
        let _ = self.kill.send(true);
        self.server.abort();
        self.close_ws();
    }
    pub fn set_resumed(&self, v: bool) {
        self.state.resumed.store(v, SeqCst);
    }
    pub fn requests(&self) -> Vec<Recorded> {
        self.state.requests.lock().unwrap().clone()
    }
    pub fn requests_matching(&self, method: &str, path_prefix: &str) -> Vec<Recorded> {
        self.requests()
            .into_iter()
            .filter(|r| r.method == method && r.path.starts_with(path_prefix))
            .collect()
    }
}

async fn accept_loop(listener: tokio::net::TcpListener, app: Router, kill: tokio::sync::watch::Receiver<bool>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        let svc = hyper_util::service::TowerToHyperService::new(app.clone());
        let mut kill = kill.clone();
        tokio::spawn(async move {
            let conn = hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                .with_upgrades();
            tokio::select! {
                _ = conn => {}
                _ = kill.wait_for(|k| *k) => {}
            }
        });
    }
}

struct InFlight<'a>(&'a AtomicU32);
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, SeqCst);
    }
}

async fn ws_handler(State(s): State<Arc<MockState>>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    s.ws_headers.lock().unwrap().push(headers);
    s.ws_connections.fetch_add(1, SeqCst);
    ws.on_upgrade(move |socket| ws_session(s, socket))
}

async fn ws_session(s: Arc<MockState>, mut socket: WebSocket) {
    let mut rx = s.push.subscribe();
    let ready = json!({"op": "ready", "resumed": s.resumed.load(SeqCst), "sessionId": s.session_id});
    if socket.send(Message::Text(ready.to_string())).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            m = rx.recv() => match m {
                Ok(Push::Text(t)) => { if socket.send(Message::Text(t)).await.is_err() { break; } }
                Ok(Push::Close) | Err(broadcast::error::RecvError::Closed) => break,
                Err(_) => {}
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(_)) => {}
                _ => break,
            },
        }
    }
}

fn merge_player(s: &MockState, gid: &str, body: &Value) -> Value {
    let mut players = s.players.lock().unwrap();
    let p = players.entry(gid.to_string()).or_insert_with(|| {
        json!({"guildId": gid, "track": null, "volume": 100, "paused": false,
            "state": {"time": 0, "position": 0, "connected": true, "ping": 1},
            "voice": {"token": "", "endpoint": "", "sessionId": ""}, "filters": {}})
    });
    if let Some(t) = body.get("track") {
        p["track"] = match t.get("encoded") {
            Some(Value::String(e)) => track_json(e),
            Some(Value::Null) => Value::Null,
            _ => p["track"].clone(),
        };
    }
    if let Some(v) = body.get("volume") { p["volume"] = v.clone(); }
    if let Some(v) = body.get("paused") { p["paused"] = v.clone(); }
    if let Some(v) = body.get("position") { p["state"]["position"] = v.clone(); }
    if let Some(v) = body.get("filters") { p["filters"] = v.clone(); }
    p.clone()
}

async fn rest_handler(State(s): State<Arc<MockState>>, method: Method, uri: Uri, body: Bytes) -> Response {
    let path = uri.path().to_string();
    let body_json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    s.requests.lock().unwrap().push(Recorded {
        method: method.to_string(),
        path: path.clone(),
        query: uri.query().unwrap_or("").to_string(),
        body: body_json.clone(),
    });
    if method == Method::GET && s.fail_next_gets.load(SeqCst) > 0 {
        s.fail_next_gets.fetch_sub(1, SeqCst);
        return (StatusCode::SERVICE_UNAVAILABLE, "unavailable").into_response();
    }
    let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match (method.as_str(), segs.as_slice()) {
        ("PATCH", ["v4", "sessions", _, "players", gid]) => {
            let now = s.in_flight.fetch_add(1, SeqCst) + 1;
            let _guard = InFlight(&s.in_flight);
            s.max_in_flight.fetch_max(now, SeqCst);
            let delay = s.patch_delay_ms.load(SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(u64::from(delay))).await;
            }
            let out = merge_player(&s, gid, &body_json);
            Json(out).into_response()
        }
        ("DELETE", ["v4", "sessions", _, "players", gid]) => {
            let delay = s.delete_delay_ms.load(SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(u64::from(delay))).await;
            }
            s.players.lock().unwrap().remove(*gid);
            StatusCode::NO_CONTENT.into_response()
        }
        ("PATCH", ["v4", "sessions", _]) => Json(json!({
            "resuming": body_json.get("resuming").and_then(Value::as_bool).unwrap_or(false),
            "timeout": body_json.get("timeout").and_then(Value::as_u64).unwrap_or(60)})).into_response(),
        ("GET", ["v4", "loadtracks"]) => match s.load_response.lock().unwrap().clone() {
            Some((code, text)) => (StatusCode::from_u16(code).unwrap(), text).into_response(),
            None => Json(json!({"loadType": "empty", "data": {}})).into_response(),
        },
        ("GET", ["v4", "decodetrack"]) => Json(track_json("QAAA")).into_response(),
        ("POST", ["v4", "decodetracks"]) => Json(json!([track_json("QAAA")])).into_response(),
        ("GET", ["version"]) => "4.0.8".into_response(),
        ("GET", ["v4", "info"]) => Json(json!({"version": {"semver": "4.0.8", "major": 4, "minor": 0, "patch": 8,
            "preRelease": null, "build": null}, "buildTime": 1, "git": {"branch": "m", "commit": "c", "commitTime": 1},
            "jvm": "17", "lavaplayer": "2", "sourceManagers": ["youtube"], "filters": ["volume"], "plugins": []})).into_response(),
        ("GET", ["v4", "stats"]) => Json(json!({"players": 3, "playingPlayers": 1, "uptime": 9,
            "memory": {"free": 1, "used": 1, "allocated": 1, "reservable": 1},
            "cpu": {"cores": 2, "systemLoad": 0.0, "lavalinkLoad": 0.0}, "frameStats": null})).into_response(),
        ("GET", ["v4", "routeplanner", "status"]) => Json(json!({"class": null, "details": null})).into_response(),
        ("POST", ["v4", "routeplanner", "free", "address"]) | ("POST", ["v4", "routeplanner", "free", "all"]) => {
            StatusCode::NO_CONTENT.into_response()
        }
        _ => (StatusCode::NOT_FOUND, Json(json!({"timestamp": 1, "status": 404, "error": "Not Found",
            "message": "Session not found", "path": path}))).into_response(),
    }
}

pub async fn eventually(timeout: Duration, mut f: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while !f() {
        assert!(start.elapsed() < timeout, "condition not met within {timeout:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

use larplink::{Event, LavalinkClient, NodeConfig, UserId};
use tokio::sync::broadcast::error::RecvError;

pub async fn client(mocks: &[&Mock]) -> LavalinkClient {
    client_with(mocks, |_| {}).await
}

pub async fn client_with(mocks: &[&Mock], tweak: impl Fn(&mut NodeConfig)) -> LavalinkClient {
    let mut b = LavalinkClient::builder(UserId(1));
    for m in mocks {
        let mut cfg = NodeConfig::new(m.host(), "pw");
        cfg.request_timeout = Duration::from_secs(3);
        tweak(&mut cfg);
        b = b.node(cfg);
    }
    let c = b.build().await.unwrap();
    c.wait_ready(Duration::from_secs(5)).await.unwrap();
    eventually(Duration::from_secs(5), || c.nodes().iter().all(|n| n.is_ready())).await;
    c
}

pub async fn next_event(rx: &mut broadcast::Receiver<Event>, pred: impl Fn(&Event) -> bool) -> Event {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await {
                Ok(e) if pred(&e) => return e,
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => panic!("event channel closed"),
            }
        }
    })
    .await
    .expect("timed out waiting for event")
}

pub fn sample_track(encoded: &str) -> larplink::Track {
    serde_json::from_value(track_json(encoded)).unwrap()
}

pub const STATS_10: &str = r#"{"op":"stats","players":10,"playingPlayers":5,"uptime":1000,"memory":{"free":1,"used":2,"allocated":3,"reservable":4},"cpu":{"cores":4,"systemLoad":0.0,"lavalinkLoad":0.0},"frameStats":null}"#;
