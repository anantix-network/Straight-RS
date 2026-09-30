# Straight-RS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `straight-rs`, a fast, correct and complete Lavalink v4 client library in Rust with a library-agnostic Discord voice interface.

**Architecture:** Workspace of two crates. `straight-rs-model` holds serde types for every Lavalink v4 payload. `straight-rs` runs one tokio task per node (WebSocket, reconnect, resume), a lock-free player state (`ArcSwap` snapshot) per guild, a node pool with pluggable balancing, failover, and optional `serenity`/`twilight`/`songbird` adapters.

**Tech Stack:** Rust 2021 (MSRV 1.80), tokio, hyper 1 + hyper-util (REST), tokio-tungstenite 0.24 (WS), serde, dashmap, arc-swap, thiserror, tracing; tests use axum 0.7 as a mock Lavalink; benches use criterion.

**Spec:** `docs/superpowers/specs/2026-09-30-straight-rs-design.md`

## Global Constraints

- Lavalink **v4 only** (REST `/v4/...`, WS `/v4/websocket`, `GET /version`). No v3.
- Core has **no Discord library dependency**; `serenity`, `twilight`, `songbird` are optional feature flags containing only conversion code.
- Runtime is `tokio`. MSRV **1.80**.
- No `unwrap`/`expect`/`panic!` in library code (tests may). Poisoned std mutexes are recovered with `into_inner`.
- `straight_rs::Error` is `thiserror`-based and `#[non_exhaustive]`.
- Guild/channel/user IDs are JSON **strings** on the wire; deserialization must also accept numbers; `u64` precision must be kept.
- Player writes for one guild are strictly ordered (never two in-flight PATCHes for one guild).
- Slow event receivers get `RecvError::Lagged`; the hot path never blocks.
- Client-Name header is `straight-rs/<crate version>`.
- Queue/autoplay, Lavalink v3, and other Discord libraries are out of scope.
- Deviations from the spec text that the plan makes on purpose: (a) voice update methods are `async fn` (they issue a REST call); (b) per-player ordering uses a fair `tokio::sync::Mutex` gate instead of an mpsc+task (same guarantee, less code); (c) `Error` also has `HttpClient`, `Gateway`, `Config` variants.

## Review Focus

1. Unknown WS `op`, unknown event `type`, non-JSON or malformed frames must never kill a node or panic (become `Unknown` events / log lines). Pinned in Task 2 and Task 6.
2. Identifiers with special characters (`ytsearch:foo bar & baz?`) must be percent-encoded in `loadtracks`. Pinned in Task 4.
3. Non-JSON error bodies (proxy 502 HTML) and Lavalink JSON errors must map to `Error::Lavalink` with a useful message; GET retries on 502/503/504. Pinned in Task 4.
4. Snowflake IDs above 2^53 and IDs given as JSON numbers or strings. Pinned in Task 1.
5. `VOICE_SERVER_UPDATE` arriving before `VOICE_STATE_UPDATE`, null endpoint, repeated identical updates. Pinned in Task 5 and Task 8.

---

## File Structure

```
Cargo.toml
straight-rs-model/
  Cargo.toml
  src/{lib,id,exception,track,player,filters,stats,info,routeplanner,session,message}.rs
  tests/model.rs
straight-rs/
  Cargo.toml
  src/{lib,error,config,backoff,balancer,position,rest,event,voice,gateway,state,node,hub,client,player}.rs
  src/adapters/{mod,twilight,serenity,songbird}.rs
  tests/common/mod.rs        (mock Lavalink server + helpers)
  tests/{rest,node,player,voice,failover}.rs
  tests/e2e.rs
  benches/core.rs
.github/workflows/ci.yml
README.md
```

---

### Task 1: Workspace, IDs, Track and LoadResult models

**Files:**
- Create: `Cargo.toml`, `straight-rs-model/Cargo.toml`, `straight-rs-model/src/{lib,id,exception,track}.rs`, `straight-rs-model/tests/model.rs`

**Interfaces:**
- Produces: `GuildId(pub u64)`, `ChannelId(pub u64)`, `UserId(pub u64)` (Copy, Hash, Ord, Display, `From<u64>`, serialize as string, deserialize from string or number); `Severity`, `Exception`; `Track { encoded: Arc<str>, info: TrackInfo, plugin_info: Value, user_data: Value }`; `TrackInfo`; `Playlist`, `PlaylistInfo`, `NoMatches`; `LoadResult::{Track, Playlist, Search, Empty, Error}`.

- [ ] **Step 1: git init and workspace files**

```bash
cd /home/arizkami/yuuma/straight-rs && git init
```

`Cargo.toml`:
```toml
[workspace]
resolver = "2"
members = ["straight-rs-model", "straight-rs"]

[workspace.package]
version = "0.1.0"
edition = "2021"
rust-version = "1.80"
license = "MIT"

[workspace.dependencies]
serde = { version = "1", features = ["derive", "rc"] }
serde_json = "1"
thiserror = "1"
tracing = "0.1"
tokio = { version = "1", features = ["rt", "macros", "sync", "time", "net"] }
tokio-tungstenite = "0.24"
futures-util = { version = "0.3", default-features = false, features = ["sink", "std"] }
hyper = { version = "1", features = ["http1", "client"] }
hyper-util = { version = "0.1", features = ["client-legacy", "http1", "tokio"] }
http-body-util = "0.1"
bytes = "1"
dashmap = "6"
arc-swap = "1"
percent-encoding = "2"
rand = "0.8"
straight-rs-model = { path = "straight-rs-model", version = "0.1.0" }
```

`.gitignore`: `/target`

`straight-rs-model/Cargo.toml`:
```toml
[package]
name = "straight-rs-model"
description = "Lavalink v4 protocol types for straight-rs"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
serde.workspace = true
serde_json.workspace = true
```

- [ ] **Step 2: Write the failing tests** — `straight-rs-model/tests/model.rs`

```rust
use straight_rs_model::*;
use serde_json::{json, Value};

pub const TRACK: &str = r#"{"encoded":"QAAA","info":{"identifier":"dQw4w9WgXcQ","isSeekable":true,"author":"RickAstleyVEVO","length":212000,"isStream":false,"position":0,"title":"Never Gonna Give You Up","uri":"https://www.youtube.com/watch?v=dQw4w9WgXcQ","artworkUrl":"https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg","isrc":null,"sourceName":"youtube"},"pluginInfo":{},"userData":{}}"#;

#[test]
fn ids_accept_string_or_number_and_keep_precision() {
    let g: GuildId = serde_json::from_str("\"18446744073709551615\"").unwrap();
    assert_eq!(g.0, u64::MAX);
    let g: GuildId = serde_json::from_str("123").unwrap();
    assert_eq!(g, GuildId(123));
    assert_eq!(serde_json::to_string(&GuildId(5)).unwrap(), "\"5\"");
    assert!(serde_json::from_str::<GuildId>("\"abc\"").is_err());
}

#[test]
fn track_roundtrips_exactly() {
    let t: Track = serde_json::from_str(TRACK).unwrap();
    assert_eq!(&*t.encoded, "QAAA");
    assert_eq!(t.info.length, 212000);
    assert!(t.info.is_seekable && !t.info.is_stream);
    assert_eq!(t.info.isrc, None);
    assert_eq!(
        serde_json::to_value(&t).unwrap(),
        serde_json::from_str::<Value>(TRACK).unwrap()
    );
}

#[test]
fn track_keeps_plugin_info_and_user_data() {
    let mut v: Value = serde_json::from_str(TRACK).unwrap();
    v["pluginInfo"] = json!({"albumName": "x"});
    v["userData"] = json!({"requester": 1});
    let t: Track = serde_json::from_value(v.clone()).unwrap();
    assert_eq!(t.plugin_info["albumName"], "x");
    assert_eq!(serde_json::to_value(&t).unwrap(), v);
}

#[test]
fn load_result_variants() {
    let track = format!(r#"{{"loadType":"track","data":{TRACK}}}"#);
    assert!(matches!(serde_json::from_str::<LoadResult>(&track).unwrap(), LoadResult::Track(_)));

    let search = format!(r#"{{"loadType":"search","data":[{TRACK},{TRACK}]}}"#);
    match serde_json::from_str::<LoadResult>(&search).unwrap() {
        LoadResult::Search(v) => assert_eq!(v.len(), 2),
        other => panic!("{other:?}"),
    }

    let pl = format!(
        r#"{{"loadType":"playlist","data":{{"info":{{"name":"Mix","selectedTrack":-1}},"pluginInfo":{{}},"tracks":[{TRACK}]}}}}"#
    );
    match serde_json::from_str::<LoadResult>(&pl).unwrap() {
        LoadResult::Playlist(p) => {
            assert_eq!(p.info.name, "Mix");
            assert_eq!(p.info.selected_track, -1);
            assert_eq!(p.tracks.len(), 1);
        }
        other => panic!("{other:?}"),
    }

    assert!(matches!(
        serde_json::from_str::<LoadResult>(r#"{"loadType":"empty","data":{}}"#).unwrap(),
        LoadResult::Empty(_)
    ));

    let err = r#"{"loadType":"error","data":{"message":"nope","severity":"fault","cause":"boom"}}"#;
    match serde_json::from_str::<LoadResult>(err).unwrap() {
        LoadResult::Error(e) => {
            assert_eq!(e.severity, Severity::Fault);
            assert_eq!(e.message.as_deref(), Some("nope"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn unknown_severity_does_not_fail() {
    let e: Exception = serde_json::from_str(r#"{"message":null,"severity":"weird","cause":""}"#).unwrap();
    assert_eq!(e.severity, Severity::Unknown);
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p straight-rs-model` (create `straight-rs-model/src/lib.rs` empty first)
Expected: FAIL — unresolved imports (`GuildId`, `Track`, …).

- [ ] **Step 4: Implement**

`straight-rs-model/src/lib.rs`:
```rust
//! Lavalink v4 protocol types.
pub mod exception;
pub mod id;
pub mod track;

pub use exception::*;
pub use id::*;
pub use track::*;
```

`id.rs`:
```rust
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

macro_rules! id_type {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub u64);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
        }
        impl From<u64> for $name {
            fn from(v: u64) -> Self { Self(v) }
        }
        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(&self.0)
            }
        }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct V;
                impl<'de> Visitor<'de> for V {
                    type Value = $name;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("a snowflake as string or unsigned integer")
                    }
                    fn visit_u64<E: de::Error>(self, v: u64) -> Result<$name, E> { Ok($name(v)) }
                    fn visit_i64<E: de::Error>(self, v: i64) -> Result<$name, E> {
                        u64::try_from(v).map($name).map_err(E::custom)
                    }
                    fn visit_str<E: de::Error>(self, v: &str) -> Result<$name, E> {
                        v.parse().map($name).map_err(E::custom)
                    }
                }
                d.deserialize_any(V)
            }
        }
    };
}

id_type!(/// Discord guild id.
    GuildId);
id_type!(/// Discord channel id.
    ChannelId);
id_type!(/// Discord user id.
    UserId);
```

`exception.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Common,
    Suspicious,
    Fault,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Exception {
    #[serde(default)]
    pub message: Option<String>,
    pub severity: Severity,
    #[serde(default)]
    pub cause: String,
    #[serde(default)]
    pub cause_stack_trace: Option<String>,
}
```

`track.rs`:
```rust
use crate::exception::Exception;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Track {
    pub encoded: Arc<str>,
    pub info: TrackInfo,
    #[serde(default)]
    pub plugin_info: Value,
    #[serde(default)]
    pub user_data: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackInfo {
    pub identifier: String,
    pub is_seekable: bool,
    pub author: String,
    pub length: u64,
    pub is_stream: bool,
    pub position: u64,
    pub title: String,
    pub uri: Option<String>,
    pub artwork_url: Option<String>,
    pub isrc: Option<String>,
    pub source_name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaylistInfo {
    pub name: String,
    pub selected_track: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Playlist {
    pub info: PlaylistInfo,
    #[serde(default)]
    pub plugin_info: Value,
    pub tracks: Vec<Track>,
}

/// Payload of `loadType: "empty"` (always `{}`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NoMatches {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "loadType", content = "data", rename_all = "camelCase")]
pub enum LoadResult {
    Track(Track),
    Playlist(Playlist),
    Search(Vec<Track>),
    Empty(NoMatches),
    Error(Exception),
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p straight-rs-model`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(model): workspace, ids, track and load result types"
```

---

### Task 2: Remaining model types and WebSocket messages

**Files:**
- Create: `straight-rs-model/src/{player,filters,stats,info,routeplanner,session,message}.rs`
- Modify: `straight-rs-model/src/lib.rs`, `straight-rs-model/tests/model.rs` (append)

**Interfaces:**
- Consumes: `GuildId`, `ChannelId`, `Track`, `Exception` from Task 1.
- Produces: `Player`, `PlayerState { time, position: u64, connected: bool, ping: i64 }`, `VoiceState { token, endpoint, session_id: String, channel_id: Option<ChannelId> }`, `UpdatePlayer`, `UpdateTrack`, `Filters` (+ `EqBand`, `Karaoke`, `Timescale`, `Tremolo`, `Vibrato`, `Rotation`, `Distortion`, `ChannelMix`, `LowPass`), `Stats { players: u32, playing_players: u32, uptime, memory, cpu: Cpu { system_load: f64, .. }, frame_stats: Option<FrameStats { sent, nulled, deficit: i64 }> }`, `Info`, `RoutePlannerStatus`, `Session`, `SessionUpdate`, `Ready`, `PlayerUpdate`, `TrackEndReason` (+ `may_start_next()`), `Event` (model, with `guild_id()`), `WsMessage::{Ready, PlayerUpdate, Stats, Event, Unknown{op, payload}}` with `WsMessage::parse(&str) -> Result<WsMessage, serde_json::Error>`, `RestError`.

- [ ] **Step 1: Append failing tests to `straight-rs-model/tests/model.rs`**

```rust
#[test]
fn update_player_serializes_only_set_fields() {
    let u = UpdatePlayer { volume: Some(50), ..Default::default() };
    assert_eq!(serde_json::to_value(&u).unwrap(), json!({"volume": 50}));

    let stop = UpdatePlayer {
        track: Some(UpdateTrack { encoded: Some(None), ..Default::default() }),
        end_time: Some(None),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&stop).unwrap(),
        json!({"track": {"encoded": null}, "endTime": null})
    );
}

#[test]
fn player_response_parses() {
    let v = json!({"guildId":"1","track":null,"volume":100,"paused":false,
        "state":{"time":1,"position":2,"connected":true,"ping":-1},
        "voice":{"token":"t","endpoint":"e","sessionId":"s","channelId":"9"},
        "filters":{"volume":1.0,"equalizer":[{"band":0,"gain":0.25}],"pluginFilters":{"x":{"a":1}}}});
    let p: Player = serde_json::from_value(v).unwrap();
    assert_eq!(p.guild_id, GuildId(1));
    assert_eq!(p.state.ping, -1);
    assert_eq!(p.voice.channel_id, Some(ChannelId(9)));
    assert_eq!(p.filters.equalizer.unwrap()[0].gain, 0.25);
    assert!(p.filters.plugin_filters.unwrap().contains_key("x"));
}

#[test]
fn filters_all_variants_roundtrip() {
    let v = json!({"volume":1.0,"equalizer":[{"band":1,"gain":0.5}],
      "karaoke":{"level":1.0,"monoLevel":1.0,"filterBand":220.0,"filterWidth":100.0},
      "timescale":{"speed":1.0,"pitch":1.0,"rate":1.0},
      "tremolo":{"frequency":2.0,"depth":0.5},"vibrato":{"frequency":2.0,"depth":0.5},
      "rotation":{"rotationHz":0.2},
      "distortion":{"sinOffset":0.0,"sinScale":1.0,"cosOffset":0.0,"cosScale":1.0,"tanOffset":0.0,"tanScale":1.0,"offset":0.0,"scale":1.0},
      "channelMix":{"leftToLeft":1.0,"leftToRight":0.0,"rightToLeft":0.0,"rightToRight":1.0},
      "lowPass":{"smoothing":20.0},"pluginFilters":{"p":{}}});
    let f: Filters = serde_json::from_value(v.clone()).unwrap();
    assert_eq!(serde_json::to_value(&f).unwrap(), v);
}

#[test]
fn stats_with_and_without_frame_stats() {
    let base = |fs: &str| format!(r#"{{"players":1,"playingPlayers":1,"uptime":5,"memory":{{"free":1,"used":2,"allocated":3,"reservable":4}},"cpu":{{"cores":4,"systemLoad":0.5,"lavalinkLoad":0.1}},"frameStats":{fs}}}"#);
    let s: Stats = serde_json::from_str(&base(r#"{"sent":6000,"nulled":10,"deficit":-3}"#)).unwrap();
    assert_eq!(s.frame_stats.unwrap().deficit, -3);
    let s: Stats = serde_json::from_str(&base("null")).unwrap();
    assert!(s.frame_stats.is_none());
    let mut v: Value = serde_json::from_str(&base("null")).unwrap();
    v.as_object_mut().unwrap().remove("frameStats");
    assert!(serde_json::from_value::<Stats>(v).unwrap().frame_stats.is_none());
}

#[test]
fn info_and_routeplanner_parse() {
    let info = json!({"version":{"semver":"4.0.8","major":4,"minor":0,"patch":8,"preRelease":null,"build":null},
      "buildTime":1,"git":{"branch":"main","commit":"abc","commitTime":2},"jvm":"17","lavaplayer":"2.2.2",
      "sourceManagers":["youtube"],"filters":["volume"],"plugins":[{"name":"p","version":"1"}]});
    let i: Info = serde_json::from_value(info).unwrap();
    assert_eq!(i.version.major, 4);
    assert_eq!(i.plugins[0].name, "p");
    let rp: RoutePlannerStatus = serde_json::from_value(json!({"class":null,"details":null})).unwrap();
    assert!(rp.class.is_none());
    let rp: RoutePlannerStatus = serde_json::from_value(json!({"class":"RotatingIpRoutePlanner","details":{
      "ipBlock":{"type":"Inet6Address","size":"1"},"failingAddresses":[{"failingAddress":"a","failingTimestamp":1,"failingTime":"t"}],
      "rotateIndex":"0","ipIndex":"0","currentAddress":"a","blockIndex":"0","currentAddressIndex":"0"}})).unwrap();
    assert_eq!(rp.details.unwrap().failing_addresses.len(), 1);
}

const EV_TRACK: &str = TRACK;

#[test]
fn ws_known_messages() {
    match WsMessage::parse(r#"{"op":"ready","resumed":true,"sessionId":"abc"}"#).unwrap() {
        WsMessage::Ready(r) => assert!(r.resumed && r.session_id == "abc"),
        o => panic!("{o:?}"),
    }
    match WsMessage::parse(r#"{"op":"playerUpdate","guildId":"18446744073709551615","state":{"time":1,"position":2,"connected":true,"ping":3}}"#).unwrap() {
        WsMessage::PlayerUpdate(u) => assert_eq!(u.guild_id.0, u64::MAX),
        o => panic!("{o:?}"),
    }
    let end = format!(r#"{{"op":"event","type":"TrackEndEvent","guildId":"1","track":{EV_TRACK},"reason":"loadFailed"}}"#);
    match WsMessage::parse(&end).unwrap() {
        WsMessage::Event(Event::TrackEnd { guild_id, reason, .. }) => {
            assert_eq!(guild_id, GuildId(1));
            assert_eq!(reason, TrackEndReason::LoadFailed);
            assert!(reason.may_start_next());
            assert!(!TrackEndReason::Replaced.may_start_next());
        }
        o => panic!("{o:?}"),
    }
    let exc = format!(r#"{{"op":"event","type":"TrackExceptionEvent","guildId":"1","track":{EV_TRACK},"exception":{{"message":"boom","severity":"fault","cause":"x"}}}}"#);
    assert!(matches!(WsMessage::parse(&exc).unwrap(), WsMessage::Event(Event::TrackException { .. })));
    let stuck = format!(r#"{{"op":"event","type":"TrackStuckEvent","guildId":"1","track":{EV_TRACK},"thresholdMs":10000}}"#);
    assert!(matches!(WsMessage::parse(&stuck).unwrap(), WsMessage::Event(Event::TrackStuck { threshold_ms: 10000, .. })));
    let closed = r#"{"op":"event","type":"WebSocketClosedEvent","guildId":"1","code":4006,"reason":"x","byRemote":true}"#;
    assert!(matches!(WsMessage::parse(closed).unwrap(), WsMessage::Event(Event::WebSocketClosed { code: 4006, by_remote: true, .. })));
    let start = format!(r#"{{"op":"event","type":"TrackStartEvent","guildId":"1","track":{EV_TRACK}}}"#);
    assert!(matches!(WsMessage::parse(&start).unwrap(), WsMessage::Event(Event::TrackStart { .. })));
}

#[test]
fn ws_unknown_and_malformed_never_error_unless_not_json() {
    match WsMessage::parse(r#"{"op":"weird","x":1}"#).unwrap() {
        WsMessage::Unknown { op, payload } => { assert_eq!(op, "weird"); assert_eq!(payload["x"], 1); }
        o => panic!("{o:?}"),
    }
    match WsMessage::parse(r#"{"op":"event","type":"NewEvent","guildId":"1"}"#).unwrap() {
        WsMessage::Unknown { op, .. } => assert_eq!(op, "event"),
        o => panic!("{o:?}"),
    }
    // known op, malformed body -> Unknown, not an error
    assert!(matches!(WsMessage::parse(r#"{"op":"playerUpdate"}"#).unwrap(), WsMessage::Unknown { .. }));
    assert!(matches!(WsMessage::parse("[1,2]").unwrap(), WsMessage::Unknown { .. }));
    assert!(WsMessage::parse("not json").is_err());
}

#[test]
fn rest_error_body() {
    let e: RestError = serde_json::from_str(r#"{"timestamp":1,"status":404,"error":"Not Found","message":"Session not found","path":"/v4/x"}"#).unwrap();
    assert_eq!(e.status, 404);
    let e: RestError = serde_json::from_str(r#"{"status":400,"error":"Bad Request","path":"/v4/x"}"#).unwrap();
    assert_eq!(e.message, "");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p straight-rs-model`
Expected: FAIL — unresolved imports (`UpdatePlayer`, `WsMessage`, …).

- [ ] **Step 3: Implement**

`lib.rs` — add:
```rust
pub mod filters;
pub mod info;
pub mod message;
pub mod player;
pub mod routeplanner;
pub mod session;
pub mod stats;

pub use filters::*;
pub use info::*;
pub use message::*;
pub use player::*;
pub use routeplanner::*;
pub use session::*;
pub use stats::*;
```

`player.rs`:
```rust
use crate::{ChannelId, Filters, GuildId, Track};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Player {
    pub guild_id: GuildId,
    pub track: Option<Track>,
    pub volume: u16,
    pub paused: bool,
    pub state: PlayerState,
    pub voice: VoiceState,
    pub filters: Filters,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PlayerState {
    pub time: u64,
    pub position: u64,
    pub connected: bool,
    pub ping: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoiceState {
    pub token: String,
    pub endpoint: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_id: Option<ChannelId>,
}

/// Body of `PATCH /v4/sessions/{id}/players/{guild}`.
/// `Some(None)` on a nested option serializes as JSON `null` (used to stop / clear).
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdatePlayer {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track: Option<UpdateTrack>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Filters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<VoiceState>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTrack {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoded: Option<Option<Arc<str>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_data: Option<Value>,
}
```

`filters.rs`:
```rust
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

macro_rules! opt_struct {
    ($(#[$m:meta])* $name:ident { $($field:ident),* $(,)? }) => {
        $(#[$m])*
        #[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct $name {
            $( #[serde(default, skip_serializing_if = "Option::is_none")] pub $field: Option<f32>, )*
        }
    };
}

opt_struct!(Karaoke { level, mono_level, filter_band, filter_width });
opt_struct!(Timescale { speed, pitch, rate });
opt_struct!(Tremolo { frequency, depth });
opt_struct!(Vibrato { frequency, depth });
opt_struct!(Rotation { rotation_hz });
opt_struct!(Distortion { sin_offset, sin_scale, cos_offset, cos_scale, tan_offset, tan_scale, offset, scale });
opt_struct!(ChannelMix { left_to_left, left_to_right, right_to_left, right_to_right });
opt_struct!(LowPass { smoothing });

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct EqBand {
    pub band: u8,
    pub gain: f32,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Filters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub equalizer: Option<Vec<EqBand>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub karaoke: Option<Karaoke>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timescale: Option<Timescale>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tremolo: Option<Tremolo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vibrato: Option<Vibrato>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation: Option<Rotation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distortion: Option<Distortion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_mix: Option<ChannelMix>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub low_pass: Option<LowPass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin_filters: Option<Map<String, Value>>,
}
```

`stats.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    pub players: u32,
    pub playing_players: u32,
    pub uptime: u64,
    pub memory: Memory,
    pub cpu: Cpu,
    #[serde(default)]
    pub frame_stats: Option<FrameStats>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Memory {
    pub free: u64,
    pub used: u64,
    pub allocated: u64,
    pub reservable: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cpu {
    pub cores: u32,
    pub system_load: f64,
    pub lavalink_load: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameStats {
    pub sent: i64,
    pub nulled: i64,
    pub deficit: i64,
}
```

`info.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Info {
    pub version: Version,
    pub build_time: u64,
    pub git: Git,
    pub jvm: String,
    pub lavaplayer: String,
    pub source_managers: Vec<String>,
    pub filters: Vec<String>,
    pub plugins: Vec<PluginMeta>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Version {
    pub semver: String,
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub pre_release: Option<String>,
    pub build: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Git {
    pub branch: String,
    pub commit: String,
    pub commit_time: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PluginMeta {
    pub name: String,
    pub version: String,
}
```

`routeplanner.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RoutePlannerStatus {
    pub class: Option<String>,
    pub details: Option<RoutePlannerDetails>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutePlannerDetails {
    pub ip_block: Option<IpBlock>,
    #[serde(default)]
    pub failing_addresses: Vec<FailingAddress>,
    pub rotate_index: Option<String>,
    pub ip_index: Option<String>,
    pub current_address: Option<String>,
    pub current_address_index: Option<String>,
    pub block_index: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IpBlock {
    #[serde(rename = "type")]
    pub kind: String,
    pub size: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailingAddress {
    pub failing_address: String,
    pub failing_timestamp: u64,
    pub failing_time: String,
}
```

`session.rs`:
```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub resuming: bool,
    pub timeout: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct SessionUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resuming: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
}
```

`message.rs`:
```rust
use crate::{Exception, GuildId, PlayerState, Stats, Track};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ready {
    pub resumed: bool,
    pub session_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlayerUpdate {
    pub guild_id: GuildId,
    pub state: PlayerState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TrackEndReason {
    Finished,
    LoadFailed,
    Stopped,
    Replaced,
    Cleanup,
}

impl TrackEndReason {
    /// Whether the next track in a queue may be started.
    pub fn may_start_next(self) -> bool {
        matches!(self, Self::Finished | Self::LoadFailed)
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    #[serde(rename = "TrackStartEvent", rename_all = "camelCase")]
    TrackStart { guild_id: GuildId, track: Track },
    #[serde(rename = "TrackEndEvent", rename_all = "camelCase")]
    TrackEnd { guild_id: GuildId, track: Track, reason: TrackEndReason },
    #[serde(rename = "TrackExceptionEvent", rename_all = "camelCase")]
    TrackException { guild_id: GuildId, track: Track, exception: Exception },
    #[serde(rename = "TrackStuckEvent", rename_all = "camelCase")]
    TrackStuck { guild_id: GuildId, track: Track, threshold_ms: u64 },
    #[serde(rename = "WebSocketClosedEvent", rename_all = "camelCase")]
    WebSocketClosed { guild_id: GuildId, code: u16, reason: String, by_remote: bool },
}

impl Event {
    pub fn guild_id(&self) -> GuildId {
        match self {
            Self::TrackStart { guild_id, .. }
            | Self::TrackEnd { guild_id, .. }
            | Self::TrackException { guild_id, .. }
            | Self::TrackStuck { guild_id, .. }
            | Self::WebSocketClosed { guild_id, .. } => *guild_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum WsMessage {
    Ready(Ready),
    PlayerUpdate(PlayerUpdate),
    Stats(Stats),
    Event(Event),
    /// Unknown op / unknown event type / malformed known message (plugins).
    Unknown { op: String, payload: Value },
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Known {
    Ready(Ready),
    PlayerUpdate(PlayerUpdate),
    Stats(Stats),
    Event(Event),
}

impl WsMessage {
    /// Errors only when `text` is not valid JSON.
    pub fn parse(text: &str) -> Result<Self, serde_json::Error> {
        if let Ok(known) = serde_json::from_str::<Known>(text) {
            return Ok(match known {
                Known::Ready(v) => Self::Ready(v),
                Known::PlayerUpdate(v) => Self::PlayerUpdate(v),
                Known::Stats(v) => Self::Stats(v),
                Known::Event(v) => Self::Event(v),
            });
        }
        let payload: Value = serde_json::from_str(text)?;
        let op = payload.get("op").and_then(Value::as_str).unwrap_or("").to_owned();
        Ok(Self::Unknown { op, payload })
    }
}

/// Lavalink REST error body.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct RestError {
    #[serde(default)]
    pub status: u16,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub path: String,
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p straight-rs-model`
Expected: PASS. If the nested internally-tagged `Event` inside `Known::Event` fails to deserialize (ws_known_messages), replace `Known::Event(Event)` with a flatten-free approach: parse `serde_json::from_str::<Event>(text)` when `op == "event"` (peek with a tiny `#[derive(Deserialize)] struct Op<'a>{ op: &'a str }`). Keep the tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(model): player, filters, stats, info, routeplanner, ws messages"
```

---

### Task 3: `straight-rs` crate skeleton — Error, NodeConfig, backoff, balancer, position

**Files:**
- Create: `straight-rs/Cargo.toml`, `straight-rs/src/{lib,error,config,backoff,balancer,position}.rs`

**Interfaces:**
- Consumes: `straight_rs_model::Stats`.
- Produces:
  - `Error` variants: `Http(hyper::Error)`, `HttpClient(hyper_util::client::legacy::Error)`, `Ws(tungstenite::Error)`, `Json(serde_json::Error)`, `Lavalink{status:u16,error:String,message:String,path:String}`, `NoNode`, `Timeout`, `PlayerNotFound`, `Closed`, `Config(String)`, `Gateway(Box<dyn Error+Send+Sync>)`; `type Result<T>`.
  - `NodeConfig { host, password, secure, request_timeout, resume_timeout_secs, failover_grace, ping_interval, ping_timeout }` with `NodeConfig::new(host, password)` and `with_tls(bool)`.
  - `Backoff::new(base, max)`, `next_delay(&mut self, jitter: f64) -> Duration`, `reset()`.
  - `balancer::{penalty(&Stats) -> u32, NodeView{index,penalty,players}, Strategy::{LeastPenalty,RoundRobin,LeastPlayers,Custom(Arc<dyn Fn(&[NodeView])->Option<usize>+Send+Sync>)}, pick(&Strategy,&[NodeView],&AtomicUsize)->Option<usize>}`.
  - `position::interpolate(position: u64, playing: bool, elapsed: Duration, length: Option<u64>) -> u64`.

- [ ] **Step 1: Cargo.toml and lib.rs**

`straight-rs/Cargo.toml`:
```toml
[package]
name = "straight-rs"
description = "High-performance Lavalink v4 client"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[features]
default = []
tls = ["dep:hyper-rustls", "dep:rustls", "tokio-tungstenite/rustls-tls-webpki-roots"]
serenity = ["dep:serenity"]
twilight = ["dep:twilight-model"]
songbird = ["dep:songbird"]
e2e = []

[dependencies]
straight-rs-model.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tracing.workspace = true
tokio.workspace = true
tokio-tungstenite.workspace = true
futures-util.workspace = true
hyper.workspace = true
hyper-util.workspace = true
http-body-util.workspace = true
bytes.workspace = true
dashmap.workspace = true
arc-swap.workspace = true
percent-encoding.workspace = true
rand.workspace = true
hyper-rustls = { version = "0.27", optional = true, default-features = false, features = ["http1", "ring", "webpki-tokio"] }
rustls = { version = "0.23", optional = true, default-features = false, features = ["ring", "std"] }
serenity = { version = "0.12", optional = true, default-features = false, features = ["model"] }
twilight-model = { version = "0.16", optional = true }
songbird = { version = "0.4", optional = true, default-features = false }

[dev-dependencies]
axum = { version = "0.7", features = ["ws"] }
tokio = { workspace = true, features = ["rt-multi-thread"] }
criterion = "0.5"

[[bench]]
name = "core"
harness = false
```

Create `straight-rs/benches/core.rs` containing only `fn main() {}` for now (Task 11 fills it).

`straight-rs/src/lib.rs`:
```rust
//! straight-rs — a fast, complete Lavalink v4 client.
pub mod balancer;
pub(crate) mod backoff;
mod config;
mod error;
pub mod position;

pub use balancer::{NodeView, Strategy};
pub use config::NodeConfig;
pub use error::{Error, Result};
pub use straight_rs_model as model;
pub use straight_rs_model::{
    ChannelId, Exception, Filters, GuildId, Info, LoadResult, Severity, Stats, Track,
    TrackEndReason, TrackInfo, UpdatePlayer, UpdateTrack, UserId, VoiceState,
};
```

- [ ] **Step 2: Write failing tests** (inline `#[cfg(test)] mod tests` at the bottom of each file)

`backoff.rs` tests:
```rust
#[test]
fn doubles_up_to_cap_with_full_jitter() {
    let mut b = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
    let d: Vec<_> = (0..8).map(|_| b.next_delay(1.0)).collect();
    assert_eq!(d[0], Duration::from_millis(500));
    assert_eq!(d[1], Duration::from_secs(1));
    assert_eq!(d[2], Duration::from_secs(2));
    assert_eq!(d[6], Duration::from_secs(30));
    assert_eq!(d[7], Duration::from_secs(30));
}
#[test]
fn zero_jitter_is_half_and_reset_restarts() {
    let mut b = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
    assert_eq!(b.next_delay(0.0), Duration::from_millis(250));
    b.next_delay(0.0);
    b.reset();
    assert_eq!(b.next_delay(1.0), Duration::from_millis(500));
}
#[test]
fn huge_attempt_counts_do_not_overflow() {
    let mut b = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
    for _ in 0..1000 { assert!(b.next_delay(0.5) <= Duration::from_secs(30)); }
}
```

`balancer.rs` tests:
```rust
fn stats(players: u32, load: f64, fs: Option<(i64, i64)>) -> Stats {
    let fs = fs.map(|(n, d)| serde_json::json!({"sent":3000,"nulled":n,"deficit":d})).unwrap_or(serde_json::Value::Null);
    serde_json::from_value(serde_json::json!({"players":players,"playingPlayers":players,"uptime":1,
      "memory":{"free":1,"used":1,"allocated":1,"reservable":1},
      "cpu":{"cores":4,"systemLoad":load,"lavalinkLoad":0.0},"frameStats":fs})).unwrap()
}
#[test] fn penalty_zero_and_players_only() {
    assert_eq!(penalty(&stats(0, 0.0, None)), 0);
    assert_eq!(penalty(&stats(10, 0.0, None)), 10);
}
#[test] fn penalty_grows_with_load_and_deficit() {
    let p = penalty(&stats(10, 0.5, None));
    assert!((114..=115).contains(&p), "{p}");
    assert!(penalty(&stats(10, 0.0, Some((0, 300)))) > penalty(&stats(10, 0.0, None)));
    assert!(penalty(&stats(10, 0.0, Some((300, 0)))) > penalty(&stats(10, 0.0, None)));
}
#[test] fn penalty_never_negative_or_overflowing() {
    assert_eq!(penalty(&stats(0, 0.0, Some((0, -5000)))), 0);
    assert_eq!(penalty(&stats(0, 1e9, None)), u32::MAX);
}
fn views() -> Vec<NodeView> {
    vec![NodeView{index:0,penalty:50,players:9}, NodeView{index:2,penalty:10,players:20}, NodeView{index:5,penalty:10,players:1}]
}
#[test] fn least_penalty_ties_break_by_index() {
    assert_eq!(pick(&Strategy::LeastPenalty, &views(), &AtomicUsize::new(0)), Some(2));
}
#[test] fn least_players() {
    assert_eq!(pick(&Strategy::LeastPlayers, &views(), &AtomicUsize::new(0)), Some(5));
}
#[test] fn round_robin_cycles() {
    let rr = AtomicUsize::new(0);
    let got: Vec<_> = (0..4).map(|_| pick(&Strategy::RoundRobin, &views(), &rr).unwrap()).collect();
    assert_eq!(got, vec![0, 2, 5, 0]);
}
#[test] fn custom_must_return_a_listed_node() {
    let good = Strategy::Custom(Arc::new(|v| v.last().map(|n| n.index)));
    assert_eq!(pick(&good, &views(), &AtomicUsize::new(0)), Some(5));
    let bad = Strategy::Custom(Arc::new(|_| Some(99)));
    assert_eq!(pick(&bad, &views(), &AtomicUsize::new(0)), None);
}
#[test] fn empty_is_none() {
    assert_eq!(pick(&Strategy::LeastPenalty, &[], &AtomicUsize::new(0)), None);
}
```

`position.rs` tests:
```rust
#[test] fn adds_elapsed_only_while_playing() {
    assert_eq!(interpolate(1000, true, Duration::from_millis(250), Some(10_000)), 1250);
    assert_eq!(interpolate(1000, false, Duration::from_millis(250), Some(10_000)), 1000);
}
#[test] fn clamps_to_track_length_but_not_for_streams() {
    assert_eq!(interpolate(9_900, true, Duration::from_secs(5), Some(10_000)), 10_000);
    assert_eq!(interpolate(9_900, true, Duration::from_secs(5), None), 14_900);
}
#[test] fn saturates() {
    assert_eq!(interpolate(u64::MAX, true, Duration::from_secs(5), None), u64::MAX);
}
```

`config.rs` test:
```rust
#[test] fn defaults() {
    let c = NodeConfig::new("localhost:2333", "pw");
    assert!(!c.secure);
    assert_eq!(c.resume_timeout_secs, 60);
    assert_eq!(c.with_tls(true).secure, true);
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p straight-rs --lib`
Expected: FAIL (items not defined).

- [ ] **Step 4: Implement**

`error.rs`:
```rust
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] hyper::Error),
    #[error("http client error: {0}")]
    HttpClient(#[from] hyper_util::client::legacy::Error),
    #[error("websocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("lavalink error {status} {error}: {message} ({path})")]
    Lavalink { status: u16, error: String, message: String, path: String },
    #[error("no lavalink node available")]
    NoNode,
    #[error("request timed out")]
    Timeout,
    #[error("player not found")]
    PlayerNotFound,
    #[error("client closed")]
    Closed,
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("voice gateway error: {0}")]
    Gateway(#[source] Box<dyn std::error::Error + Send + Sync>),
}
```

`config.rs`:
```rust
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
```

`backoff.rs`:
```rust
use std::time::Duration;

pub(crate) struct Backoff {
    base: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    pub(crate) fn new(base: Duration, max: Duration) -> Self {
        Self { base, max, attempt: 0 }
    }

    /// `jitter` in `[0, 1]`: 1.0 = full delay, 0.0 = half of it.
    pub(crate) fn next_delay(&mut self, jitter: f64) -> Duration {
        let exp = self.base.saturating_mul(1u32 << self.attempt.min(16));
        let capped = exp.min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        capped.mul_f64(0.5 + 0.5 * jitter.clamp(0.0, 1.0))
    }

    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }
}
```

`balancer.rs`:
```rust
use straight_rs_model::Stats;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeView {
    pub index: usize,
    pub penalty: u32,
    pub players: u32,
}

pub type CustomStrategy = Arc<dyn Fn(&[NodeView]) -> Option<usize> + Send + Sync>;

#[derive(Clone, Default)]
pub enum Strategy {
    #[default]
    LeastPenalty,
    RoundRobin,
    LeastPlayers,
    /// Receives only ready nodes; must return the `index` of one of them.
    Custom(CustomStrategy),
}

impl fmt::Debug for Strategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeastPenalty => f.write_str("LeastPenalty"),
            Self::RoundRobin => f.write_str("RoundRobin"),
            Self::LeastPlayers => f.write_str("LeastPlayers"),
            Self::Custom(_) => f.write_str("Custom(..)"),
        }
    }
}

/// Penalty as used by the standard Lavalink clients.
pub fn penalty(stats: &Stats) -> u32 {
    let mut p = f64::from(stats.players);
    p += 1.05f64.powf(100.0 * stats.cpu.system_load) * 10.0 - 10.0;
    if let Some(f) = &stats.frame_stats {
        p += 1.03f64.powf(500.0 * f.deficit as f64 / 3000.0) * 600.0 - 600.0;
        p += (1.03f64.powf(500.0 * f.nulled as f64 / 3000.0) * 300.0 - 300.0) * 2.0;
    }
    if p.is_nan() || p <= 0.0 {
        0
    } else if p >= f64::from(u32::MAX) {
        u32::MAX
    } else {
        p as u32
    }
}

/// Picks among **ready** nodes. Returns the chosen `NodeView::index`.
pub fn pick(strategy: &Strategy, ready: &[NodeView], rr: &AtomicUsize) -> Option<usize> {
    if ready.is_empty() {
        return None;
    }
    match strategy {
        Strategy::LeastPenalty => ready.iter().min_by_key(|n| (n.penalty, n.index)).map(|n| n.index),
        Strategy::LeastPlayers => ready.iter().min_by_key(|n| (n.players, n.index)).map(|n| n.index),
        Strategy::RoundRobin => {
            let i = rr.fetch_add(1, Ordering::Relaxed) % ready.len();
            Some(ready[i].index)
        }
        Strategy::Custom(f) => f(ready).filter(|i| ready.iter().any(|n| n.index == *i)),
    }
}
```

`position.rs`:
```rust
use std::time::Duration;

/// Estimates the current position (ms) from the last server-reported one.
/// `playing` must be true only when a track is loaded, connected and not paused.
/// `length` is `None` for streams (no clamp).
#[doc(hidden)]
pub fn interpolate(position: u64, playing: bool, elapsed: Duration, length: Option<u64>) -> u64 {
    let mut p = position;
    if playing {
        p = p.saturating_add(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX));
    }
    match length {
        Some(l) => p.min(l),
        None => p,
    }
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p straight-rs --lib`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: straight-rs crate skeleton with config, backoff, balancer, position"
```

---

### Task 4: REST client and mock Lavalink harness

**Files:**
- Create: `straight-rs/src/rest.rs`, `straight-rs/tests/common/mod.rs`, `straight-rs/tests/rest.rs`
- Modify: `straight-rs/src/lib.rs` (add `pub mod rest;` and `pub use rest::RestClient;`)

**Interfaces:**
- Consumes: `NodeConfig`, `Error`, model types.
- Produces `RestClient::new(&NodeConfig, client_name: &str) -> RestClient` (Clone) with:
  `load_tracks(&str)->Result<LoadResult>`, `decode_track(&str)->Result<Track>`, `decode_tracks(&[String])->Result<Vec<Track>>`, `get_players(session:&str)->Result<Vec<Player>>`, `update_player(session:&str, GuildId, &UpdatePlayer, no_replace: bool)->Result<Player>`, `destroy_player(session:&str, GuildId)->Result<()>`, `update_session(session:&str,&SessionUpdate)->Result<Session>`, `info()->Result<Info>`, `version()->Result<String>`, `stats()->Result<Stats>`, `route_planner_status()->Result<RoutePlannerStatus>`, `free_address(&str)->Result<()>`, `free_all()->Result<()>`.
  (`Player` here is `straight_rs_model::Player`.)
- Produces test harness (`tests/common/mod.rs`): `Mock::start()`, `Mock::start_on(addr:&str, resumed: bool)`, `mock.host()`, `mock.push_text(s)`, `mock.close_ws()`, `mock.kill()`, `mock.requests()`, `mock.requests_matching(method, path_prefix)`, `mock.set_resumed(bool)`, `mock.state` (`MockState`: `requests`, `ws_headers`, `ws_connections`, `session_id = "mock-session"`, `load_response`, `fail_next_gets`, `patch_delay_ms`, `max_in_flight`), `eventually(timeout, f)`, `track_json(encoded)`, and (added in later tasks) `client(..)`.

- [ ] **Step 1: Write the mock server** — `straight-rs/tests/common/mod.rs`

```rust
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
    pub in_flight: AtomicU32,
    pub max_in_flight: AtomicU32,
    players: Mutex<HashMap<String, Value>>,
    push: broadcast::Sender<Push>,
}

pub struct Mock {
    pub addr: SocketAddr,
    pub state: Arc<MockState>,
    server: tokio::task::JoinHandle<()>,
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
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Mock { addr, state, server }
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
            s.max_in_flight.fetch_max(now, SeqCst);
            let delay = s.patch_delay_ms.load(SeqCst);
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(u64::from(delay))).await;
            }
            let out = merge_player(&s, gid, &body_json);
            s.in_flight.fetch_sub(1, SeqCst);
            Json(out).into_response()
        }
        ("DELETE", ["v4", "sessions", _, "players", gid]) => {
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
```

- [ ] **Step 2: Write failing tests** — `straight-rs/tests/rest.rs`

```rust
mod common;
use common::*;
use straight_rs::model::{SessionUpdate, UpdatePlayer};
use straight_rs::rest::RestClient;
use straight_rs::{Error, GuildId, LoadResult, NodeConfig};
use std::time::Duration;

fn rest(m: &Mock) -> RestClient {
    let mut cfg = NodeConfig::new(m.host(), "pw");
    cfg.request_timeout = Duration::from_secs(3);
    RestClient::new(&cfg, "straight-rs-test")
}

#[tokio::test]
async fn load_tracks_percent_encodes_identifier() {
    let m = Mock::start().await;
    let r = rest(&m).load_tracks("ytsearch:foo bar & baz?").await.unwrap();
    assert!(matches!(r, LoadResult::Empty(_)));
    let req = &m.requests_matching("GET", "/v4/loadtracks")[0];
    assert_eq!(req.query, "identifier=ytsearch%3Afoo%20bar%20%26%20baz%3F");
}

#[tokio::test]
async fn get_retries_on_503_then_succeeds() {
    let m = Mock::start().await;
    m.state.fail_next_gets.store(2, std::sync::atomic::Ordering::SeqCst);
    assert!(rest(&m).load_tracks("x").await.is_ok());
    assert_eq!(m.requests_matching("GET", "/v4/loadtracks").len(), 3);
}

#[tokio::test]
async fn non_json_error_body_becomes_lavalink_error_after_retries() {
    let m = Mock::start().await;
    *m.state.load_response.lock().unwrap() = Some((502, "<html>bad gateway</html>".into()));
    match rest(&m).load_tracks("x").await.unwrap_err() {
        Error::Lavalink { status, message, .. } => {
            assert_eq!(status, 502);
            assert!(message.contains("bad gateway"));
        }
        e => panic!("{e:?}"),
    }
    assert_eq!(m.requests_matching("GET", "/v4/loadtracks").len(), 3);
}

#[tokio::test]
async fn lavalink_json_error_is_parsed_and_not_retried() {
    let m = Mock::start().await;
    let e = rest(&m).get_players("nope").await.unwrap_err(); // mock 404s unknown routes
    match e {
        Error::Lavalink { status, message, error, .. } => {
            assert_eq!((status, message.as_str(), error.as_str()), (404, "Session not found", "Not Found"));
        }
        e => panic!("{e:?}"),
    }
    assert_eq!(m.requests().len(), 1);
}

#[tokio::test]
async fn update_and_destroy_player_and_session() {
    let m = Mock::start().await;
    let r = rest(&m);
    let upd = UpdatePlayer { volume: Some(40), ..Default::default() };
    let p = r.update_player("mock-session", GuildId(7), &upd, true).await.unwrap();
    assert_eq!(p.volume, 40);
    let req = &m.requests_matching("PATCH", "/v4/sessions/mock-session/players/7")[0];
    assert_eq!(req.query, "noReplace=true");
    assert_eq!(req.body, serde_json::json!({"volume": 40}));
    r.destroy_player("mock-session", GuildId(7)).await.unwrap();
    let s = r.update_session("mock-session", &SessionUpdate { resuming: Some(true), timeout: Some(60) }).await.unwrap();
    assert!(s.resuming && s.timeout == 60);
}

#[tokio::test]
async fn misc_endpoints() {
    let m = Mock::start().await;
    let r = rest(&m);
    assert_eq!(r.version().await.unwrap(), "4.0.8");
    assert_eq!(r.info().await.unwrap().version.major, 4);
    assert_eq!(r.stats().await.unwrap().players, 3);
    assert!(r.route_planner_status().await.unwrap().class.is_none());
    r.free_address("1.2.3.4").await.unwrap();
    r.free_all().await.unwrap();
    assert_eq!(&*r.decode_track("QAAA").await.unwrap().encoded, "QAAA");
    assert_eq!(r.decode_tracks(&["QAAA".to_string()]).await.unwrap().len(), 1);
}

#[tokio::test]
async fn timeout_is_reported() {
    let m = Mock::start().await;
    m.state.patch_delay_ms.store(500, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = NodeConfig::new(m.host(), "pw");
    cfg.request_timeout = Duration::from_millis(100);
    let r = RestClient::new(&cfg, "t");
    let e = r.update_player("s", GuildId(1), &UpdatePlayer::default(), false).await.unwrap_err();
    assert!(matches!(e, Error::Timeout));
}
```

`straight_rs::model::SessionUpdate` is reachable because `straight_rs::model` re-exports the model crate.

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p straight-rs --test rest`
Expected: FAIL — `straight_rs::rest` does not exist.

- [ ] **Step 4: Implement** — `straight-rs/src/rest.rs`

```rust
use crate::{Error, NodeConfig, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use straight_rs_model::{
    GuildId, Info, LoadResult, Player, RestError, RoutePlannerStatus, Session, SessionUpdate, Stats,
    Track, UpdatePlayer,
};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use serde::de::DeserializeOwned;
use std::time::Duration;

#[cfg(feature = "tls")]
type Conn = hyper_rustls::HttpsConnector<HttpConnector>;
#[cfg(not(feature = "tls"))]
type Conn = HttpConnector;

const RETRIES: u32 = 2;

#[derive(Clone)]
pub struct RestClient {
    client: Client<Conn, Full<Bytes>>,
    origin: String,
    password: String,
    user_agent: String,
    timeout: Duration,
}

fn enc(s: &str) -> String {
    utf8_percent_encode(s, NON_ALPHANUMERIC).to_string()
}

impl RestClient {
    pub fn new(cfg: &NodeConfig, client_name: &str) -> Self {
        #[allow(unused_mut)]
        let mut http = HttpConnector::new();
        http.set_nodelay(true);
        #[cfg(feature = "tls")]
        let conn = {
            http.enforce_http(false);
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_webpki_roots()
                .https_or_http()
                .enable_http1()
                .wrap_connector(http)
        };
        #[cfg(not(feature = "tls"))]
        let conn = http;
        Self {
            client: Client::builder(TokioExecutor::new()).build(conn),
            origin: format!("{}://{}", if cfg.secure { "https" } else { "http" }, cfg.host),
            password: cfg.password.clone(),
            user_agent: client_name.to_owned(),
            timeout: cfg.request_timeout,
        }
    }

    async fn send(&self, method: Method, path: &str, body: Option<Vec<u8>>) -> Result<(StatusCode, Bytes)> {
        let b = Request::builder()
            .method(method)
            .uri(format!("{}{}", self.origin, path))
            .header("Authorization", &self.password)
            .header("User-Agent", &self.user_agent);
        let req = match body {
            Some(v) => b.header("Content-Type", "application/json").body(Full::new(Bytes::from(v))),
            None => b.body(Full::new(Bytes::new())),
        }
        .map_err(|e| Error::Config(e.to_string()))?;
        let fut = async {
            let resp = self.client.request(req).await?;
            let status = resp.status();
            let bytes = resp.into_body().collect().await?.to_bytes();
            Ok::<_, Error>((status, bytes))
        };
        tokio::time::timeout(self.timeout, fut).await.map_err(|_| Error::Timeout)?
    }

    fn check(status: StatusCode, path: &str, body: &[u8]) -> Result<()> {
        if status.is_success() {
            return Ok(());
        }
        Err(match serde_json::from_slice::<RestError>(body) {
            Ok(e) => Error::Lavalink {
                status: if e.status == 0 { status.as_u16() } else { e.status },
                error: e.error,
                message: e.message,
                path: e.path,
            },
            Err(_) => Error::Lavalink {
                status: status.as_u16(),
                error: status.canonical_reason().unwrap_or("").to_owned(),
                message: String::from_utf8_lossy(body).chars().take(200).collect(),
                path: path.to_owned(),
            },
        })
    }

    /// Idempotent GET with retries on transient failures.
    async fn get(&self, path: &str) -> Result<Bytes> {
        let mut attempt = 0;
        loop {
            let res = self.send(Method::GET, path, None).await;
            let transient = match &res {
                Ok((s, _)) => matches!(s.as_u16(), 502 | 503 | 504),
                Err(Error::Http(_) | Error::HttpClient(_) | Error::Timeout) => true,
                Err(_) => false,
            };
            if transient && attempt < RETRIES {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt))).await;
                continue;
            }
            let (status, body) = res?;
            Self::check(status, path, &body)?;
            return Ok(body);
        }
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        Ok(serde_json::from_slice(&self.get(path).await?)?)
    }

    async fn write(&self, method: Method, path: &str, body: Option<Vec<u8>>) -> Result<Bytes> {
        let (status, bytes) = self.send(method, path, body).await?;
        Self::check(status, path, &bytes)?;
        Ok(bytes)
    }

    pub async fn load_tracks(&self, identifier: &str) -> Result<LoadResult> {
        self.get_json(&format!("/v4/loadtracks?identifier={}", enc(identifier))).await
    }

    pub async fn decode_track(&self, encoded: &str) -> Result<Track> {
        self.get_json(&format!("/v4/decodetrack?encodedTrack={}", enc(encoded))).await
    }

    pub async fn decode_tracks(&self, encoded: &[String]) -> Result<Vec<Track>> {
        let body = serde_json::to_vec(encoded)?;
        let out = self.write(Method::POST, "/v4/decodetracks", Some(body)).await?;
        Ok(serde_json::from_slice(&out)?)
    }

    pub async fn get_players(&self, session: &str) -> Result<Vec<Player>> {
        self.get_json(&format!("/v4/sessions/{}/players", enc(session))).await
    }

    pub async fn update_player(
        &self,
        session: &str,
        guild: GuildId,
        upd: &UpdatePlayer,
        no_replace: bool,
    ) -> Result<Player> {
        let path = format!("/v4/sessions/{}/players/{}?noReplace={}", enc(session), guild, no_replace);
        let out = self.write(Method::PATCH, &path, Some(serde_json::to_vec(upd)?)).await?;
        Ok(serde_json::from_slice(&out)?)
    }

    pub async fn destroy_player(&self, session: &str, guild: GuildId) -> Result<()> {
        let path = format!("/v4/sessions/{}/players/{}", enc(session), guild);
        self.write(Method::DELETE, &path, None).await.map(|_| ())
    }

    pub async fn update_session(&self, session: &str, upd: &SessionUpdate) -> Result<Session> {
        let path = format!("/v4/sessions/{}", enc(session));
        let out = self.write(Method::PATCH, &path, Some(serde_json::to_vec(upd)?)).await?;
        Ok(serde_json::from_slice(&out)?)
    }

    pub async fn info(&self) -> Result<Info> {
        self.get_json("/v4/info").await
    }

    pub async fn version(&self) -> Result<String> {
        Ok(String::from_utf8_lossy(&self.get("/version").await?).trim().to_owned())
    }

    pub async fn stats(&self) -> Result<Stats> {
        self.get_json("/v4/stats").await
    }

    pub async fn route_planner_status(&self) -> Result<RoutePlannerStatus> {
        let body = self.get("/v4/routeplanner/status").await?;
        if body.iter().all(u8::is_ascii_whitespace) {
            return Ok(RoutePlannerStatus::default());
        }
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn free_address(&self, address: &str) -> Result<()> {
        let body = serde_json::to_vec(&serde_json::json!({ "address": address }))?;
        self.write(Method::POST, "/v4/routeplanner/free/address", Some(body)).await.map(|_| ())
    }

    pub async fn free_all(&self) -> Result<()> {
        self.write(Method::POST, "/v4/routeplanner/free/all", None).await.map(|_| ())
    }
}
```

Add to `lib.rs`: `pub mod rest;` and `pub use rest::RestClient;`.

- [ ] **Step 5: Run to verify pass**

Run: `cargo test -p straight-rs --test rest`
Expected: PASS (7 tests). Fix compile details against the pinned crate versions without changing behavior.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat: REST client with retries and mock lavalink test harness"
```

---

### Task 5: Events, voice assembler, voice gateway trait, player state

**Files:**
- Create: `straight-rs/src/{event,voice,gateway,state}.rs`
- Modify: `straight-rs/src/lib.rs`

**Interfaces:**
- Consumes: model types, `position::interpolate`.
- Produces:
  - `Event` (Clone, Debug, `#[non_exhaustive]`): `NodeConnected{node}`, `NodeDisconnected{node}`, `Ready{node,resumed,session_id:Arc<str>}`, `Stats{node,stats:Arc<Stats>}`, `PlayerUpdate{node,guild,state:PlayerState}`, `TrackStart{node,guild,track:Arc<Track>}`, `TrackEnd{node,guild,track,reason}`, `TrackException{node,guild,track,exception:Arc<Exception>}`, `TrackStuck{node,guild,track,threshold_ms}`, `WebSocketClosed{node,guild,code,reason,by_remote}`, `PlayerMigrated{guild,from,to}`, `Unknown{node,op,payload}`. Methods `guild() -> Option<GuildId>`, `may_start_next() -> bool`, `pub(crate) from_model(node: usize, model::Event) -> Event`.
  - `VoiceStateUpdate{channel_id:Option<ChannelId>, session_id:String}`, `VoiceServerUpdate{token:String, endpoint:Option<String>}`, `VoiceOutcome::{Pending, Ready(VoiceState), Left}`, `pub(crate) VoiceAssembler` with `update_state`, `update_server`, `mark_sent(VoiceState)`, `current() -> Option<VoiceState>`.
  - `VoiceGateway` trait: `fn join(&self, GuildId, ChannelId) -> BoxFuture<'_, Result<()>>`, `fn leave(&self, GuildId) -> BoxFuture<'_, Result<()>>`; `BoxFuture<'a, T>`.
  - `PlayerSnapshot { track: Option<Arc<Track>>, paused, volume: u16, filters: Filters, position: u64, connected: bool, ping: i64, updated_at: Instant }` with `position_now()`.
  - `pub(crate) PlayerInner { guild, orphaned: AtomicBool, voice: Mutex<VoiceAssembler>, gate: tokio::sync::Mutex<()> }` with `new(GuildId)`, `node_index() -> Option<usize>`, `assign_node(usize) -> usize` (compare-exchange from unassigned, returns the winner), `set_node(usize)`, `snapshot() -> Arc<PlayerSnapshot>`, `apply_update(&PlayerState)`, `apply_player(&model::Player)`, `clear_track_if(&str)`, `restore_payload() -> Option<UpdatePlayer>`; `pub(crate) fn lock<T>(&Mutex<T>) -> MutexGuard<T>` (poison-tolerant).

- [ ] **Step 1: Write failing tests**

`voice.rs` tests:
```rust
fn st(ch: Option<u64>, s: &str) -> VoiceStateUpdate { VoiceStateUpdate { channel_id: ch.map(ChannelId), session_id: s.into() } }
fn sv(t: &str, e: Option<&str>) -> VoiceServerUpdate { VoiceServerUpdate { token: t.into(), endpoint: e.map(Into::into) } }
fn expect_ready(o: VoiceOutcome) -> VoiceState { match o { VoiceOutcome::Ready(v) => v, o => panic!("{o:?}") } }

#[test] fn state_then_server() {
    let mut a = VoiceAssembler::default();
    assert!(matches!(a.update_state(st(Some(9), "sess")), VoiceOutcome::Pending));
    let v = expect_ready(a.update_server(sv("tok", Some("e.discord.media:443"))));
    assert_eq!((v.token.as_str(), v.endpoint.as_str(), v.session_id.as_str(), v.channel_id), ("tok", "e.discord.media:443", "sess", Some(ChannelId(9))));
}
#[test] fn server_then_state() {
    let mut a = VoiceAssembler::default();
    assert!(matches!(a.update_server(sv("tok", Some("e"))), VoiceOutcome::Pending));
    let v = expect_ready(a.update_state(st(Some(9), "sess")));
    assert_eq!(v.session_id, "sess");
}
#[test] fn null_endpoint_clears_and_waits() {
    let mut a = VoiceAssembler::default();
    a.update_state(st(Some(9), "s"));
    expect_ready(a.update_server(sv("t", Some("e"))));
    assert!(matches!(a.update_server(sv("t2", None)), VoiceOutcome::Pending));
    // state update alone must not re-emit stale token/endpoint
    assert!(matches!(a.update_state(st(Some(9), "s2")), VoiceOutcome::Pending));
    expect_ready(a.update_server(sv("t3", Some("e2"))));
}
#[test] fn leaving_resets_everything() {
    let mut a = VoiceAssembler::default();
    a.update_state(st(Some(9), "s"));
    a.update_server(sv("t", Some("e")));
    assert!(matches!(a.update_state(st(None, "s")), VoiceOutcome::Left));
    assert!(matches!(a.update_server(sv("t", Some("e"))), VoiceOutcome::Pending));
    assert!(a.current().is_none());
}
#[test] fn duplicates_are_suppressed_after_mark_sent_but_changes_are_not() {
    let mut a = VoiceAssembler::default();
    a.update_state(st(Some(9), "s"));
    let v = expect_ready(a.update_server(sv("t", Some("e"))));
    // not marked sent yet -> would emit again (a failed PATCH can be retried)
    expect_ready(a.update_server(sv("t", Some("e"))));
    a.mark_sent(v.clone());
    assert_eq!(a.current(), Some(v));
    assert!(matches!(a.update_server(sv("t", Some("e"))), VoiceOutcome::Pending));
    expect_ready(a.update_server(sv("t", Some("new-region"))));
}
```

`event.rs` tests:
```rust
#[test] fn from_model_maps_fields() {
    let track: straight_rs_model::Track = serde_json::from_str(r#"{"encoded":"E","info":{"identifier":"i","isSeekable":true,"author":"a","length":1,"isStream":false,"position":0,"title":"t","uri":null,"artworkUrl":null,"isrc":null,"sourceName":"s"}}"#).unwrap();
    let e = Event::from_model(3, straight_rs_model::Event::TrackEnd { guild_id: GuildId(7), track, reason: TrackEndReason::Finished });
    assert_eq!(e.guild(), Some(GuildId(7)));
    assert!(e.may_start_next());
    assert!(matches!(e, Event::TrackEnd { node: 3, .. }));
    assert_eq!(Event::NodeDisconnected { node: 0 }.guild(), None);
}
```

`state.rs` tests:
```rust
fn track(enc: &str, len: u64, stream: bool) -> straight_rs_model::Track {
    serde_json::from_value(serde_json::json!({"encoded":enc,"info":{"identifier":"i","isSeekable":true,"author":"a","length":len,"isStream":stream,"position":0,"title":"t","uri":null,"artworkUrl":null,"isrc":null,"sourceName":"s"}})).unwrap()
}
fn model_player(t: Option<straight_rs_model::Track>, pos: u64, paused: bool) -> straight_rs_model::Player {
    serde_json::from_value(serde_json::json!({"guildId":"1","track":t,"volume":80,"paused":paused,
      "state":{"time":0,"position":pos,"connected":true,"ping":4},"voice":{"token":"","endpoint":"","sessionId":""},"filters":{"volume":0.5}})).unwrap()
}
#[test] fn assign_node_first_wins() {
    let p = PlayerInner::new(GuildId(1));
    assert_eq!(p.node_index(), None);
    assert_eq!(p.assign_node(2), 2);
    assert_eq!(p.assign_node(5), 2);
    p.set_node(5);
    assert_eq!(p.node_index(), Some(5));
}
#[test] fn apply_player_and_update() {
    let p = PlayerInner::new(GuildId(1));
    p.apply_player(&model_player(Some(track("A", 10_000, false)), 100, false));
    let s = p.snapshot();
    assert_eq!((s.volume, s.paused, s.position, s.ping), (80, false, 100, 4));
    assert_eq!(s.filters.volume, Some(0.5));
    p.apply_update(&straight_rs_model::PlayerState { time: 0, position: 500, connected: false, ping: 9 });
    let s = p.snapshot();
    assert_eq!((s.position, s.connected, s.ping), (500, false, 9));
    assert!(s.track.is_some(), "playerUpdate must not touch the track");
}
#[test] fn position_now_respects_pause_disconnect_and_no_track() {
    let p = PlayerInner::new(GuildId(1));
    p.apply_player(&model_player(Some(track("A", 10_000, false)), 1_000, true));
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert_eq!(p.snapshot().position_now(), 1_000);
    p.apply_player(&model_player(None, 0, false));
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert_eq!(p.snapshot().position_now(), 0);
    p.apply_player(&model_player(Some(track("A", 10_000, false)), 1_000, false));
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert!(p.snapshot().position_now() >= 1_030);
}
#[test] fn clear_track_only_when_encoded_matches() {
    let p = PlayerInner::new(GuildId(1));
    p.apply_player(&model_player(Some(track("A", 1, false)), 0, false));
    p.clear_track_if("B");
    assert!(p.snapshot().track.is_some());
    p.clear_track_if("A");
    assert!(p.snapshot().track.is_none());
}
#[test] fn restore_payload_contains_track_position_filters_voice() {
    let p = PlayerInner::new(GuildId(1));
    assert!(p.restore_payload().is_none());
    p.apply_player(&model_player(Some(track("A", 100_000, false)), 2_000, true));
    lock(&p.voice).mark_sent(VoiceState { token: "t".into(), endpoint: "e".into(), session_id: "s".into(), channel_id: None });
    let u = p.restore_payload().unwrap();
    assert_eq!(u.track.unwrap().encoded, Some(Some("A".into())));
    assert!(u.position.unwrap() >= 2_000);
    assert_eq!(u.paused, Some(true));
    assert_eq!(u.volume, Some(80));
    assert_eq!(u.voice.unwrap().token, "t");
}
#[test] fn restore_payload_skips_position_for_streams() {
    let p = PlayerInner::new(GuildId(1));
    p.apply_player(&model_player(Some(track("S", 0, true)), 2_000, false));
    assert!(p.restore_payload().unwrap().position.is_none());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p straight-rs --lib`
Expected: FAIL (modules missing).

- [ ] **Step 3: Implement**

`lib.rs` additions:
```rust
mod event;
mod gateway;
pub(crate) mod state;
mod voice;

pub use event::Event;
pub use gateway::{BoxFuture, VoiceGateway};
pub use state::PlayerSnapshot;
pub use voice::{VoiceOutcome, VoiceServerUpdate, VoiceStateUpdate};
```

`gateway.rs`:
```rust
use crate::Result;
use straight_rs_model::{ChannelId, GuildId};
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Implemented by the host Discord library so straight-rs can join/leave voice
/// channels (sends gateway opcode 4).
pub trait VoiceGateway: Send + Sync + 'static {
    fn join(&self, guild: GuildId, channel: ChannelId) -> BoxFuture<'_, Result<()>>;
    fn leave(&self, guild: GuildId) -> BoxFuture<'_, Result<()>>;
}
```

`voice.rs`:
```rust
use straight_rs_model::{ChannelId, VoiceState};

#[derive(Clone, Debug)]
pub struct VoiceStateUpdate {
    /// `None` = the bot left the channel.
    pub channel_id: Option<ChannelId>,
    pub session_id: String,
}

#[derive(Clone, Debug)]
pub struct VoiceServerUpdate {
    pub token: String,
    /// `None` = Discord's voice server is unavailable.
    pub endpoint: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum VoiceOutcome {
    Pending,
    Ready(VoiceState),
    Left,
}

#[derive(Default)]
pub(crate) struct VoiceAssembler {
    session_id: Option<String>,
    channel_id: Option<ChannelId>,
    token: Option<String>,
    endpoint: Option<String>,
    sent: Option<VoiceState>,
}

impl VoiceAssembler {
    pub(crate) fn update_state(&mut self, u: VoiceStateUpdate) -> VoiceOutcome {
        match u.channel_id {
            None => {
                *self = Self::default();
                VoiceOutcome::Left
            }
            Some(c) => {
                self.channel_id = Some(c);
                self.session_id = Some(u.session_id);
                self.evaluate()
            }
        }
    }

    pub(crate) fn update_server(&mut self, u: VoiceServerUpdate) -> VoiceOutcome {
        match u.endpoint {
            None => {
                self.token = None;
                self.endpoint = None;
                VoiceOutcome::Pending
            }
            Some(e) => {
                self.token = Some(u.token);
                self.endpoint = Some(e);
                self.evaluate()
            }
        }
    }

    fn evaluate(&self) -> VoiceOutcome {
        let (Some(session_id), Some(token), Some(endpoint)) =
            (&self.session_id, &self.token, &self.endpoint)
        else {
            return VoiceOutcome::Pending;
        };
        let vs = VoiceState {
            token: token.clone(),
            endpoint: endpoint.clone(),
            session_id: session_id.clone(),
            channel_id: self.channel_id,
        };
        if self.sent.as_ref() == Some(&vs) {
            VoiceOutcome::Pending
        } else {
            VoiceOutcome::Ready(vs)
        }
    }

    /// Record that `vs` reached a node, so identical updates are not re-sent.
    pub(crate) fn mark_sent(&mut self, vs: VoiceState) {
        self.sent = Some(vs);
    }

    /// Last voice state successfully delivered to a node.
    pub(crate) fn current(&self) -> Option<VoiceState> {
        self.sent.clone()
    }
}
```

`event.rs`:
```rust
use crate::TrackEndReason;
use straight_rs_model::{self as model, Exception, GuildId, PlayerState, Stats, Track};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
    NodeConnected { node: usize },
    NodeDisconnected { node: usize },
    Ready { node: usize, resumed: bool, session_id: Arc<str> },
    Stats { node: usize, stats: Arc<Stats> },
    PlayerUpdate { node: usize, guild: GuildId, state: PlayerState },
    TrackStart { node: usize, guild: GuildId, track: Arc<Track> },
    TrackEnd { node: usize, guild: GuildId, track: Arc<Track>, reason: TrackEndReason },
    TrackException { node: usize, guild: GuildId, track: Arc<Track>, exception: Arc<Exception> },
    TrackStuck { node: usize, guild: GuildId, track: Arc<Track>, threshold_ms: u64 },
    WebSocketClosed { node: usize, guild: GuildId, code: u16, reason: String, by_remote: bool },
    PlayerMigrated { guild: GuildId, from: usize, to: usize },
    Unknown { node: usize, op: String, payload: Value },
}

impl Event {
    pub fn guild(&self) -> Option<GuildId> {
        match self {
            Self::PlayerUpdate { guild, .. }
            | Self::TrackStart { guild, .. }
            | Self::TrackEnd { guild, .. }
            | Self::TrackException { guild, .. }
            | Self::TrackStuck { guild, .. }
            | Self::WebSocketClosed { guild, .. }
            | Self::PlayerMigrated { guild, .. } => Some(*guild),
            _ => None,
        }
    }

    /// True for `TrackEnd` events after which a queue may start the next track.
    pub fn may_start_next(&self) -> bool {
        matches!(self, Self::TrackEnd { reason, .. } if reason.may_start_next())
    }

    pub(crate) fn from_model(node: usize, ev: model::Event) -> Self {
        match ev {
            model::Event::TrackStart { guild_id, track } => {
                Self::TrackStart { node, guild: guild_id, track: Arc::new(track) }
            }
            model::Event::TrackEnd { guild_id, track, reason } => {
                Self::TrackEnd { node, guild: guild_id, track: Arc::new(track), reason }
            }
            model::Event::TrackException { guild_id, track, exception } => Self::TrackException {
                node, guild: guild_id, track: Arc::new(track), exception: Arc::new(exception),
            },
            model::Event::TrackStuck { guild_id, track, threshold_ms } => {
                Self::TrackStuck { node, guild: guild_id, track: Arc::new(track), threshold_ms }
            }
            model::Event::WebSocketClosed { guild_id, code, reason, by_remote } => {
                Self::WebSocketClosed { node, guild: guild_id, code, reason, by_remote }
            }
        }
    }
}
```

`state.rs`:
```rust
use crate::position::interpolate;
use crate::voice::VoiceAssembler;
use arc_swap::ArcSwap;
use straight_rs_model::{self as model, Filters, GuildId, PlayerState, Track, UpdatePlayer, UpdateTrack};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

const NO_NODE: usize = usize::MAX;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone, Debug)]
pub struct PlayerSnapshot {
    pub track: Option<Arc<Track>>,
    pub paused: bool,
    pub volume: u16,
    pub filters: Filters,
    /// Position in ms as last reported by the node (see `position_now`).
    pub position: u64,
    pub connected: bool,
    pub ping: i64,
    pub updated_at: Instant,
}

impl Default for PlayerSnapshot {
    fn default() -> Self {
        Self {
            track: None,
            paused: false,
            volume: 100,
            filters: Filters::default(),
            position: 0,
            connected: false,
            ping: -1,
            updated_at: Instant::now(),
        }
    }
}

impl PlayerSnapshot {
    /// Interpolated current position in ms.
    pub fn position_now(&self) -> u64 {
        let playing = self.track.is_some() && self.connected && !self.paused;
        let length = self.track.as_ref().filter(|t| !t.info.is_stream).map(|t| t.info.length);
        interpolate(self.position, playing, self.updated_at.elapsed(), length)
    }
}

pub(crate) struct PlayerInner {
    pub(crate) guild: GuildId,
    node: AtomicUsize,
    pub(crate) orphaned: AtomicBool,
    snapshot: ArcSwap<PlayerSnapshot>,
    pub(crate) voice: Mutex<VoiceAssembler>,
    /// Fair FIFO gate: at most one in-flight write per guild, in call order.
    pub(crate) gate: tokio::sync::Mutex<()>,
}

impl PlayerInner {
    pub(crate) fn new(guild: GuildId) -> Self {
        Self {
            guild,
            node: AtomicUsize::new(NO_NODE),
            orphaned: AtomicBool::new(false),
            snapshot: ArcSwap::from_pointee(PlayerSnapshot::default()),
            voice: Mutex::new(VoiceAssembler::default()),
            gate: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) fn node_index(&self) -> Option<usize> {
        match self.node.load(Ordering::Acquire) {
            NO_NODE => None,
            i => Some(i),
        }
    }

    /// Assign a node if none is assigned yet; returns the node index in effect.
    pub(crate) fn assign_node(&self, idx: usize) -> usize {
        match self.node.compare_exchange(NO_NODE, idx, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => idx,
            Err(cur) => cur,
        }
    }

    pub(crate) fn set_node(&self, idx: usize) {
        self.node.store(idx, Ordering::Release);
    }

    pub(crate) fn snapshot(&self) -> Arc<PlayerSnapshot> {
        self.snapshot.load_full()
    }

    pub(crate) fn apply_update(&self, st: &PlayerState) {
        self.snapshot.rcu(|cur| {
            let mut n = PlayerSnapshot::clone(cur);
            n.position = st.position;
            n.connected = st.connected;
            n.ping = st.ping;
            n.updated_at = Instant::now();
            n
        });
    }

    pub(crate) fn apply_player(&self, p: &model::Player) {
        self.snapshot.store(Arc::new(PlayerSnapshot {
            track: p.track.clone().map(Arc::new),
            paused: p.paused,
            volume: p.volume,
            filters: p.filters.clone(),
            position: p.state.position,
            connected: p.state.connected,
            ping: p.state.ping,
            updated_at: Instant::now(),
        }));
    }

    pub(crate) fn clear_track_if(&self, encoded: &str) {
        self.snapshot.rcu(|cur| {
            let mut n = PlayerSnapshot::clone(cur);
            if n.track.as_ref().is_some_and(|t| &*t.encoded == encoded) {
                n.track = None;
                n.position = 0;
            }
            n
        });
    }

    /// Everything needed to recreate this player on another node.
    pub(crate) fn restore_payload(&self) -> Option<UpdatePlayer> {
        let s = self.snapshot();
        let voice = lock(&self.voice).current();
        if s.track.is_none() && voice.is_none() {
            return None;
        }
        let mut upd = UpdatePlayer::default();
        if let Some(t) = &s.track {
            upd.track = Some(UpdateTrack { encoded: Some(Some(t.encoded.clone())), ..Default::default() });
            if !t.info.is_stream {
                upd.position = Some(s.position_now());
            }
        }
        upd.paused = Some(s.paused);
        upd.volume = Some(s.volume);
        upd.filters = Some(s.filters.clone());
        upd.voice = voice;
        Some(upd)
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p straight-rs --lib`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: events, voice assembler, gateway trait, lock-free player state"
```

---

### Task 6: Node connection, hub and client builder

**Files:**
- Create: `straight-rs/src/{node,hub,client}.rs`, `straight-rs/tests/node.rs`
- Modify: `straight-rs/src/lib.rs`, `straight-rs/tests/common/mod.rs` (append helpers)

**Interfaces:**
- Consumes: `RestClient`, `Backoff`, `balancer::{penalty, pick, NodeView}`, `Event`, `PlayerInner`, `VoiceGateway`.
- Produces:
  - `Node` (pub): `index()`, `status() -> NodeStatus`, `is_ready()`, `penalty() -> u32`, `players() -> u32`, `stats() -> Option<Arc<Stats>>`, `session_id() -> Option<Arc<String>>`, `rest() -> &RestClient`, `async info() -> Result<&Info>`, `async version() -> Result<&str>`, `async route_planner_status()`, `async free_address(&str)`, `async free_all()`. `NodeStatus::{Connecting, Ready, Disconnected}`.
  - `pub(crate) Hub { user_id, client_name, nodes: Vec<Arc<Node>>, players: DashMap<GuildId, Arc<PlayerInner>>, events: broadcast::Sender<Event>, strategy, rr, ready: Notify, gateway }` with `emit`, `pick_node(exclude: Option<usize>) -> Option<Arc<Node>>`, `node_for(&PlayerInner) -> Result<Arc<Node>>`, `node_ready(&Arc<Self>, &Arc<Node>, resumed: bool)`, `node_down(&Arc<Self>, &Arc<Node>)`.
  - `LavalinkClient` (Clone): `builder(impl Into<UserId>) -> ClientBuilder`, `events() -> broadcast::Receiver<Event>`, `nodes() -> &[Arc<Node>]`, `async wait_ready(Duration) -> Result<()>`, `async load(&str)`, `async decode_track(&str)`, `async decode_tracks(&[String])`, `shutdown()`. `ClientBuilder`: `node(NodeConfig)`, `strategy(Strategy)`, `client_name(String)`, `event_capacity(usize)`, `gateway(Arc<dyn VoiceGateway>)`, `async build() -> Result<LavalinkClient>`.

- [ ] **Step 1: Append test helpers to `straight-rs/tests/common/mod.rs`**

```rust
use straight_rs::{Event, LavalinkClient, NodeConfig, UserId};
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

pub fn sample_track(encoded: &str) -> straight_rs::Track {
    serde_json::from_value(track_json(encoded)).unwrap()
}

pub const STATS_10: &str = r#"{"op":"stats","players":10,"playingPlayers":5,"uptime":1000,"memory":{"free":1,"used":2,"allocated":3,"reservable":4},"cpu":{"cores":4,"systemLoad":0.0,"lavalinkLoad":0.0},"frameStats":null}"#;
```

- [ ] **Step 2: Write failing tests** — `straight-rs/tests/node.rs`

```rust
mod common;
use common::*;
use straight_rs::{Error, Event};
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

#[tokio::test]
async fn connects_with_auth_headers_and_enables_resume() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let h = mock.state.ws_headers.lock().unwrap()[0].clone();
    assert_eq!(h["authorization"].to_str().unwrap(), "pw");
    assert_eq!(h["user-id"].to_str().unwrap(), "1");
    assert!(h["client-name"].to_str().unwrap().starts_with("straight-rs/"));
    assert!(h.get("session-id").is_none());
    assert_eq!(c.nodes()[0].session_id().unwrap().as_str(), "mock-session");
    eventually(Duration::from_secs(5), || !mock.requests_matching("PATCH", "/v4/sessions/mock-session").is_empty()).await;
    let r = &mock.requests_matching("PATCH", "/v4/sessions/mock-session")[0];
    assert_eq!(r.body, serde_json::json!({"resuming": true, "timeout": 60}));
}

#[tokio::test]
async fn reconnects_after_drop_and_sends_session_id() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let mut rx = c.events();
    mock.close_ws();
    next_event(&mut rx, |e| matches!(e, Event::NodeDisconnected { .. })).await;
    eventually(Duration::from_secs(5), || mock.state.ws_connections.load(SeqCst) == 2).await;
    let h = mock.state.ws_headers.lock().unwrap()[1].clone();
    assert_eq!(h["session-id"].to_str().unwrap(), "mock-session");
    next_event(&mut rx, |e| matches!(e, Event::NodeConnected { .. })).await;
}

#[tokio::test]
async fn unknown_and_garbage_frames_do_not_kill_the_node() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let mut rx = c.events();
    mock.push_text("not json at all");
    mock.push_text(r#"{"op":"weird","x":1}"#);
    mock.push_text(r#"{"op":"event","type":"NewEvent","guildId":"1"}"#);
    mock.push_text(STATS_10);
    let a = next_event(&mut rx, |e| matches!(e, Event::Unknown { .. })).await;
    assert!(matches!(a, Event::Unknown { ref op, .. } if op == "weird"));
    let b = next_event(&mut rx, |e| matches!(e, Event::Unknown { .. })).await;
    assert!(matches!(b, Event::Unknown { ref op, .. } if op == "event"));
    next_event(&mut rx, |e| matches!(e, Event::Stats { .. })).await;
    assert_eq!(mock.state.ws_connections.load(SeqCst), 1);
    assert!(c.nodes()[0].is_ready());
}

#[tokio::test]
async fn stats_update_penalty_and_players() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    mock.push_text(STATS_10);
    eventually(Duration::from_secs(5), || c.nodes()[0].penalty() == 10).await;
    assert_eq!(c.nodes()[0].players(), 10);
    assert_eq!(c.nodes()[0].stats().unwrap().playing_players, 5);
}

#[tokio::test]
async fn load_uses_a_ready_node_and_fails_without_one() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    assert!(matches!(c.load("x").await.unwrap(), straight_rs::LoadResult::Empty(_)));
    mock.kill();
    eventually(Duration::from_secs(5), || !c.nodes()[0].is_ready()).await;
    assert!(matches!(c.load("x").await.unwrap_err(), Error::NoNode));
}

#[tokio::test]
async fn node_info_and_version_are_available() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    assert_eq!(c.nodes()[0].version().await.unwrap(), "4.0.8");
    assert_eq!(c.nodes()[0].info().await.unwrap().version.major, 4);
}

#[tokio::test]
async fn wait_ready_times_out_when_nothing_listens() {
    let mut b = straight_rs::LavalinkClient::builder(straight_rs::UserId(1));
    b = b.node(straight_rs::NodeConfig::new("127.0.0.1:1", "pw"));
    let c = b.build().await.unwrap();
    assert!(matches!(c.wait_ready(Duration::from_millis(300)).await.unwrap_err(), Error::Timeout));
}

#[tokio::test]
async fn builder_requires_a_node() {
    let r = straight_rs::LavalinkClient::builder(straight_rs::UserId(1)).build().await;
    assert!(matches!(r, Err(Error::Config(_))));
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p straight-rs --test node`
Expected: FAIL — `LavalinkClient`, `Node` do not exist.

- [ ] **Step 4: Implement `node.rs`**

```rust
use crate::backoff::Backoff;
use crate::balancer::penalty;
use crate::hub::Hub;
use crate::rest::RestClient;
use crate::{Error, Event, NodeConfig, Result};
use arc_swap::ArcSwapOption;
use futures_util::{SinkExt, StreamExt};
use straight_rs_model::{Info, RoutePlannerStatus, SessionUpdate, Stats, WsMessage};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering::*};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, OnceCell};
use tokio::time::Instant;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    Connecting,
    Ready,
    Disconnected,
}

pub struct Node {
    pub(crate) index: usize,
    pub(crate) cfg: NodeConfig,
    rest: RestClient,
    session_id: ArcSwapOption<String>,
    status: AtomicU8,
    penalty: AtomicU32,
    players: AtomicU32,
    stats: ArcSwapOption<Stats>,
    epoch: AtomicU64,
    info: OnceCell<Info>,
    version: OnceCell<String>,
}

fn header(v: &str) -> Result<HeaderValue> {
    HeaderValue::from_str(v).map_err(|e| Error::Config(format!("invalid header value: {e}")))
}

impl Node {
    pub(crate) fn new(index: usize, cfg: NodeConfig, client_name: &str) -> Arc<Self> {
        Arc::new(Self {
            index,
            rest: RestClient::new(&cfg, client_name),
            cfg,
            session_id: ArcSwapOption::empty(),
            status: AtomicU8::new(NodeStatus::Connecting as u8),
            penalty: AtomicU32::new(0),
            players: AtomicU32::new(0),
            stats: ArcSwapOption::empty(),
            epoch: AtomicU64::new(0),
            info: OnceCell::new(),
            version: OnceCell::new(),
        })
    }

    pub fn index(&self) -> usize { self.index }
    pub fn status(&self) -> NodeStatus {
        match self.status.load(Acquire) {
            0 => NodeStatus::Connecting,
            1 => NodeStatus::Ready,
            _ => NodeStatus::Disconnected,
        }
    }
    fn set_status(&self, s: NodeStatus) {
        self.status.store(
            match s { NodeStatus::Connecting => 0, NodeStatus::Ready => 1, NodeStatus::Disconnected => 2 },
            Release,
        );
    }
    pub fn is_ready(&self) -> bool { self.status() == NodeStatus::Ready }
    pub fn penalty(&self) -> u32 { self.penalty.load(Relaxed) }
    pub fn players(&self) -> u32 { self.players.load(Relaxed) }
    pub fn stats(&self) -> Option<Arc<Stats>> { self.stats.load_full() }
    pub fn session_id(&self) -> Option<Arc<String>> { self.session_id.load_full() }
    pub fn rest(&self) -> &RestClient { &self.rest }
    pub(crate) fn bump_epoch(&self) -> u64 { self.epoch.fetch_add(1, AcqRel) + 1 }
    pub(crate) fn epoch(&self) -> u64 { self.epoch.load(Acquire) }

    pub async fn info(&self) -> Result<&Info> {
        self.info.get_or_try_init(|| self.rest.info()).await
    }
    pub async fn version(&self) -> Result<&str> {
        self.version.get_or_try_init(|| self.rest.version()).await.map(String::as_str)
    }
    pub async fn route_planner_status(&self) -> Result<RoutePlannerStatus> {
        self.rest.route_planner_status().await
    }
    pub async fn free_address(&self, address: &str) -> Result<()> {
        self.rest.free_address(address).await
    }
    pub async fn free_all(&self) -> Result<()> {
        self.rest.free_all().await
    }

    /// Connection loop: connect, read, reconnect with backoff, until shutdown.
    pub(crate) async fn run(self: Arc<Self>, hub: Arc<Hub>, mut shutdown: watch::Receiver<bool>) {
        let mut backoff = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        loop {
            if *shutdown.borrow() {
                return;
            }
            self.set_status(NodeStatus::Connecting);
            let outcome = tokio::select! {
                r = self.session(&hub, &mut backoff) => r,
                _ = shutdown.changed() => return,
            };
            let was_ready = self.is_ready();
            self.set_status(NodeStatus::Disconnected);
            if was_ready {
                hub.node_down(&self);
            }
            match outcome {
                Ok(()) => tracing::info!(node = self.index, "lavalink socket closed"),
                Err(e) => tracing::warn!(node = self.index, error = %e, "lavalink socket error"),
            }
            let delay = backoff.next_delay(rand::random::<f64>());
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.changed() => return,
            }
        }
    }

    async fn session(self: &Arc<Self>, hub: &Arc<Hub>, backoff: &mut Backoff) -> Result<()> {
        let scheme = if self.cfg.secure { "wss" } else { "ws" };
        let mut req = format!("{scheme}://{}/v4/websocket", self.cfg.host).into_client_request()?;
        let h = req.headers_mut();
        h.insert("Authorization", header(&self.cfg.password)?);
        h.insert("User-Id", header(&hub.user_id.to_string())?);
        h.insert("Client-Name", header(&hub.client_name)?);
        if let Some(sid) = self.session_id() {
            h.insert("Session-Id", header(&sid)?);
        }
        let (ws, _) = tokio::time::timeout(self.cfg.request_timeout, connect_async(req))
            .await
            .map_err(|_| Error::Timeout)??;
        let (mut write, mut read) = ws.split();
        let mut ping = tokio::time::interval_at(Instant::now() + self.cfg.ping_interval, self.cfg.ping_interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_rx = Instant::now();
        loop {
            tokio::select! {
                frame = read.next() => match frame {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(Message::Text(t))) => {
                        last_rx = Instant::now();
                        self.on_text(hub, backoff, &t);
                    }
                    Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Ok(_)) => last_rx = Instant::now(),
                },
                _ = ping.tick() => {
                    if last_rx.elapsed() > self.cfg.ping_timeout {
                        return Err(Error::Timeout);
                    }
                    write.send(Message::Ping(Vec::new())).await?;
                }
            }
        }
    }

    fn on_text(self: &Arc<Self>, hub: &Arc<Hub>, backoff: &mut Backoff, text: &str) {
        match WsMessage::parse(text) {
            Ok(WsMessage::Ready(r)) => {
                self.session_id.store(Some(Arc::new(r.session_id.clone())));
                self.set_status(crate::NodeStatus::Ready);
                backoff.reset();
                self.enable_resume(r.session_id.clone());
                hub.emit(Event::Ready { node: self.index, resumed: r.resumed, session_id: r.session_id.into() });
                hub.node_ready(self, r.resumed);
            }
            Ok(WsMessage::Stats(s)) => {
                self.penalty.store(penalty(&s), Relaxed);
                self.players.store(s.players, Relaxed);
                let s = Arc::new(s);
                self.stats.store(Some(s.clone()));
                hub.emit(Event::Stats { node: self.index, stats: s });
            }
            Ok(msg) => hub.on_message(self, msg),
            Err(e) => tracing::warn!(node = self.index, error = %e, "ignoring unparseable frame"),
        }
    }

    fn enable_resume(self: &Arc<Self>, session: String) {
        let node = self.clone();
        tokio::spawn(async move {
            let upd = SessionUpdate { resuming: Some(true), timeout: Some(node.cfg.resume_timeout_secs) };
            if let Err(e) = node.rest.update_session(&session, &upd).await {
                tracing::warn!(node = node.index, error = %e, "failed to enable session resuming");
            }
        });
    }
}
```

Note: `set_status(crate::NodeStatus::Ready)` should simply be `set_status(NodeStatus::Ready)` (same module); keep it that way when typing it in.

- [ ] **Step 5: Implement `hub.rs`**

```rust
use crate::balancer::{pick, NodeView, Strategy};
use crate::node::Node;
use crate::state::PlayerInner;
use crate::{Error, Event, Result, TrackEndReason, VoiceGateway};
use dashmap::DashMap;
use straight_rs_model::{GuildId, UserId, WsMessage};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::{broadcast, Notify};

pub(crate) struct Hub {
    pub(crate) user_id: UserId,
    pub(crate) client_name: String,
    pub(crate) nodes: Vec<Arc<Node>>,
    pub(crate) players: DashMap<GuildId, Arc<PlayerInner>>,
    pub(crate) events: broadcast::Sender<Event>,
    pub(crate) strategy: Strategy,
    pub(crate) rr: AtomicUsize,
    pub(crate) ready: Notify,
    pub(crate) gateway: Option<Arc<dyn VoiceGateway>>,
}

impl Hub {
    pub(crate) fn emit(&self, ev: Event) {
        // Err only means "no receivers"; that is fine.
        let _ = self.events.send(ev);
    }

    /// Best ready node, optionally excluding one.
    pub(crate) fn pick_node(&self, exclude: Option<usize>) -> Option<Arc<Node>> {
        let views: Vec<NodeView> = self
            .nodes
            .iter()
            .filter(|n| n.is_ready() && Some(n.index) != exclude)
            .map(|n| NodeView { index: n.index, penalty: n.penalty(), players: n.players() })
            .collect();
        pick(&self.strategy, &views, &self.rr).map(|i| self.nodes[i].clone())
    }

    /// The node a player should talk to; assigns one on first use.
    pub(crate) fn node_for(&self, p: &PlayerInner) -> Result<Arc<Node>> {
        match p.node_index() {
            Some(i) => {
                let n = self.nodes.get(i).ok_or(Error::NoNode)?;
                if n.is_ready() { Ok(n.clone()) } else { Err(Error::NoNode) }
            }
            None => {
                let n = self.pick_node(None).ok_or(Error::NoNode)?;
                let i = p.assign_node(n.index);
                let n = self.nodes.get(i).ok_or(Error::NoNode)?;
                if n.is_ready() { Ok(n.clone()) } else { Err(Error::NoNode) }
            }
        }
    }

    pub(crate) fn node_ready(self: &Arc<Self>, node: &Arc<Node>, _resumed: bool) {
        node.bump_epoch();
        self.emit(Event::NodeConnected { node: node.index });
        self.ready.notify_waiters();
    }

    pub(crate) fn node_down(self: &Arc<Self>, node: &Arc<Node>) {
        node.bump_epoch();
        self.emit(Event::NodeDisconnected { node: node.index });
    }

    pub(crate) fn on_message(self: &Arc<Self>, node: &Arc<Node>, msg: WsMessage) {
        match msg {
            WsMessage::PlayerUpdate(u) => {
                if let Some(p) = self.players.get(&u.guild_id) {
                    if p.node_index() == Some(node.index) {
                        p.apply_update(&u.state);
                    } else {
                        return; // stale update from a node the player left
                    }
                }
                self.emit(Event::PlayerUpdate { node: node.index, guild: u.guild_id, state: u.state });
            }
            WsMessage::Event(ev) => {
                if let Some(p) = self.players.get(&ev.guild_id()) {
                    if p.node_index() != Some(node.index) {
                        return;
                    }
                    if let straight_rs_model::Event::TrackEnd { track, reason, .. } = &ev {
                        if *reason != TrackEndReason::Replaced {
                            p.clear_track_if(&track.encoded);
                        }
                    }
                }
                self.emit(Event::from_model(node.index, ev));
            }
            WsMessage::Unknown { op, payload } => {
                self.emit(Event::Unknown { node: node.index, op, payload });
            }
            WsMessage::Ready(_) | WsMessage::Stats(_) => {}
        }
    }
}
```

- [ ] **Step 6: Implement `client.rs`**

```rust
use crate::balancer::Strategy;
use crate::hub::Hub;
use crate::node::Node;
use crate::{Error, Event, NodeConfig, Result, VoiceGateway};
use dashmap::DashMap;
use straight_rs_model::{LoadResult, Track, UserId};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch, Notify};

const DEFAULT_CLIENT_NAME: &str = concat!("straight-rs/", env!("CARGO_PKG_VERSION"));

struct ShutdownGuard(watch::Sender<bool>);
impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

/// Handle to the node pool. Cheap to clone. Dropping the last clone stops all node tasks.
#[derive(Clone)]
pub struct LavalinkClient {
    pub(crate) hub: Arc<Hub>,
    guard: Arc<ShutdownGuard>,
}

pub struct ClientBuilder {
    user_id: UserId,
    nodes: Vec<NodeConfig>,
    strategy: Strategy,
    client_name: String,
    event_capacity: usize,
    gateway: Option<Arc<dyn VoiceGateway>>,
}

impl ClientBuilder {
    pub fn node(mut self, cfg: NodeConfig) -> Self { self.nodes.push(cfg); self }
    pub fn strategy(mut self, s: Strategy) -> Self { self.strategy = s; self }
    pub fn client_name(mut self, n: impl Into<String>) -> Self { self.client_name = n.into(); self }
    pub fn event_capacity(mut self, n: usize) -> Self { self.event_capacity = n.max(1); self }
    pub fn gateway(mut self, g: Arc<dyn VoiceGateway>) -> Self { self.gateway = Some(g); self }

    /// Spawns one task per node; does not wait for connections (see `wait_ready`).
    pub async fn build(self) -> Result<LavalinkClient> {
        if self.nodes.is_empty() {
            return Err(Error::Config("at least one node is required".into()));
        }
        #[cfg(feature = "tls")]
        {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }
        let nodes: Vec<Arc<Node>> = self
            .nodes
            .into_iter()
            .enumerate()
            .map(|(i, cfg)| Node::new(i, cfg, &self.client_name))
            .collect();
        let (events, _) = broadcast::channel(self.event_capacity);
        let hub = Arc::new(Hub {
            user_id: self.user_id,
            client_name: self.client_name,
            nodes,
            players: DashMap::new(),
            events,
            strategy: self.strategy,
            rr: AtomicUsize::new(0),
            ready: Notify::new(),
            gateway: self.gateway,
        });
        let (tx, rx) = watch::channel(false);
        for node in &hub.nodes {
            tokio::spawn(node.clone().run(hub.clone(), rx.clone()));
        }
        Ok(LavalinkClient { hub, guard: Arc::new(ShutdownGuard(tx)) })
    }
}

impl LavalinkClient {
    pub fn builder(user_id: impl Into<UserId>) -> ClientBuilder {
        ClientBuilder {
            user_id: user_id.into(),
            nodes: Vec::new(),
            strategy: Strategy::default(),
            client_name: DEFAULT_CLIENT_NAME.to_owned(),
            event_capacity: 1024,
            gateway: None,
        }
    }

    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.hub.events.subscribe()
    }

    pub fn nodes(&self) -> &[Arc<Node>] {
        &self.hub.nodes
    }

    /// Waits until at least one node is ready.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.hub.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.hub.nodes.iter().any(|n| n.is_ready()) {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(Error::Timeout);
            }
        }
    }

    pub async fn load(&self, identifier: &str) -> Result<LoadResult> {
        self.hub.pick_node(None).ok_or(Error::NoNode)?.rest().load_tracks(identifier).await
    }

    pub async fn decode_track(&self, encoded: &str) -> Result<Track> {
        self.hub.pick_node(None).ok_or(Error::NoNode)?.rest().decode_track(encoded).await
    }

    pub async fn decode_tracks(&self, encoded: &[String]) -> Result<Vec<Track>> {
        self.hub.pick_node(None).ok_or(Error::NoNode)?.rest().decode_tracks(encoded).await
    }

    /// Stops all node tasks immediately.
    pub fn shutdown(&self) {
        let _ = self.guard.0.send(true);
    }
}
```

`lib.rs` additions: `mod client; mod hub; mod node;` and
`pub use client::{ClientBuilder, LavalinkClient}; pub use node::{Node, NodeStatus};`

- [ ] **Step 7: Run to verify pass**

Run: `cargo test -p straight-rs --test node`
Expected: PASS (8 tests). `RUST_LOG` is not needed; use `--nocapture` only when debugging.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat: node connection loop, hub, client builder"
```

---

### Task 7: Player handle and commands

**Files:**
- Create: `straight-rs/src/player.rs`, `straight-rs/tests/player.rs`
- Modify: `straight-rs/src/lib.rs`

**Interfaces:**
- Consumes: `Hub::node_for`, `PlayerInner`, `RestClient::{update_player, destroy_player}`, `Event`.
- Produces:
  - `LavalinkClient::player(impl Into<GuildId>) -> Player` (creates lazily), `LavalinkClient::get_player(impl Into<GuildId>) -> Option<Player>`.
  - `Player` (Clone): sync reads `guild_id()`, `snapshot() -> Arc<PlayerSnapshot>`, `position() -> u64`, `track() -> Option<Arc<Track>>`, `is_paused()`, `volume()`, `node_index() -> Option<usize>`, `events() -> PlayerEvents`; async writes `update(UpdatePlayer)`, `update_with(UpdatePlayer, no_replace: bool)`, `play(&Track)`, `stop()`, `pause(bool)`, `seek(u64)`, `set_volume(u16)` (clamped to 1000), `set_filters(Filters)`, `destroy()`, `join(ChannelId)`, `leave()` (need `.gateway(..)`).
  - `PlayerEvents::recv() -> Result<Event, broadcast::error::RecvError>` (only this guild's events; `Lagged(n)` is surfaced).

- [ ] **Step 1: Write failing tests** — `straight-rs/tests/player.rs`

```rust
mod common;
use common::*;
use straight_rs::{Error, Event, Filters, GuildId};
use serde_json::json;
use std::sync::atomic::Ordering::SeqCst;
use std::time::Duration;

const G: &str = "/v4/sessions/mock-session/players/42";

fn track_end(enc: &str, reason: &str) -> String {
    format!(r#"{{"op":"event","type":"TrackEndEvent","guildId":"42","track":{},"reason":"{reason}"}}"#, track_json(enc))
}

#[tokio::test]
async fn play_sends_patch_and_updates_snapshot() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    let r = mock.requests_matching("PATCH", G);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].body["track"]["encoded"], "ABC");
    assert_eq!(r[0].query, "noReplace=false");
    assert_eq!(&*p.track().unwrap().encoded, "ABC");
    assert_eq!(p.node_index(), Some(0));
}

#[tokio::test]
async fn stop_pause_seek_volume_filters() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    p.pause(true).await.unwrap();
    assert!(p.is_paused());
    p.seek(5_000).await.unwrap();
    assert_eq!(p.snapshot().position, 5_000);
    p.set_volume(5_000).await.unwrap();
    assert_eq!(p.volume(), 1000);
    let f = Filters { volume: Some(0.5), ..Default::default() };
    p.set_filters(f.clone()).await.unwrap();
    assert_eq!(p.snapshot().filters, f);
    p.stop().await.unwrap();
    assert!(p.track().is_none());
    let bodies: Vec<_> = mock.requests_matching("PATCH", G).into_iter().map(|r| r.body).collect();
    assert_eq!(bodies[1], json!({"paused": true}));
    assert_eq!(bodies[2], json!({"position": 5000}));
    assert_eq!(bodies[3], json!({"volume": 1000}));
    assert_eq!(bodies[4], json!({"filters": {"volume": 0.5}}));
    assert_eq!(bodies[5], json!({"track": {"encoded": null}}));
}

#[tokio::test]
async fn position_interpolates_between_updates_and_freezes_when_paused() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    mock.push_text(r#"{"op":"playerUpdate","guildId":"42","state":{"time":0,"position":10000,"connected":true,"ping":5}}"#);
    eventually(Duration::from_secs(5), || p.position() >= 10_000).await;
    let a = p.position();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(p.position() >= a + 90);
    p.pause(true).await.unwrap();
    let frozen = p.position();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(p.position(), frozen);
}

#[tokio::test]
async fn writes_for_one_guild_never_overlap_and_keep_call_order() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.set_volume(1).await.unwrap(); // assign node
    mock.state.patch_delay_ms.store(150, SeqCst);
    let (a, b) = (p.clone(), p.clone());
    let t1 = tokio::spawn(async move { a.set_volume(10).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let t2 = tokio::spawn(async move { b.set_volume(20).await.unwrap() });
    t1.await.unwrap();
    t2.await.unwrap();
    assert_eq!(mock.state.max_in_flight.load(SeqCst), 1);
    let vols: Vec<_> = mock.requests_matching("PATCH", G).iter().map(|r| r.body["volume"].as_u64().unwrap()).collect();
    assert_eq!(vols, vec![1, 10, 20]);
    assert_eq!(p.volume(), 20);
}

#[tokio::test]
async fn track_end_clears_track_except_when_replaced_or_other_track() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    let mut rx = c.events();
    p.play(&sample_track("ABC")).await.unwrap();
    mock.push_text(track_end("ABC", "replaced"));
    next_event(&mut rx, |e| matches!(e, Event::TrackEnd { .. })).await;
    assert!(p.track().is_some());
    mock.push_text(track_end("OTHER", "finished"));
    next_event(&mut rx, |e| matches!(e, Event::TrackEnd { .. })).await;
    assert!(p.track().is_some());
    mock.push_text(track_end("ABC", "finished"));
    let e = next_event(&mut rx, |e| matches!(e, Event::TrackEnd { .. })).await;
    assert!(e.may_start_next());
    eventually(Duration::from_secs(5), || p.track().is_none()).await;
}

#[tokio::test]
async fn player_events_only_yield_own_guild() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    let mut ev = p.events();
    let other = format!(r#"{{"op":"event","type":"TrackStartEvent","guildId":"43","track":{}}}"#, track_json("X"));
    let mine = format!(r#"{{"op":"event","type":"TrackStartEvent","guildId":"42","track":{}}}"#, track_json("Y"));
    mock.push_text(other);
    mock.push_text(mine);
    let e = tokio::time::timeout(Duration::from_secs(5), ev.recv()).await.unwrap().unwrap();
    assert_eq!(e.guild(), Some(GuildId(42)));
}

#[tokio::test]
async fn destroy_deletes_on_node_and_forgets_player() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let p = c.player(GuildId(42));
    p.play(&sample_track("ABC")).await.unwrap();
    p.destroy().await.unwrap();
    assert_eq!(mock.requests_matching("DELETE", G).len(), 1);
    assert!(c.get_player(GuildId(42)).is_none());
}

#[tokio::test]
async fn writes_fail_with_no_node_when_pool_is_down() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    mock.kill();
    eventually(Duration::from_secs(5), || !c.nodes()[0].is_ready()).await;
    let e = c.player(GuildId(1)).play(&sample_track("A")).await.unwrap_err();
    assert!(matches!(e, Error::NoNode));
}

#[tokio::test]
async fn join_without_gateway_is_a_config_error() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let e = c.player(GuildId(1)).join(straight_rs::ChannelId(2)).await.unwrap_err();
    assert!(matches!(e, Error::Config(_)));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p straight-rs --test player`
Expected: FAIL — `LavalinkClient::player` does not exist.

- [ ] **Step 3: Implement `straight-rs/src/player.rs`**

```rust
use crate::hub::Hub;
use crate::state::{PlayerInner, PlayerSnapshot};
use crate::{ChannelId, Error, Event, Filters, GuildId, LavalinkClient, Result, Track, UpdatePlayer, UpdateTrack};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// Cheap, cloneable handle to one guild's player.
#[derive(Clone)]
pub struct Player {
    pub(crate) hub: Arc<Hub>,
    pub(crate) inner: Arc<PlayerInner>,
}

/// Events of a single guild.
pub struct PlayerEvents {
    rx: broadcast::Receiver<Event>,
    guild: GuildId,
}

impl PlayerEvents {
    pub async fn recv(&mut self) -> std::result::Result<Event, RecvError> {
        loop {
            let e = self.rx.recv().await?;
            if e.guild() == Some(self.guild) {
                return Ok(e);
            }
        }
    }
}

impl LavalinkClient {
    /// Returns the player for `guild`, creating local state if needed.
    pub fn player(&self, guild: impl Into<GuildId>) -> Player {
        let guild = guild.into();
        let inner = self
            .hub
            .players
            .entry(guild)
            .or_insert_with(|| Arc::new(PlayerInner::new(guild)))
            .clone();
        Player { hub: self.hub.clone(), inner }
    }

    pub fn get_player(&self, guild: impl Into<GuildId>) -> Option<Player> {
        let inner = self.hub.players.get(&guild.into())?.clone();
        Some(Player { hub: self.hub.clone(), inner })
    }
}

impl Player {
    pub fn guild_id(&self) -> GuildId { self.inner.guild }
    pub fn snapshot(&self) -> Arc<PlayerSnapshot> { self.inner.snapshot() }
    /// Interpolated position in ms; never awaits.
    pub fn position(&self) -> u64 { self.inner.snapshot().position_now() }
    pub fn track(&self) -> Option<Arc<Track>> { self.inner.snapshot().track.clone() }
    pub fn is_paused(&self) -> bool { self.inner.snapshot().paused }
    pub fn volume(&self) -> u16 { self.inner.snapshot().volume }
    pub fn node_index(&self) -> Option<usize> { self.inner.node_index() }

    pub fn events(&self) -> PlayerEvents {
        PlayerEvents { rx: self.hub.events.subscribe(), guild: self.inner.guild }
    }

    pub async fn update(&self, upd: UpdatePlayer) -> Result<()> {
        self.update_with(upd, false).await
    }

    /// `no_replace = true` keeps the current track if one is playing.
    pub async fn update_with(&self, upd: UpdatePlayer, no_replace: bool) -> Result<()> {
        let _gate = self.inner.gate.lock().await;
        let node = self.hub.node_for(&self.inner)?;
        let sid = node.session_id().ok_or(Error::NoNode)?;
        let resp = node.rest().update_player(&sid, self.inner.guild, &upd, no_replace).await?;
        self.inner.apply_player(&resp);
        Ok(())
    }

    pub async fn play(&self, track: &Track) -> Result<()> {
        let user_data = (!track.user_data.is_null()).then(|| track.user_data.clone());
        self.update(UpdatePlayer {
            track: Some(UpdateTrack { encoded: Some(Some(track.encoded.clone())), user_data, ..Default::default() }),
            ..Default::default()
        })
        .await
    }

    pub async fn stop(&self) -> Result<()> {
        self.update(UpdatePlayer {
            track: Some(UpdateTrack { encoded: Some(None), ..Default::default() }),
            ..Default::default()
        })
        .await
    }

    pub async fn pause(&self, paused: bool) -> Result<()> {
        self.update(UpdatePlayer { paused: Some(paused), ..Default::default() }).await
    }

    pub async fn seek(&self, position_ms: u64) -> Result<()> {
        self.update(UpdatePlayer { position: Some(position_ms), ..Default::default() }).await
    }

    /// Lavalink accepts 0..=1000.
    pub async fn set_volume(&self, volume: u16) -> Result<()> {
        self.update(UpdatePlayer { volume: Some(volume.min(1000)), ..Default::default() }).await
    }

    pub async fn set_filters(&self, filters: Filters) -> Result<()> {
        self.update(UpdatePlayer { filters: Some(filters), ..Default::default() }).await
    }

    /// Destroys the player on its node and forgets local state. A 404 is not an error.
    pub async fn destroy(&self) -> Result<()> {
        let _gate = self.inner.gate.lock().await;
        let guild = self.inner.guild;
        self.hub.players.remove_if(&guild, |_, v| Arc::ptr_eq(v, &self.inner));
        let Some(node) = self.inner.node_index().and_then(|i| self.hub.nodes.get(i)) else {
            return Ok(());
        };
        let Some(sid) = node.session_id() else { return Ok(()) };
        if !node.is_ready() {
            return Ok(());
        }
        match node.rest().destroy_player(&sid, guild).await {
            Err(Error::Lavalink { status: 404, .. }) => Ok(()),
            r => r,
        }
    }

    /// Joins a voice channel through the configured `VoiceGateway`.
    pub async fn join(&self, channel: ChannelId) -> Result<()> {
        self.gateway()?.join(self.inner.guild, channel).await
    }

    pub async fn leave(&self) -> Result<()> {
        self.gateway()?.leave(self.inner.guild).await
    }

    fn gateway(&self) -> Result<&Arc<dyn crate::VoiceGateway>> {
        self.hub.gateway.as_ref().ok_or_else(|| Error::Config("no voice gateway configured".into()))
    }
}
```

`lib.rs` additions: `mod player; pub use player::{Player, PlayerEvents};`

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p straight-rs --test player`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: player handle with ordered writes, position interpolation, events"
```

---

### Task 8: Voice state/server updates

**Files:**
- Modify: `straight-rs/src/player.rs` (append), `straight-rs/src/lib.rs`
- Create: `straight-rs/tests/voice.rs`

**Interfaces:**
- Consumes: `VoiceAssembler`, `VoiceOutcome`, `Player::update`, `Player::destroy`.
- Produces on `LavalinkClient`: `async voice_state_update(impl Into<GuildId>, VoiceStateUpdate) -> Result<()>`, `async voice_server_update(impl Into<GuildId>, VoiceServerUpdate) -> Result<()>`, `async voice_update(impl Into<GuildId>, VoiceState) -> Result<()>` (for libraries such as songbird that hand over a complete connection).
  Callers must only pass voice events for **the bot's own user**.

- [ ] **Step 1: Write failing tests** — `straight-rs/tests/voice.rs`

```rust
mod common;
use common::*;
use straight_rs::{BoxFuture, ChannelId, GuildId, LavalinkClient, NodeConfig, Result, UserId, VoiceGateway, VoiceServerUpdate, VoiceState, VoiceStateUpdate};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const P7: &str = "/v4/sessions/mock-session/players/7";
fn su(ch: Option<u64>) -> VoiceStateUpdate { VoiceStateUpdate { channel_id: ch.map(ChannelId), session_id: "sess".into() } }
fn sv(e: Option<&str>) -> VoiceServerUpdate { VoiceServerUpdate { token: "tok".into(), endpoint: e.map(Into::into) } }
fn voice_body() -> serde_json::Value { json!({"token":"tok","endpoint":"e:443","sessionId":"sess","channelId":"9"}) }

#[tokio::test]
async fn state_then_server_sends_exactly_one_patch() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    assert!(mock.requests_matching("PATCH", P7).is_empty(), "must wait for the server update");
    c.voice_server_update(GuildId(7), sv(Some("e:443"))).await.unwrap();
    let r = mock.requests_matching("PATCH", P7);
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].body["voice"], voice_body());
}

#[tokio::test]
async fn server_before_state_works() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_server_update(GuildId(7), sv(Some("e:443"))).await.unwrap();
    assert!(mock.requests_matching("PATCH", P7).is_empty());
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7)[0].body["voice"], voice_body());
}

#[tokio::test]
async fn null_endpoint_sends_nothing_and_duplicates_are_suppressed() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    c.voice_server_update(GuildId(7), sv(None)).await.unwrap();
    assert!(mock.requests_matching("PATCH", P7).is_empty());
    c.voice_server_update(GuildId(7), sv(Some("e:443"))).await.unwrap();
    c.voice_server_update(GuildId(7), sv(Some("e:443"))).await.unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7).len(), 1);
    c.voice_server_update(GuildId(7), sv(Some("other:443"))).await.unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7).len(), 2);
}

#[tokio::test]
async fn leaving_destroys_the_player() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(7), su(Some(9))).await.unwrap();
    c.voice_server_update(GuildId(7), sv(Some("e:443"))).await.unwrap();
    c.voice_state_update(GuildId(7), su(None)).await.unwrap();
    assert_eq!(mock.requests_matching("DELETE", P7).len(), 1);
    assert!(c.get_player(GuildId(7)).is_none());
}

#[tokio::test]
async fn leaving_a_never_joined_guild_is_a_noop() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    c.voice_state_update(GuildId(8), su(None)).await.unwrap();
    assert!(mock.requests_matching("DELETE", "/v4/sessions").is_empty());
    assert!(c.get_player(GuildId(8)).is_none());
}

#[tokio::test]
async fn direct_voice_update_for_complete_connections() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let vs = VoiceState { token: "tok".into(), endpoint: "e:443".into(), session_id: "sess".into(), channel_id: Some(ChannelId(9)) };
    c.voice_update(GuildId(7), vs).await.unwrap();
    assert_eq!(mock.requests_matching("PATCH", P7)[0].body["voice"], voice_body());
}

struct Fake(Mutex<Vec<String>>);
impl VoiceGateway for Fake {
    fn join(&self, g: GuildId, c: ChannelId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.0.lock().unwrap().push(format!("join {g} {c}")); Ok(()) })
    }
    fn leave(&self, g: GuildId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.0.lock().unwrap().push(format!("leave {g}")); Ok(()) })
    }
}

#[tokio::test]
async fn join_and_leave_go_through_the_gateway() {
    let mock = Mock::start().await;
    let fake = Arc::new(Fake(Mutex::new(vec![])));
    let c = LavalinkClient::builder(UserId(1)).node(NodeConfig::new(mock.host(), "pw")).gateway(fake.clone()).build().await.unwrap();
    c.wait_ready(Duration::from_secs(5)).await.unwrap();
    let p = c.player(GuildId(7));
    p.join(ChannelId(9)).await.unwrap();
    p.leave().await.unwrap();
    assert_eq!(*fake.0.lock().unwrap(), vec!["join 7 9", "leave 7"]);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p straight-rs --test voice`
Expected: FAIL — `voice_state_update` does not exist.

- [ ] **Step 3: Implement** — append to `straight-rs/src/player.rs`

```rust
use crate::state::lock;
use crate::{VoiceOutcome, VoiceServerUpdate, VoiceState, VoiceStateUpdate};

impl Player {
    pub(crate) async fn apply_voice(&self, outcome: VoiceOutcome) -> Result<()> {
        match outcome {
            VoiceOutcome::Pending => Ok(()),
            VoiceOutcome::Ready(vs) => {
                self.update(UpdatePlayer { voice: Some(vs.clone()), ..Default::default() }).await?;
                lock(&self.inner.voice).mark_sent(vs);
                Ok(())
            }
            VoiceOutcome::Left => self.destroy().await,
        }
    }
}

impl LavalinkClient {
    /// Feed the bot's own `VOICE_STATE_UPDATE`. `channel_id: None` destroys the player.
    pub async fn voice_state_update(&self, guild: impl Into<GuildId>, upd: VoiceStateUpdate) -> Result<()> {
        let p = self.player(guild);
        let outcome = lock(&p.inner.voice).update_state(upd);
        p.apply_voice(outcome).await
    }

    /// Feed `VOICE_SERVER_UPDATE`.
    pub async fn voice_server_update(&self, guild: impl Into<GuildId>, upd: VoiceServerUpdate) -> Result<()> {
        let p = self.player(guild);
        let outcome = lock(&p.inner.voice).update_server(upd);
        p.apply_voice(outcome).await
    }

    /// Hand over a complete voice connection (e.g. from songbird).
    pub async fn voice_update(&self, guild: impl Into<GuildId>, vs: VoiceState) -> Result<()> {
        self.player(guild).apply_voice(VoiceOutcome::Ready(vs)).await
    }
}
```

Merge the new `use` lines into the existing import block of `player.rs` when typing it in.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p straight-rs --test voice && cargo test -p straight-rs --test player`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: voice state/server update handling and gateway join/leave"
```

---

### Task 9: Resume restore, failover and orphan rescue

**Files:**
- Modify: `straight-rs/src/hub.rs`, `straight-rs/src/client.rs`
- Create: `straight-rs/tests/failover.rs`

**Interfaces:**
- Consumes: `Hub::{pick_node, emit}`, `PlayerInner::{restore_payload, apply_player, set_node, orphaned}`, `Node::{epoch, bump_epoch, cfg.failover_grace}`.
- Produces on `Hub` (all `pub(crate)`): `restore_to(&self, &Arc<PlayerInner>, &Arc<Node>) -> Result<()>`, `restore_players(&self, &Arc<Node>)`, `migrate_from(self: &Arc<Self>, usize)`, `rescue_orphans(&self, &Arc<Node>)`, `cleanup_stale(&self, &Arc<Node>, resumed: bool)`; field `stale: DashSet<(usize, GuildId)>`; behavior: `resumed=false` → players re-created on the same node; node down longer than `failover_grace` → players migrate and `Event::PlayerMigrated{guild, from, to}` is emitted; no target → players orphaned and rescued when any node becomes ready; when an old node returns with `resumed=true`, migrated-away players are deleted there (no ghost audio).

- [ ] **Step 1: Write failing tests** — `straight-rs/tests/failover.rs`

```rust
mod common;
use common::*;
use straight_rs::{ChannelId, Error, Event, GuildId, VoiceServerUpdate, VoiceStateUpdate};
use std::time::Duration;

const G: &str = "/v4/sessions/mock-session/players/42";
const LONG: Duration = Duration::from_secs(20);

async fn join_and_play(c: &straight_rs::LavalinkClient) -> straight_rs::Player {
    let p = c.player(GuildId(42));
    c.voice_state_update(GuildId(42), VoiceStateUpdate { channel_id: Some(ChannelId(9)), session_id: "sess".into() }).await.unwrap();
    c.voice_server_update(GuildId(42), VoiceServerUpdate { token: "tok".into(), endpoint: Some("e:443".into()) }).await.unwrap();
    p.play(&sample_track("ABC")).await.unwrap();
    p
}

#[tokio::test]
async fn resumed_false_recreates_players_on_the_same_node() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let _p = join_and_play(&c).await;
    let before = mock.requests_matching("PATCH", G).len();
    mock.close_ws(); // mock says resumed=false on reconnect
    eventually(LONG, || mock.requests_matching("PATCH", G).len() > before).await;
    let last = mock.requests_matching("PATCH", G).pop().unwrap().body;
    assert_eq!(last["track"]["encoded"], "ABC");
    assert_eq!(last["voice"]["token"], "tok");
    assert_eq!(last["volume"], 100);
    assert_eq!(last["paused"], false);
}

#[tokio::test]
async fn resumed_true_does_not_resend_state() {
    let mock = Mock::start().await;
    let c = client(&[&mock]).await;
    let _p = join_and_play(&c).await;
    let before = mock.requests_matching("PATCH", G).len();
    let mut rx = c.events();
    mock.set_resumed(true);
    mock.close_ws();
    next_event(&mut rx, |e| matches!(e, Event::NodeConnected { .. })).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(mock.requests_matching("PATCH", G).len(), before);
}

#[tokio::test]
async fn players_migrate_to_another_node_after_grace() {
    let (a, b) = (Mock::start().await, Mock::start().await);
    let c = client_with(&[&a, &b], |cfg| cfg.failover_grace = Duration::from_millis(300)).await;
    let p = join_and_play(&c).await;
    assert_eq!(p.node_index(), Some(0));
    let mut rx = c.events();
    a.kill();
    let e = next_event(&mut rx, |e| matches!(e, Event::PlayerMigrated { .. })).await;
    assert!(matches!(e, Event::PlayerMigrated { from: 0, to: 1, .. }));
    assert_eq!(p.node_index(), Some(1));
    let last = b.requests_matching("PATCH", G).pop().unwrap().body;
    assert_eq!(last["track"]["encoded"], "ABC");
    assert_eq!(last["voice"]["token"], "tok");
    // player keeps working on the new node
    p.set_volume(30).await.unwrap();
}

#[tokio::test]
async fn short_outage_within_grace_does_not_migrate() {
    let (a, b) = (Mock::start().await, Mock::start().await);
    let c = client_with(&[&a, &b], |cfg| cfg.failover_grace = Duration::from_secs(30)).await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    a.close_ws(); // drops the socket; the server stays up so it reconnects
    next_event(&mut rx, |e| matches!(e, Event::NodeConnected { .. })).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(p.node_index(), Some(0));
    assert!(b.requests_matching("PATCH", G).is_empty());
}

#[tokio::test]
async fn orphaned_players_are_rescued_when_the_node_returns() {
    let mock = Mock::start().await;
    let addr = mock.addr.to_string();
    let c = client_with(&[&mock], |cfg| cfg.failover_grace = Duration::from_millis(200)).await;
    let p = join_and_play(&c).await;
    mock.kill();
    eventually(LONG, || !c.nodes()[0].is_ready()).await;
    tokio::time::sleep(Duration::from_millis(500)).await; // grace elapsed, nowhere to go
    assert!(matches!(p.pause(true).await.unwrap_err(), Error::NoNode));
    let back = Mock::start_on(&addr, false).await;
    eventually(LONG, || !back.requests_matching("PATCH", G).is_empty()).await;
    let body = back.requests_matching("PATCH", G).pop().unwrap().body;
    assert_eq!(body["track"]["encoded"], "ABC");
    assert_eq!(body["voice"]["token"], "tok");
    p.pause(true).await.unwrap(); // usable again
}

#[tokio::test]
async fn migrated_away_player_is_deleted_when_old_node_returns_resumed() {
    let (a, b) = (Mock::start().await, Mock::start().await);
    let a_addr = a.addr.to_string();
    let c = client_with(&[&a, &b], |cfg| cfg.failover_grace = Duration::from_millis(300)).await;
    let p = join_and_play(&c).await;
    let mut rx = c.events();
    a.kill();
    next_event(&mut rx, |e| matches!(e, Event::PlayerMigrated { .. })).await;
    let a2 = Mock::start_on(&a_addr, true).await; // server kept the old session
    eventually(LONG, || !a2.requests_matching("DELETE", G).is_empty()).await;
    assert_eq!(p.node_index(), Some(1));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p straight-rs --test failover`
Expected: FAIL (players are not restored or migrated yet; assertions/timeouts fail).

- [ ] **Step 3: Implement**

`client.rs`: `use dashmap::{DashMap, DashSet};`, add `stale: DashSet::new(),` to the `Hub { .. }` literal.

`hub.rs`: add `use dashmap::DashSet;`, `use std::sync::atomic::Ordering;`, and the field
```rust
    /// (old node, guild) pairs whose player was moved away and may still exist on the old node.
    pub(crate) stale: DashSet<(usize, GuildId)>,
```
Replace `node_ready` and `node_down` with:

```rust
    pub(crate) fn node_ready(self: &Arc<Self>, node: &Arc<Node>, resumed: bool) {
        node.bump_epoch(); // cancels a pending failover timer
        self.emit(Event::NodeConnected { node: node.index });
        self.ready.notify_waiters();
        let hub = self.clone();
        let node = node.clone();
        tokio::spawn(async move {
            hub.cleanup_stale(&node, resumed).await;
            if !resumed {
                hub.restore_players(&node).await;
            }
            hub.rescue_orphans(&node).await;
        });
    }

    pub(crate) fn node_down(self: &Arc<Self>, node: &Arc<Node>) {
        let epoch = node.bump_epoch();
        self.emit(Event::NodeDisconnected { node: node.index });
        let hub = self.clone();
        let node = node.clone();
        tokio::spawn(async move {
            tokio::time::sleep(node.cfg.failover_grace).await;
            if node.epoch() == epoch && !node.is_ready() {
                hub.migrate_from(node.index).await;
            }
        });
    }
```

Append to `impl Hub`:

```rust
    /// Re-create `p` on `node` from client-side state.
    pub(crate) async fn restore_to(&self, p: &Arc<PlayerInner>, node: &Arc<Node>) -> Result<()> {
        let _gate = p.gate.lock().await;
        if let Some(upd) = p.restore_payload() {
            let sid = node.session_id().ok_or(Error::NoNode)?;
            let resp = node.rest().update_player(&sid, p.guild, &upd, false).await?;
            p.apply_player(&resp);
        }
        p.set_node(node.index);
        p.orphaned.store(false, Ordering::Release);
        Ok(())
    }

    fn players_where(&self, f: impl Fn(&PlayerInner) -> bool) -> Vec<Arc<PlayerInner>> {
        self.players.iter().filter(|e| f(e.value())).map(|e| e.value().clone()).collect()
    }

    /// Server lost our session: put every player of `node` back.
    pub(crate) async fn restore_players(&self, node: &Arc<Node>) {
        let idx = node.index;
        for p in self.players_where(|p| p.node_index() == Some(idx) && !p.orphaned.load(Ordering::Acquire)) {
            if let Err(e) = self.restore_to(&p, node).await {
                tracing::warn!(guild = %p.guild, error = %e, "failed to restore player after session loss");
            }
        }
    }

    /// Node stayed down past the grace period: move its players elsewhere.
    pub(crate) async fn migrate_from(self: &Arc<Self>, idx: usize) {
        for p in self.players_where(|p| p.node_index() == Some(idx)) {
            let Some(target) = self.pick_node(Some(idx)) else {
                p.orphaned.store(true, Ordering::Release);
                continue;
            };
            match self.restore_to(&p, &target).await {
                Ok(()) => {
                    self.stale.insert((idx, p.guild));
                    self.emit(Event::PlayerMigrated { guild: p.guild, from: idx, to: target.index });
                }
                Err(e) => {
                    tracing::warn!(guild = %p.guild, error = %e, "player migration failed");
                    p.orphaned.store(true, Ordering::Release);
                }
            }
        }
    }

    /// A node became ready: adopt players that had nowhere to go.
    pub(crate) async fn rescue_orphans(&self, node: &Arc<Node>) {
        for p in self.players_where(|p| p.orphaned.load(Ordering::Acquire)) {
            let from = p.node_index();
            match self.restore_to(&p, node).await {
                Ok(()) => {
                    if let Some(from) = from.filter(|f| *f != node.index) {
                        self.stale.insert((from, p.guild));
                        self.emit(Event::PlayerMigrated { guild: p.guild, from, to: node.index });
                    }
                }
                Err(e) => tracing::warn!(guild = %p.guild, error = %e, "orphan rescue failed"),
            }
        }
    }

    /// An old node is back: remove players we migrated away from it.
    pub(crate) async fn cleanup_stale(&self, node: &Arc<Node>, resumed: bool) {
        let mine: Vec<GuildId> = self.stale.iter().filter(|e| e.0 == node.index).map(|e| e.1).collect();
        for guild in mine {
            self.stale.remove(&(node.index, guild));
            if !resumed {
                continue; // the server forgot the player already
            }
            let back_here = self.players.get(&guild).is_some_and(|p| p.node_index() == Some(node.index));
            if back_here {
                continue;
            }
            if let Some(sid) = node.session_id() {
                if let Err(e) = node.rest().destroy_player(&sid, guild).await {
                    tracing::debug!(guild = %guild, error = %e, "stale player cleanup failed");
                }
            }
        }
    }
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p straight-rs --test failover -- --test-threads=1`, then `cargo test -p straight-rs` (all suites)
Expected: PASS. The failover tests use real reconnect backoff (≤ ~10 s each); they must not be flaky — if one is, raise its `LONG` timeout, do not remove the assertion.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: session-loss restore, node failover, orphan rescue, stale cleanup"
```

---

### Task 10: Discord library adapters

**Files:**
- Create: `straight-rs/src/adapters/{mod,twilight,serenity,songbird}.rs`
- Modify: `straight-rs/src/lib.rs` (`pub mod adapters;`)

**Interfaces:**
- Consumes: `VoiceStateUpdate`, `VoiceServerUpdate`, `VoiceState`, `GuildId`, `ChannelId`, `UserId`.
- Produces (each behind its feature flag, conversion only, no logic beyond field mapping and the "only the bot's own voice state" filter):
  - `adapters::twilight::{voice_state(&twilight_model::voice::VoiceState, bot: UserId) -> Option<(GuildId, VoiceStateUpdate)>, voice_server(&twilight_model::gateway::payload::incoming::VoiceServerUpdate) -> (GuildId, VoiceServerUpdate)}`
  - `adapters::serenity::{voice_state(&serenity::model::voice::VoiceState, bot: UserId) -> Option<(GuildId, VoiceStateUpdate)>, voice_server(&serenity::model::event::VoiceServerUpdateEvent) -> Option<(GuildId, VoiceServerUpdate)>}`
  - `adapters::songbird::connection_info(&songbird::ConnectionInfo) -> (GuildId, VoiceState)` (feed into `LavalinkClient::voice_update`).

External crates change shape between versions: the field access below matches twilight-model 0.16, serenity 0.12, songbird 0.4. If the pinned versions differ, adjust the field access and the test JSON/literals to the version's docs.rs, keeping the function signatures identical.

- [ ] **Step 1: Write failing tests** (inline in each adapter file, gated by the feature)

`twilight.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    fn vs(user: u64, channel: Option<u64>) -> TwState {
        serde_json::from_value(serde_json::json!({
            "channel_id": channel.map(|c| c.to_string()), "deaf": false, "guild_id": "100", "mute": false,
            "self_deaf": false, "self_mute": false, "self_video": false, "session_id": "sess",
            "suppress": false, "user_id": user.to_string(), "request_to_speak_timestamp": null
        })).unwrap()
    }
    #[test] fn maps_own_voice_state_and_ignores_others() {
        let (g, u) = voice_state(&vs(1, Some(9)), UserId(1)).unwrap();
        assert_eq!((g, u.channel_id, u.session_id.as_str()), (GuildId(100), Some(ChannelId(9)), "sess"));
        assert!(voice_state(&vs(2, Some(9)), UserId(1)).is_none());
        assert_eq!(voice_state(&vs(1, None), UserId(1)).unwrap().1.channel_id, None);
    }
    #[test] fn maps_voice_server() {
        let ev: TwServer = serde_json::from_value(serde_json::json!({"endpoint":"e:443","guild_id":"100","token":"tok"})).unwrap();
        let (g, u) = voice_server(&ev);
        assert_eq!((g, u.token.as_str(), u.endpoint.as_deref()), (GuildId(100), "tok", Some("e:443")));
    }
}
```

`serenity.rs`: the same two tests using `SerVoiceState` / `VoiceServerUpdateEvent` deserialized from the same JSON shapes (serenity 0.12 accepts `{"channel_id":..,"guild_id":"100","session_id":"sess","user_id":"1","deaf":false,"mute":false,"self_deaf":false,"self_mute":false,"self_video":false,"suppress":false,"request_to_speak_timestamp":null}` and `{"token":"tok","guild_id":"100","endpoint":"e:443"}`); add `assert!(voice_server(&event_without_guild).is_none())`.

`songbird.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn maps_connection_info() {
        use ::songbird::id::{ChannelId as SbChannel, GuildId as SbGuild, UserId as SbUser};
        use std::num::NonZeroU64;
        let n = |v| NonZeroU64::new(v).unwrap();
        let info = ::songbird::ConnectionInfo {
            channel_id: Some(SbChannel::from(n(9))), endpoint: "e:443".into(), guild_id: SbGuild::from(n(100)),
            session_id: "sess".into(), token: "tok".into(), user_id: SbUser::from(n(1)),
        };
        let (g, vs) = connection_info(&info);
        assert_eq!(g, GuildId(100));
        assert_eq!((vs.token.as_str(), vs.endpoint.as_str(), vs.session_id.as_str(), vs.channel_id), ("tok", "e:443", "sess", Some(ChannelId(9))));
    }
}
```
(If `ConnectionInfo` is `#[non_exhaustive]` in the pinned version, construct it through its builder/`Default` as documented.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p straight-rs --features twilight --lib adapters`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement**

`adapters/mod.rs`:
```rust
//! Conversions from Discord library types into straight-rs's library-agnostic voice inputs.
#[cfg(feature = "serenity")]
pub mod serenity;
#[cfg(feature = "songbird")]
pub mod songbird;
#[cfg(feature = "twilight")]
pub mod twilight;
```

`adapters/twilight.rs` (above the tests):
```rust
use crate::{ChannelId, GuildId, UserId, VoiceServerUpdate, VoiceStateUpdate};
use ::twilight_model::gateway::payload::incoming::VoiceServerUpdate as TwServer;
use ::twilight_model::voice::VoiceState as TwState;

/// `None` for other users' voice states and for events without a guild.
pub fn voice_state(vs: &TwState, bot: UserId) -> Option<(GuildId, VoiceStateUpdate)> {
    if vs.user_id.get() != bot.0 {
        return None;
    }
    let guild = GuildId(vs.guild_id?.get());
    Some((
        guild,
        VoiceStateUpdate {
            channel_id: vs.channel_id.map(|c| ChannelId(c.get())),
            session_id: vs.session_id.clone(),
        },
    ))
}

pub fn voice_server(v: &TwServer) -> (GuildId, VoiceServerUpdate) {
    (GuildId(v.guild_id.get()), VoiceServerUpdate { token: v.token.clone(), endpoint: v.endpoint.clone() })
}
```

`adapters/serenity.rs`:
```rust
use crate::{ChannelId, GuildId, UserId, VoiceServerUpdate, VoiceStateUpdate};
use ::serenity::model::event::VoiceServerUpdateEvent;
use ::serenity::model::voice::VoiceState as SerVoiceState;

pub fn voice_state(vs: &SerVoiceState, bot: UserId) -> Option<(GuildId, VoiceStateUpdate)> {
    if vs.user_id.get() != bot.0 {
        return None;
    }
    let guild = GuildId(vs.guild_id?.get());
    Some((
        guild,
        VoiceStateUpdate {
            channel_id: vs.channel_id.map(|c| ChannelId(c.get())),
            session_id: vs.session_id.clone(),
        },
    ))
}

pub fn voice_server(ev: &VoiceServerUpdateEvent) -> Option<(GuildId, VoiceServerUpdate)> {
    let guild = GuildId(ev.guild_id?.get());
    Some((guild, VoiceServerUpdate { token: ev.token.clone(), endpoint: ev.endpoint.clone() }))
}
```

`adapters/songbird.rs`:
```rust
use crate::{ChannelId, GuildId, VoiceState};
use ::songbird::ConnectionInfo;

/// A complete connection; pass it to `LavalinkClient::voice_update`.
pub fn connection_info(info: &ConnectionInfo) -> (GuildId, VoiceState) {
    (
        GuildId(info.guild_id.get()),
        VoiceState {
            token: info.token.clone(),
            endpoint: info.endpoint.clone(),
            session_id: info.session_id.clone(),
            channel_id: info.channel_id.map(|c| ChannelId(c.get())),
        },
    )
}
```

- [ ] **Step 4: Run to verify pass**

Run each: `cargo test -p straight-rs --features twilight --lib`, `--features serenity`, `--features songbird`
Expected: PASS for each feature.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: serenity, twilight and songbird voice adapters"
```

---

### Task 11: Benchmarks, e2e suite, CI, README

**Files:**
- Modify: `straight-rs/benches/core.rs`
- Create: `straight-rs/tests/e2e.rs`, `.github/workflows/ci.yml`, `README.md`

**Interfaces:**
- Consumes: `straight_rs::model::WsMessage`, `balancer::{pick, NodeView, Strategy}`, `position::interpolate`, `PlayerSnapshot`.

- [ ] **Step 1: Benchmarks** — `straight-rs/benches/core.rs`

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use straight_rs::balancer::{pick, NodeView, Strategy};
use straight_rs::model::WsMessage;
use straight_rs::position::interpolate;
use straight_rs::PlayerSnapshot;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

const PLAYER_UPDATE: &str = r#"{"op":"playerUpdate","guildId":"123456789012345678","state":{"time":1700000000000,"position":123456,"connected":true,"ping":12}}"#;
const STATS: &str = r#"{"op":"stats","players":120,"playingPlayers":80,"uptime":123456,"memory":{"free":1,"used":2,"allocated":3,"reservable":4},"cpu":{"cores":8,"systemLoad":0.3,"lavalinkLoad":0.1},"frameStats":{"sent":6000,"nulled":3,"deficit":-2}}"#;

fn benches(c: &mut Criterion) {
    c.bench_function("parse_player_update", |b| b.iter(|| WsMessage::parse(black_box(PLAYER_UPDATE)).unwrap()));
    c.bench_function("parse_stats", |b| b.iter(|| WsMessage::parse(black_box(STATS)).unwrap()));
    let views: Vec<NodeView> = (0..16).map(|i| NodeView { index: i, penalty: (i as u32 * 7) % 13, players: i as u32 }).collect();
    let rr = AtomicUsize::new(0);
    c.bench_function("pick_least_penalty_16_nodes", |b| b.iter(|| pick(&Strategy::LeastPenalty, black_box(&views), &rr)));
    c.bench_function("interpolate", |b| b.iter(|| interpolate(black_box(1000), true, black_box(Duration::from_millis(250)), Some(200_000))));
    let snap = PlayerSnapshot::default();
    c.bench_function("snapshot_position_now", |b| b.iter(|| black_box(&snap).position_now()));
}

criterion_group!(core, benches);
criterion_main!(core);
```

Run: `cargo bench -p straight-rs --no-run` (must compile), then `cargo bench -p straight-rs` once and record the numbers in the README "Performance" section. Sanity bound: parsing a `playerUpdate` should be far below 10 µs; if not, profile before shipping.

- [ ] **Step 2: Optional e2e suite** — `straight-rs/tests/e2e.rs`

```rust
#![cfg(feature = "e2e")]
//! Needs a real Lavalink v4: `docker run -p 2333:2333 -e SERVER_PORT=2333 ghcr.io/lavalink-devs/lavalink:4`
//! Run: `cargo test -p straight-rs --features e2e -- --ignored`
use straight_rs::{GuildId, LavalinkClient, NodeConfig, UserId};
use std::time::Duration;

async fn connect() -> LavalinkClient {
    let host = std::env::var("LAVALINK_HOST").unwrap_or_else(|_| "127.0.0.1:2333".into());
    let pw = std::env::var("LAVALINK_PASSWORD").unwrap_or_else(|_| "youshallnotpass".into());
    let c = LavalinkClient::builder(UserId(1)).node(NodeConfig::new(host, pw)).build().await.unwrap();
    c.wait_ready(Duration::from_secs(10)).await.unwrap();
    c
}

#[tokio::test]
#[ignore]
async fn real_server_info_version_and_stats() {
    let c = connect().await;
    let n = &c.nodes()[0];
    assert_eq!(n.info().await.unwrap().version.major, 4);
    assert!(n.version().await.unwrap().starts_with('4'));
    assert!(n.rest().stats().await.is_ok());
}

#[tokio::test]
#[ignore]
async fn real_server_load_and_player_lifecycle() {
    let c = connect().await;
    assert!(c.load("https://example.invalid/none.mp3").await.is_ok());
    let p = c.player(GuildId(1));
    p.set_volume(50).await.unwrap();
    assert_eq!(p.volume(), 50);
    p.destroy().await.unwrap();
}
```

- [ ] **Step 3: CI** — `.github/workflows/ci.yml`

```yaml
name: ci
on: [push, pull_request]
env:
  CARGO_TERM_COLOR: always
jobs:
  stable:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: "clippy, rustfmt" }
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all --check
      - run: cargo clippy --workspace --all-targets --all-features -- -D warnings
      - run: cargo test --workspace
      - run: cargo test -p straight-rs --features twilight
      - run: cargo test -p straight-rs --features serenity
      - run: cargo test -p straight-rs --features songbird
      - run: cargo check -p straight-rs --features tls
      - run: cargo bench -p straight-rs --no-run
  msrv:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@1.80
      - uses: Swatinem/rust-cache@v2
      - run: cargo test --workspace
```

- [ ] **Step 4: README** — `README.md` with: what straight-rs is (Lavalink v4 client, library-agnostic), install (`straight-rs = "0.1"`, optional features `serenity|twilight|songbird|tls`), a quick start matching the API below, a "Wiring your Discord library" section (feed the bot's own voice events to `voice_state_update` / `voice_server_update`, or `voice_update` for songbird; implement `VoiceGateway` for `join`/`leave`), the event model (`client.events()`, `player.events()`, `Lagged`), resume/failover semantics (grace period, `PlayerMigrated`), the out-of-scope list, and the measured benchmark numbers from Step 1.

Quick-start code to include verbatim:
```rust
let client = LavalinkClient::builder(UserId(bot_id))
    .node(NodeConfig::new("127.0.0.1:2333", "youshallnotpass"))
    .strategy(Strategy::LeastPenalty)
    .build()
    .await?;
client.wait_ready(std::time::Duration::from_secs(10)).await?;

client.voice_state_update(guild_id, VoiceStateUpdate { channel_id: Some(channel), session_id }).await?;
client.voice_server_update(guild_id, VoiceServerUpdate { token, endpoint: Some(endpoint) }).await?;

if let LoadResult::Search(tracks) = client.load("ytsearch:never gonna give you up").await? {
    if let Some(track) = tracks.first() {
        client.player(guild_id).play(track).await?;
    }
}
let mut events = client.events();
```

- [ ] **Step 5: Final verification**

Run, in order, and confirm every command exits 0:
```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo test -p straight-rs --features twilight
cargo test -p straight-rs --features serenity
cargo test -p straight-rs --features songbird
cargo check -p straight-rs --features tls
cargo bench -p straight-rs --no-run
```
Expected: all pass with zero warnings. Fix any clippy findings in the code (not with `#[allow]`, except the two documented `#[allow]`s already in the plan).

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "chore: benchmarks, e2e suite, CI, README"
```

---

## Self-Review (spec coverage)

| Spec section | Task |
|---|---|
| §3 model crate (all payloads, plugin passthrough, `Arc<str>` encoded) | 1, 2 |
| §4 public API (builder, `voice_*`, `player`, `events`, `load`/`decode`) | 6, 7, 8 |
| §5 node task, headers, resume enable, resumed=false rebuild, backoff, ping watchdog | 3, 6, 9 |
| §5 REST pooling, timeouts, GET-only retry, per-player ordering | 4, 7 |
| §5 failover, orphaned, balancing strategies, node utilities | 3, 6, 9 |
| §6 player snapshot/position, voice assembly (either order, null endpoint, leave), `VoiceGateway`, adapters, events (incl. `Unknown`, `Lagged`) | 5, 7, 8, 10 |
| §6 benches | 11 |
| §7 error type, no panics, errors as events/logs | 3, 6 (tracing warnings), all |
| §8 unit/integration/e2e/bench/CI/MSRV | every task's tests, 11 |
