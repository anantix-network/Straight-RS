use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use straight_rs::__bench::PlayerCell;
use straight_rs::PlayerSnapshot;
use straight_rs::balancer::{NodeView, Strategy, pick};
use straight_rs::model::{Player, PlayerState, WsMessage};
use straight_rs::position::interpolate;

const PLAYER_UPDATE: &str = r#"{"op":"playerUpdate","guildId":"123456789012345678","state":{"time":1700000000000,"position":123456,"connected":true,"ping":12}}"#;
const STATS: &str = r#"{"op":"stats","players":120,"playingPlayers":80,"uptime":123456,"memory":{"free":1,"used":2,"allocated":3,"reservable":4},"cpu":{"cores":8,"systemLoad":0.3,"lavalinkLoad":0.1},"frameStats":{"sent":6000,"nulled":3,"deficit":-2}}"#;

/// A playing player with a realistic filter chain (15-band EQ, timescale, plugin filter).
fn player_with_filters() -> Player {
    let bands: Vec<_> = (0..15)
        .map(|b| serde_json::json!({"band": b, "gain": 0.1}))
        .collect();
    serde_json::from_value(serde_json::json!({
        "guildId": "123456789012345678",
        "track": {"encoded": "QAAAjQIAJVJpY2sgQXN0bGV5IC0gTmV2ZXIgR29ubmEgR2l2ZSBZb3UgVXA", "info": {
            "identifier": "dQw4w9WgXcQ", "isSeekable": true, "author": "Rick Astley",
            "length": 212000, "isStream": false, "position": 0, "title": "Never Gonna Give You Up",
            "uri": "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "artworkUrl": null, "isrc": null,
            "sourceName": "youtube"}, "pluginInfo": {}, "userData": {"requester": 42}},
        "volume": 100, "paused": false,
        "state": {"time": 0, "position": 0, "connected": true, "ping": 12},
        "voice": {"token": "t", "endpoint": "e", "sessionId": "s"},
        "filters": {"volume": 0.8, "equalizer": bands,
            "timescale": {"speed": 1.1, "pitch": 1.0, "rate": 1.0},
            "pluginFilters": {"echo": {"delay": 0.5, "decay": 0.3}}}
    }))
    .unwrap()
}

fn benches(c: &mut Criterion) {
    c.bench_function("parse_player_update", |b| {
        b.iter(|| WsMessage::parse(black_box(PLAYER_UPDATE)).unwrap())
    });
    c.bench_function("parse_stats", |b| {
        b.iter(|| WsMessage::parse(black_box(STATS)).unwrap())
    });
    let views: Vec<NodeView> = (0..16)
        .map(|i| NodeView {
            index: i,
            penalty: (i as u32 * 7) % 13,
            players: i as u32,
        })
        .collect();
    let rr = AtomicUsize::new(0);
    c.bench_function("pick_least_penalty_16_nodes", |b| {
        b.iter(|| pick(&Strategy::LeastPenalty, black_box(&views), &rr))
    });
    c.bench_function("interpolate", |b| {
        b.iter(|| {
            interpolate(
                black_box(1000),
                true,
                black_box(Duration::from_millis(250)),
                Some(200_000),
            )
        })
    });
    let cell = PlayerCell::new(&player_with_filters());
    let st = PlayerState {
        time: 1,
        position: 1000,
        connected: true,
        ping: 12,
    };
    c.bench_function("apply_update", |b| {
        b.iter(|| cell.apply_update(black_box(&st)))
    });
    let snap = PlayerSnapshot::default();
    c.bench_function("snapshot_position_now", |b| {
        b.iter(|| black_box(&snap).position_now())
    });
}

criterion_group!(core, benches);
criterion_main!(core);
