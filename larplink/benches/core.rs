use criterion::{black_box, criterion_group, criterion_main, Criterion};
use larplink::balancer::{pick, NodeView, Strategy};
use larplink::model::WsMessage;
use larplink::position::interpolate;
use larplink::PlayerSnapshot;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

const PLAYER_UPDATE: &str = r#"{"op":"playerUpdate","guildId":"123456789012345678","state":{"time":1700000000000,"position":123456,"connected":true,"ping":12}}"#;
const STATS: &str = r#"{"op":"stats","players":120,"playingPlayers":80,"uptime":123456,"memory":{"free":1,"used":2,"allocated":3,"reservable":4},"cpu":{"cores":8,"systemLoad":0.3,"lavalinkLoad":0.1},"frameStats":{"sent":6000,"nulled":3,"deficit":-2}}"#;

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
    let snap = PlayerSnapshot::default();
    c.bench_function("snapshot_position_now", |b| {
        b.iter(|| black_box(&snap).position_now())
    });
}

criterion_group!(core, benches);
criterion_main!(core);
