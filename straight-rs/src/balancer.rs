use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use straight_rs_model::Stats;

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
        Strategy::LeastPenalty => ready
            .iter()
            .min_by_key(|n| (n.penalty, n.index))
            .map(|n| n.index),
        Strategy::LeastPlayers => ready
            .iter()
            .min_by_key(|n| (n.players, n.index))
            .map(|n| n.index),
        Strategy::RoundRobin => {
            let i = rr.fetch_add(1, Ordering::Relaxed) % ready.len();
            Some(ready[i].index)
        }
        Strategy::Custom(f) => f(ready).filter(|i| ready.iter().any(|n| n.index == *i)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use straight_rs_model::Stats;

    fn stats(players: u32, load: f64, fs: Option<(i64, i64)>) -> Stats {
        let fs = fs
            .map(|(n, d)| serde_json::json!({"sent":3000,"nulled":n,"deficit":d}))
            .unwrap_or(serde_json::Value::Null);
        serde_json::from_value(
            serde_json::json!({"players":players,"playingPlayers":players,"uptime":1,
          "memory":{"free":1,"used":1,"allocated":1,"reservable":1},
          "cpu":{"cores":4,"systemLoad":load,"lavalinkLoad":0.0},"frameStats":fs}),
        )
        .unwrap()
    }
    #[test]
    fn penalty_zero_and_players_only() {
        assert_eq!(penalty(&stats(0, 0.0, None)), 0);
        assert_eq!(penalty(&stats(10, 0.0, None)), 10);
    }
    #[test]
    fn penalty_grows_with_load_and_deficit() {
        let p = penalty(&stats(10, 0.5, None));
        assert!((114..=115).contains(&p), "{p}");
        assert!(penalty(&stats(10, 0.0, Some((0, 300)))) > penalty(&stats(10, 0.0, None)));
        assert!(penalty(&stats(10, 0.0, Some((300, 0)))) > penalty(&stats(10, 0.0, None)));
    }
    #[test]
    fn penalty_never_negative_or_overflowing() {
        assert_eq!(penalty(&stats(0, 0.0, Some((0, -5000)))), 0);
        assert_eq!(penalty(&stats(0, 1e9, None)), u32::MAX);
    }
    fn views() -> Vec<NodeView> {
        vec![
            NodeView {
                index: 0,
                penalty: 50,
                players: 9,
            },
            NodeView {
                index: 2,
                penalty: 10,
                players: 20,
            },
            NodeView {
                index: 5,
                penalty: 10,
                players: 1,
            },
        ]
    }
    #[test]
    fn least_penalty_ties_break_by_index() {
        assert_eq!(
            pick(&Strategy::LeastPenalty, &views(), &AtomicUsize::new(0)),
            Some(2)
        );
    }
    #[test]
    fn least_players() {
        assert_eq!(
            pick(&Strategy::LeastPlayers, &views(), &AtomicUsize::new(0)),
            Some(5)
        );
    }
    #[test]
    fn round_robin_cycles() {
        let rr = AtomicUsize::new(0);
        let got: Vec<_> = (0..4)
            .map(|_| pick(&Strategy::RoundRobin, &views(), &rr).unwrap())
            .collect();
        assert_eq!(got, vec![0, 2, 5, 0]);
    }
    #[test]
    fn custom_must_return_a_listed_node() {
        let good = Strategy::Custom(Arc::new(|v| v.last().map(|n| n.index)));
        assert_eq!(pick(&good, &views(), &AtomicUsize::new(0)), Some(5));
        let bad = Strategy::Custom(Arc::new(|_| Some(99)));
        assert_eq!(pick(&bad, &views(), &AtomicUsize::new(0)), None);
    }
    #[test]
    fn empty_is_none() {
        assert_eq!(
            pick(&Strategy::LeastPenalty, &[], &AtomicUsize::new(0)),
            None
        );
    }
}
