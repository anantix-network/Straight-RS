use std::time::Duration;

pub(crate) struct Backoff {
    base: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    pub(crate) fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            attempt: 0,
        }
    }

    /// `jitter` in `[0, 1]`: 1.0 = full delay, 0.0 = half of it.
    pub(crate) fn next_delay(&mut self, jitter: f64) -> Duration {
        let exp = self.base.saturating_mul(1u32 << self.attempt.min(16));
        let capped = exp.min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        // NaN would survive `clamp` and make `mul_f64` panic; treat it as full delay.
        let jitter = if jitter.is_nan() {
            1.0
        } else {
            jitter.clamp(0.0, 1.0)
        };
        capped.mul_f64(0.5 + 0.5 * jitter)
    }

    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
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
    fn nan_jitter_does_not_panic() {
        let mut b = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        assert_eq!(b.next_delay(f64::NAN), Duration::from_millis(500));
    }
    #[test]
    fn huge_attempt_counts_do_not_overflow() {
        let mut b = Backoff::new(Duration::from_millis(500), Duration::from_secs(30));
        for _ in 0..1000 {
            assert!(b.next_delay(0.5) <= Duration::from_secs(30));
        }
    }
}
