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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
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
}
