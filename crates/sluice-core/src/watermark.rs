use crate::event::{MAX_WATERMARK, Timestamp};

#[derive(Debug, Clone, Copy, Default)]
struct InputState {
    watermark: Option<Timestamp>,
    idle: bool,
}

/// Tracks the latest watermark on each input channel of an operator and
/// derives the operator's own watermark: the minimum across the *active*
/// inputs.
///
/// The combined watermark only exists once every active input has reported
/// one, and it never moves backwards. Idle inputs are left out, so a quiet
/// input (one side of a join with no traffic) does not stall event time.
/// When every input is idle, nothing holds event time back and the combined
/// watermark advances to the highest watermark seen on any input (as
/// Flink's status valve does). A finished input counts as [`MAX_WATERMARK`].
#[derive(Debug, Clone)]
pub struct WatermarkTracker {
    inputs: Vec<InputState>,
    current: Option<Timestamp>,
}

impl WatermarkTracker {
    pub fn new(num_inputs: usize) -> Self {
        assert!(num_inputs > 0, "an operator needs at least one input");
        Self {
            inputs: vec![InputState::default(); num_inputs],
            current: None,
        }
    }

    /// The combined watermark, if every active input has reported one.
    pub fn current(&self) -> Option<Timestamp> {
        self.current
    }

    /// Records a watermark from `input` (which is thereby active). Returns
    /// the new combined watermark if it advanced, `None` otherwise.
    pub fn update(&mut self, input: usize, watermark: Timestamp) -> Option<Timestamp> {
        let state = &mut self.inputs[input];
        state.idle = false;
        if state.watermark.is_some_and(|seen| watermark <= seen) {
            return None;
        }
        state.watermark = Some(watermark);
        self.recompute()
    }

    /// Marks `input` idle or active. Going idle can advance the combined
    /// watermark; becoming active never moves it back.
    pub fn set_idle(&mut self, input: usize, idle: bool) -> Option<Timestamp> {
        self.inputs[input].idle = idle;
        self.recompute()
    }

    /// Whether every input is idle (the operator itself is then idle).
    pub fn all_idle(&self) -> bool {
        self.inputs.iter().all(|i| i.idle)
    }

    /// Marks `input` as finished.
    pub fn finish_input(&mut self, input: usize) -> Option<Timestamp> {
        self.update(input, MAX_WATERMARK)
    }

    fn recompute(&mut self) -> Option<Timestamp> {
        let candidate = if self.all_idle() {
            self.inputs.iter().filter_map(|i| i.watermark).max()?
        } else {
            let mut min: Option<Timestamp> = None;
            for input in self.inputs.iter().filter(|i| !i.idle) {
                let wm = input.watermark?;
                min = Some(min.map_or(wm, |m| m.min(wm)));
            }
            min?
        };
        let min = candidate;
        if self.current.is_some_and(|current| min <= current) {
            return None;
        }
        self.current = Some(min);
        Some(min)
    }
}

/// Source-side watermark generator for streams whose events arrive at most
/// `max_out_of_orderness` ms behind the highest event time seen so far.
///
/// With max event time `t` seen, it promises no further events at or before
/// `t - max_out_of_orderness - 1`.
#[derive(Debug, Clone)]
pub struct BoundedOutOfOrderness {
    max_out_of_orderness: Timestamp,
    max_seen: Option<Timestamp>,
}

impl BoundedOutOfOrderness {
    pub fn new(max_out_of_orderness: Timestamp) -> Self {
        Self {
            max_out_of_orderness,
            max_seen: None,
        }
    }

    pub fn on_event(&mut self, event_time: Timestamp) {
        self.max_seen = Some(self.max_seen.map_or(event_time, |m| m.max(event_time)));
    }

    /// Highest event time seen so far; part of a source's checkpointed state.
    pub fn max_seen(&self) -> Option<Timestamp> {
        self.max_seen
    }

    /// Restores the state captured by [`max_seen`](Self::max_seen).
    pub fn restore(&mut self, max_seen: Option<Timestamp>) {
        self.max_seen = max_seen;
    }

    pub fn current_watermark(&self) -> Option<Timestamp> {
        self.max_seen?
            .checked_sub(self.max_out_of_orderness.saturating_add(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_input_follows_its_watermark() {
        let mut t = WatermarkTracker::new(1);
        assert_eq!(t.current(), None);
        assert_eq!(t.update(0, 10), Some(10));
        assert_eq!(t.update(0, 20), Some(20));
        assert_eq!(t.current(), Some(20));
    }

    #[test]
    fn never_moves_backwards() {
        let mut t = WatermarkTracker::new(1);
        t.update(0, 20);
        assert_eq!(t.update(0, 10), None);
        assert_eq!(t.update(0, 20), None);
        assert_eq!(t.current(), Some(20));
    }

    #[test]
    fn waits_for_every_input_and_takes_the_minimum() {
        let mut t = WatermarkTracker::new(2);
        assert_eq!(t.update(0, 100), None, "input 1 has not reported yet");
        assert_eq!(t.update(1, 50), Some(50));
        assert_eq!(t.update(1, 80), Some(80));
        assert_eq!(t.update(1, 150), Some(100), "input 0 now holds it back");
        assert_eq!(t.update(0, 120), Some(120));
    }

    #[test]
    fn slower_input_advancing_below_current_is_ignored() {
        let mut t = WatermarkTracker::new(2);
        t.update(0, 100);
        t.update(1, 100);
        assert_eq!(t.update(0, 200), None, "input 1 is still at 100");
    }

    #[test]
    fn finished_input_no_longer_holds_back_others() {
        let mut t = WatermarkTracker::new(2);
        t.update(0, 10);
        t.update(1, 5);
        assert_eq!(t.finish_input(1), Some(10));
        assert_eq!(t.finish_input(0), Some(MAX_WATERMARK));
    }

    #[test]
    fn idle_inputs_do_not_hold_back_the_watermark() {
        let mut t = WatermarkTracker::new(2);
        assert_eq!(t.update(0, 100), None, "input 1 silent and not yet idle");
        assert_eq!(t.set_idle(1, true), Some(100));
        assert_eq!(t.update(0, 200), Some(200));
        assert!(!t.all_idle());

        // Input 1 wakes up behind: the combined watermark does not regress,
        // and it holds back further progress until it catches up.
        assert_eq!(t.update(1, 150), None);
        assert_eq!(t.current(), Some(200));
        assert_eq!(t.update(0, 300), None);
        assert_eq!(t.update(1, 250), Some(250));

        assert_eq!(t.set_idle(0, true), None, "input 1 (at 250) still active");
        assert_eq!(t.set_idle(1, true), Some(300), "all idle: advance to the highest input");
        assert!(t.all_idle());
    }

    #[test]
    fn bounded_out_of_orderness_lags_behind_max_seen() {
        let mut g = BoundedOutOfOrderness::new(1_000);
        assert_eq!(g.current_watermark(), None);
        g.on_event(5_000);
        assert_eq!(g.current_watermark(), Some(3_999));
        g.on_event(4_000); // out of order: does not move the watermark back
        assert_eq!(g.current_watermark(), Some(3_999));
        g.on_event(10_000);
        assert_eq!(g.current_watermark(), Some(8_999));
    }

    #[test]
    fn bounded_out_of_orderness_has_no_watermark_near_zero() {
        let mut g = BoundedOutOfOrderness::new(1_000);
        g.on_event(500);
        assert_eq!(g.current_watermark(), None);
        let mut monotonous = BoundedOutOfOrderness::new(0);
        monotonous.on_event(500);
        assert_eq!(monotonous.current_watermark(), Some(499));
    }
}
