use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use sluice_core::{CheckpointId, Event, Timestamp};

/// The end of a pipeline.
///
/// Sinks that want exactly-once output implement the two-phase commit
/// hooks: output written between two barriers is staged by
/// [`pre_commit`](Self::pre_commit) when the barrier arrives and made
/// visible by [`commit`](Self::commit) once that checkpoint completes.
/// Staging must survive the sink's failure (it lives in the external
/// system), so [`restore`](Self::restore) can finish the job on recovery.
pub trait Sink<T>: Send + 'static {
    fn write(&mut self, event: Event<T>);

    fn on_watermark(&mut self, _watermark: Timestamp) {}

    /// A checkpoint barrier arrived: stage everything written since the
    /// previous barrier under `checkpoint_id`.
    fn pre_commit(&mut self, _checkpoint_id: CheckpointId) {}

    /// Checkpoint `checkpoint_id` completed: publish everything staged up
    /// to and including it.
    fn commit(&mut self, _checkpoint_id: CheckpointId) {}

    /// Called once before anything else in every attempt, with the
    /// checkpoint the job was restored from. Commit what that checkpoint
    /// covers; discard staged output it does not (it will be replayed).
    fn restore(&mut self, _checkpoint: Option<CheckpointId>) {}

    /// The stream ended normally.
    fn finish(&mut self) {}
}

struct Shared<T> {
    committed: VecDeque<Event<T>>,
    staged: BTreeMap<CheckpointId, Vec<Event<T>>>,
}

/// Collects output into a buffer shared with the caller.
///
/// By default every write is immediately visible (at-least-once: after a
/// restart, replayed records show up twice). [`exactly_once`](Self::exactly_once)
/// makes the sink transactional: output only becomes visible once the
/// checkpoint covering it completes, so recovery never duplicates or loses
/// a record, at the cost of output latency up to one checkpoint interval.
pub struct CollectSink<T> {
    shared: Arc<Mutex<Shared<T>>>,
    capacity: Option<usize>,
    exactly_once: bool,
    /// Written since the last barrier; local to one pipeline attempt.
    pending: Vec<Event<T>>,
}

impl<T> Clone for CollectSink<T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            capacity: self.capacity,
            exactly_once: self.exactly_once,
            pending: Vec::new(),
        }
    }
}

impl<T> Default for CollectSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CollectSink<T> {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Mutex::new(Shared {
                committed: VecDeque::new(),
                staged: BTreeMap::new(),
            })),
            capacity: None,
            exactly_once: false,
            pending: Vec::new(),
        }
    }

    /// Keeps only the most recent `capacity` committed events.
    pub fn bounded(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        Self {
            capacity: Some(capacity),
            ..Self::new()
        }
    }

    pub fn exactly_once(mut self) -> Self {
        self.exactly_once = true;
        self
    }

    fn lock(&self) -> MutexGuard<'_, Shared<T>> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn publish(&self, shared: &mut Shared<T>, events: impl IntoIterator<Item = Event<T>>) {
        for event in events {
            if let Some(capacity) = self.capacity {
                while shared.committed.len() >= capacity {
                    shared.committed.pop_front();
                }
            }
            shared.committed.push_back(event);
        }
    }

    fn commit_up_to(&self, checkpoint_id: CheckpointId) {
        let mut shared = self.lock();
        let later = shared.staged.split_off(&(checkpoint_id.saturating_add(1)));
        let ready = std::mem::replace(&mut shared.staged, later);
        for (_, events) in ready {
            self.publish(&mut shared, events);
        }
    }

    /// Committed events.
    pub fn len(&self) -> usize {
        self.lock().committed.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().committed.is_empty()
    }

    /// Number of events staged but not yet committed.
    pub fn staged_len(&self) -> usize {
        self.lock().staged.values().map(Vec::len).sum()
    }

    /// Removes and returns everything committed so far.
    pub fn take(&self) -> Vec<Event<T>> {
        self.lock().committed.drain(..).collect()
    }

    pub fn snapshot(&self) -> Vec<Event<T>>
    where
        T: Clone,
    {
        self.lock().committed.iter().cloned().collect()
    }

    /// The `n` most recent committed events, oldest first.
    pub fn latest(&self, n: usize) -> Vec<Event<T>>
    where
        T: Clone,
    {
        let shared = self.lock();
        let skip = shared.committed.len().saturating_sub(n);
        shared.committed.iter().skip(skip).cloned().collect()
    }

    pub fn values(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.lock().committed.iter().map(|e| e.value.clone()).collect()
    }
}

impl<T: Send + 'static> Sink<T> for CollectSink<T> {
    fn write(&mut self, event: Event<T>) {
        if self.exactly_once {
            self.pending.push(event);
        } else {
            let mut shared = self.lock();
            self.publish(&mut shared, [event]);
        }
    }

    fn pre_commit(&mut self, checkpoint_id: CheckpointId) {
        if self.exactly_once && !self.pending.is_empty() {
            let events = std::mem::take(&mut self.pending);
            self.lock().staged.entry(checkpoint_id).or_default().extend(events);
        }
    }

    fn commit(&mut self, checkpoint_id: CheckpointId) {
        self.commit_up_to(checkpoint_id);
    }

    fn restore(&mut self, checkpoint: Option<CheckpointId>) {
        self.pending.clear();
        if let Some(id) = checkpoint {
            self.commit_up_to(id);
        }
        self.lock().staged.clear();
    }

    fn finish(&mut self) {
        // End of input: everything is final.
        self.commit_up_to(CheckpointId::MAX);
        let pending = std::mem::take(&mut self.pending);
        let mut shared = self.lock();
        self.publish(&mut shared, pending);
    }
}

/// Prints each event to stdout.
pub struct StdoutSink<T> {
    _types: PhantomData<fn(T)>,
}

impl<T> StdoutSink<T> {
    pub fn new() -> Self {
        Self { _types: PhantomData }
    }
}

impl<T> Default for StdoutSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Debug + Send + 'static> Sink<T> for StdoutSink<T> {
    fn write(&mut self, event: Event<T>) {
        let key = event.key.as_deref().map(String::from_utf8_lossy);
        println!(
            "[t={}] key={} value={:?}",
            event.event_time,
            key.as_deref().unwrap_or("-"),
            event.value
        );
    }
}

/// Calls a closure for every event.
pub struct FnSink<F>(F);

impl<F> FnSink<F> {
    pub fn new(f: F) -> Self {
        Self(f)
    }
}

impl<T, F> Sink<T> for FnSink<F>
where
    F: FnMut(Event<T>) + Send + 'static,
{
    fn write(&mut self, event: Event<T>) {
        (self.0)(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(sink: &CollectSink<u32>) -> Vec<u32> {
        sink.values()
    }

    #[test]
    fn bounded_collect_sink_keeps_most_recent() {
        let sink = CollectSink::bounded(3);
        let mut writer = sink.clone();
        for i in 0..5 {
            writer.write(Event::new(i, i as u64));
        }
        assert_eq!(values(&sink), vec![2, 3, 4]);
        assert_eq!(sink.latest(2).iter().map(|e| e.value).collect::<Vec<_>>(), vec![3, 4]);
        assert_eq!(sink.take().len(), 3);
        assert!(sink.is_empty());
    }

    #[test]
    fn exactly_once_publishes_only_committed_checkpoints() {
        let sink = CollectSink::new().exactly_once();
        let mut w = sink.clone();
        w.restore(None);
        w.write(Event::new(1, 0));
        w.pre_commit(1);
        w.write(Event::new(2, 0));
        w.pre_commit(2);
        w.write(Event::new(3, 0));
        assert!(values(&sink).is_empty());
        assert_eq!(sink.staged_len(), 2);

        w.commit(1);
        assert_eq!(values(&sink), vec![1]);

        // Crash: recovery from checkpoint 2 means its staged output is
        // covered (the Complete notification was lost); 3 is replayed.
        let mut recovered = sink.clone();
        recovered.restore(Some(2));
        assert_eq!(values(&sink), vec![1, 2]);
        assert_eq!(sink.staged_len(), 0);

        recovered.write(Event::new(3, 0));
        recovered.finish();
        assert_eq!(values(&sink), vec![1, 2, 3]);
    }

    #[test]
    fn restore_discards_output_of_uncompleted_checkpoints() {
        let sink = CollectSink::new().exactly_once();
        let mut w = sink.clone();
        w.write(Event::new(1, 0));
        w.pre_commit(5);
        w.clone().restore(Some(4)); // checkpoint 5 never completed
        assert!(values(&sink).is_empty());
        assert_eq!(sink.staged_len(), 0);
    }
}
