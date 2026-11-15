use std::sync::Arc;

use sluice_runtime::{SourceFunction, SourceItem};

use crate::router::{Offset, ReadResult, Subscription};

/// A replayable job source over a stream's log.
///
/// Its position is a log offset: checkpoints record it, recovery seeks back
/// to it, and checkpoint completion commits it so the log can discard what
/// is no longer needed for replay. Never ends on its own.
pub struct LogSource {
    subscription: Arc<Subscription>,
    position: Offset,
}

impl LogSource {
    /// Starts at the subscription's committed offset.
    pub fn new(subscription: Arc<Subscription>) -> Self {
        let position = subscription.committed();
        Self { subscription, position }
    }
}

impl SourceFunction<Vec<u8>> for LogSource {
    async fn next(&mut self) -> Option<SourceItem<Vec<u8>>> {
        loop {
            match self.subscription.log().read_wait(self.position).await {
                ReadResult::Event(event) => {
                    self.position += 1;
                    return Some(SourceItem::Record(event));
                }
                ReadResult::Truncated(oldest) => {
                    tracing::warn!(from = self.position, to = oldest, "log truncated past the read position; skipping ahead");
                    self.position = oldest;
                }
                ReadResult::Pending => unreachable!("read_wait only returns available results"),
            }
        }
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn seek(&mut self, position: u64) {
        self.position = position;
    }

    fn commit(&mut self, position: u64) {
        self.subscription.commit(position);
    }
}
