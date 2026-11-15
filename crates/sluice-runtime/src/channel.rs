//! Credit-based flow control between pipeline stages.
//!
//! A channel starts with `capacity` credits. The sender spends one credit
//! per element and waits when it has none, so at most `capacity` elements
//! are ever in flight. The receiver hands credits back as it consumes
//! elements, batched (every quarter of the capacity) to keep the
//! announcement traffic low, and always before it goes to sleep, so a
//! sender can never wait on credits the receiver is sitting on.
//!
//! The time a sender spends waiting for credit is the stage's
//! *backpressure*: it is busy waiting on a slower downstream.

use std::future::poll_fn;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::sync::{Semaphore, TryAcquireError, mpsc};

/// Flow-control statistics of one channel, shared with the metrics of the
/// stages at both ends.
#[derive(Debug)]
pub struct ChannelStats {
    capacity: usize,
    queued: AtomicUsize,
    epoch: Instant,
    blocked_nanos: AtomicU64,
    /// Nanoseconds since `epoch` + 1 at which the current wait began; 0 if
    /// the sender is not waiting.
    blocked_since: AtomicU64,
}

impl ChannelStats {
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Elements sent but not yet received.
    pub fn queued(&self) -> usize {
        self.queued.load(Relaxed)
    }

    /// Total time the sender has waited for credit, including a wait in
    /// progress.
    pub fn blocked_nanos(&self) -> u64 {
        let done = self.blocked_nanos.load(Relaxed);
        match self.blocked_since.load(Relaxed) {
            0 => done,
            since => done + (self.epoch.elapsed().as_nanos() as u64).saturating_sub(since - 1),
        }
    }
}

struct Shared {
    credits: Semaphore,
    stats: Arc<ChannelStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("receiver dropped")]
pub struct SendError;

pub struct Sender<T> {
    tx: mpsc::UnboundedSender<T>,
    shared: Arc<Shared>,
}

pub struct Receiver<T> {
    rx: mpsc::UnboundedReceiver<T>,
    shared: Arc<Shared>,
    unreturned: usize,
    batch: usize,
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "channel capacity must be positive");
    let (tx, rx) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        credits: Semaphore::new(capacity),
        stats: Arc::new(ChannelStats {
            capacity,
            queued: AtomicUsize::new(0),
            epoch: Instant::now(),
            blocked_nanos: AtomicU64::new(0),
            blocked_since: AtomicU64::new(0),
        }),
    });
    (
        Sender {
            tx,
            shared: Arc::clone(&shared),
        },
        Receiver {
            rx,
            shared,
            unreturned: 0,
            batch: (capacity / 4).max(1),
        },
    )
}

impl<T> Sender<T> {
    /// Sends `value`, first waiting for a credit if none is available.
    pub async fn send(&self, value: T) -> Result<(), SendError> {
        let stats = &self.shared.stats;
        match self.shared.credits.try_acquire() {
            Ok(permit) => permit.forget(),
            Err(TryAcquireError::Closed) => return Err(SendError),
            Err(TryAcquireError::NoPermits) => {
                let started = Instant::now();
                stats
                    .blocked_since
                    .store(stats.epoch.elapsed().as_nanos() as u64 + 1, Relaxed);
                let acquired = self.shared.credits.acquire().await;
                stats.blocked_since.store(0, Relaxed);
                stats.blocked_nanos.fetch_add(started.elapsed().as_nanos() as u64, Relaxed);
                acquired.map_err(|_| SendError)?.forget();
            }
        }
        stats.queued.fetch_add(1, Relaxed);
        self.tx.send(value).map_err(|_| SendError)
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    pub fn stats(&self) -> Arc<ChannelStats> {
        Arc::clone(&self.shared.stats)
    }
}

impl<T> Receiver<T> {
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(value)) => {
                self.shared.stats.queued.fetch_sub(1, Relaxed);
                self.unreturned += 1;
                if self.unreturned >= self.batch {
                    self.return_credits();
                }
                Poll::Ready(Some(value))
            }
            other => {
                // About to wait (or done): hand back everything consumed.
                self.return_credits();
                other
            }
        }
    }

    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub fn try_recv(&mut self) -> Option<T> {
        let value = self.rx.try_recv().ok()?;
        self.shared.stats.queued.fetch_sub(1, Relaxed);
        self.unreturned += 1;
        self.return_credits();
        Some(value)
    }

    pub fn stats(&self) -> Arc<ChannelStats> {
        Arc::clone(&self.shared.stats)
    }

    fn return_credits(&mut self) {
        if self.unreturned > 0 {
            self.shared.credits.add_permits(self.unreturned);
            self.unreturned = 0;
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // Wake a sender waiting for credit: it must fail, not hang.
        self.shared.credits.close();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn sender_waits_without_credit_and_resumes_when_credits_return() {
        let (tx, mut rx) = channel::<u32>(4);
        for i in 0..4 {
            tx.send(i).await.unwrap();
        }
        assert_eq!(tx.stats().queued(), 4);

        let blocked = tokio::spawn(async move {
            tx.send(4).await.unwrap();
            tx
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished(), "no credits left");

        assert_eq!(rx.recv().await, Some(0)); // batch of 1 at capacity 4: credit returned
        let tx = blocked.await.unwrap();
        assert!(tx.stats().blocked_nanos() >= 15_000_000, "waited ~20ms for credit");
        for expected in 1..=4 {
            assert_eq!(rx.recv().await, Some(expected));
        }
    }

    #[tokio::test]
    async fn batched_credits_are_returned_before_the_receiver_sleeps() {
        // Batch of 4: after taking 2 elements the receiver holds 2 credits;
        // they must come back once it finds the channel empty.
        let (tx, mut rx) = channel::<u32>(16);
        for i in 0..16 {
            tx.send(i).await.unwrap();
        }
        let consumer = tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(v) = rx.recv().await {
                got.push(v);
            }
            got
        });
        for i in 16..100 {
            tx.send(i).await.unwrap();
        }
        drop(tx);
        assert_eq!(consumer.await.unwrap(), (0..100).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn dropping_the_receiver_fails_a_waiting_sender() {
        let (tx, rx) = channel::<u32>(1);
        tx.send(1).await.unwrap();
        let waiting = tokio::spawn(async move { tx.send(2).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(rx);
        assert_eq!(waiting.await.unwrap(), Err(SendError));
    }

    #[tokio::test]
    async fn ongoing_wait_counts_as_blocked() {
        let (tx, _rx) = channel::<u32>(1);
        tx.send(1).await.unwrap();
        let stats = tx.stats();
        tokio::spawn(async move { tx.send(2).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(stats.blocked_nanos() >= 20_000_000);
    }
}
