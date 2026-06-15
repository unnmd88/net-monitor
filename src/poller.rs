use crate::models::{PollEvent, PollType, Strategy};
use crate::{models::LogEvent, traits::Pollable};
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::sync::mpsc::Sender;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time as tokio_time;
use tracing::instrument;
use tracing::{error, info};

pub struct IndependentPoller<T: Pollable> {
    provider: T,
    interval: Duration,
    tx: mpsc::Sender<PollEvent>,
}

impl<T: Pollable> IndependentPoller<T> {
    pub fn new(
        provider: T,
        interval: u64,
        tx: mpsc::Sender<PollEvent>,
    ) -> Self {
        let interval = Duration::from_secs(interval);
        Self {
            provider,
            interval,
            tx,
        }
    }

    pub async fn run(self) {
        let mut interval = tokio_time::interval(self.interval);
        let mut step = 0usize;
        // interval.tick().await;
        info!(
            "IndependentPoller started with interval={}s. Provider={} Strategy={:?}",
            self.interval.as_secs(),
            self.provider.get_provider_name(),
            Strategy::Independent,
        );
        loop {
            interval.tick().await;
            step += 1;
            let event = self.provider.fetch().await;
            let envelope = PollEvent {
                step,
                log_event: event,
                strategy: Strategy::Independent,
            };
            if let Err(e) = self.tx.send(envelope).await {
                error!("Failed to send poll event: {}", e);
            }
        }
    }
}

pub struct SynchronizedPoller {
    tasks: Vec<Box<dyn Pollable>>,
    interval: Duration,
    tx: mpsc::Sender<PollEvent>,
}

impl SynchronizedPoller {
    pub fn new(
        tasks: Vec<Box<dyn Pollable>>,
        interval: u64,
        tx: mpsc::Sender<PollEvent>,
    ) -> Self {
        let interval = Duration::from_secs(interval);
        Self {
            tasks,
            interval,
            tx,
        }
    }
    pub async fn run(self) {
        let mut interval = tokio_time::interval(self.interval);
        let mut step = 0usize;
        // interval.tick().await;
        info!(
            "SynchronizedPoller started with interval={}s. Count tasks={:?} Strategy={:?}",
            self.interval.as_secs(),
            self.tasks.len(),
            Strategy::Independent,
        );

        loop {
            interval.tick().await;
            step += 1;
            let mut futures = FuturesUnordered::new();
            for task in &self.tasks {
                futures.push(task.fetch());
            }

            while let Some(result) = futures.next().await {
                let envelope = PollEvent {
                    step,
                    log_event: result,
                    strategy: Strategy::Synchronized,
                };
                if let Err(e) = self.tx.send(envelope).await {
                    error!("Failed to send log event: {}", e);
                }
            }
        }
    }
}
