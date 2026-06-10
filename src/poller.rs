use crate::config::Strategy;
use crate::models::PollEvent;
use crate::{models::LogEvent, traits::Pollable};
use std::sync::mpsc::Sender;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time as tokio_time;
use tracing::error;

pub struct Poller<T: Pollable> {
    task: T,
    interval: Duration,
    tx: mpsc::Sender<PollEvent>,
}

impl<T: Pollable> Poller<T> {
    pub fn new(task: T, interval: u64, tx: mpsc::Sender<PollEvent>) -> Self {
        let interval = Duration::from_secs(interval);
        Self { task, interval, tx }
    }

    pub async fn run(self) {
        let mut interval = tokio_time::interval(self.interval);
        let mut step = 0usize;
        interval.tick().await;
        loop {
            interval.tick().await;
            step += 1;
            let event = self.task.fetch().await;
            let envelope = PollEvent {
                step,
                log_event: event,
                strategy: Strategy::Independent,
            };
            if let Err(e) = self.tx.send(envelope).await {
                error!("Failed to send log event: {}", e);
            }
        }
    }
}
