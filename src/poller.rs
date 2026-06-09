use crate::{models::TestEvent, traits::Pollable};
use std::sync::mpsc::Sender;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time as tokio_time;
use tracing::error;

pub struct Poller<T: Pollable> {
    task: T,
    interval: Duration,
    tx: mpsc::Sender<TestEvent>,
}

impl<T: Pollable> Poller<T> {
    pub fn new(task: T, interval: u64, tx: mpsc::Sender<TestEvent>) -> Self {
        let interval = Duration::from_secs(interval);
        Self { task, interval, tx }
    }

    pub async fn run(self) {
        let mut interval = tokio_time::interval(self.interval);
        interval.tick().await;
        loop {
            interval.tick().await;
            let test_event = self.task.fetch().await;
            if let Err(e) = self.tx.send(test_event).await {
                error!("Failed to send log event: {}", e);
            }
        }
    }
}
