use crate::config;
use crate::constants::TIME_FMT;
use crate::models::{Event, FetchResult, IndependentPollerConfig, Strategy};
use crate::traits::Pollable;
use chrono::Local;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self as tokio_time, Instant};
use tracing::{error, info};

async fn poll_with_retries(
    provider: &dyn Pollable,
    retries: u8,
    retries_delay_ms: u64,
) -> FetchResult {
    let now = Local::now();
    let started_at = now.format(TIME_FMT).to_string();
    let mut details = Vec::with_capacity(retries as usize);
    let start_point = Instant::now();

    let mut success = false;
    let mut attempts = 0u8;

    for attempt in 1..=retries {
        attempts += 1;

        let detail = match provider.fetch().await {
            Ok(msg) => {
                success = true;
                format!("Попытка {attempt}: {msg}")
            }
            Err(e) => format!("Попытка {attempt}: {e}"),
        };
        details.push(detail);

        if success {
            break;
        }

        if attempt < retries {
            tokio::time::sleep(Duration::from_millis(retries_delay_ms)).await;
        }
    }

    let latency_ms = start_point.elapsed().as_secs_f64() * 1000.0;
    let finished_at = Local::now()
        .format(TIME_FMT)
        .to_string();

    FetchResult {
        target: provider.target(),
        start: started_at,
        end: finished_at,
        test_type: provider.whoami(),
        success,
        attempts,
        latency_ms,
        details: Some(details.join("; ")),
    }
}

#[derive(Debug, Clone)]
pub struct PollerConfig {
    pub interval_ms: u64,
    pub retries: u8,
    pub retries_delay_ms: u64,
}

impl PollerConfig {
    pub fn new(interval_ms: u64, retries: u8, retries_delay_ms: u64) -> Self {
        Self {
            interval_ms,
            retries,
            retries_delay_ms,
        }
    }
}

pub struct IndependentPoller<T: Pollable> {
    provider: T,
    config: PollerConfig,
    tx: mpsc::Sender<Event>,
}

impl<T: Pollable> IndependentPoller<T> {
    pub fn new(
        provider: T,
        config: PollerConfig,
        tx: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            provider,
            config,
            tx,
        }
    }

    pub fn dump(&self) -> IndependentPollerConfig {
        IndependentPollerConfig {
            provider: self.provider.dump(),
            retries: self.config.retries,
            retries_interval_ms: self.config.retries_delay_ms,
            interval_ms: self.config.interval_ms,
        }
    }

    pub async fn run(self) {
        let mut step = 0usize;
        let duration = Duration::from_millis(self.config.interval_ms);
        let mut interval = tokio_time::interval(duration);
        let target = self.provider.target();
        let test_type = self.provider.whoami();

        // interval.tick().await;
        info!(
            "IndependentPoller started with interval={}ms. Provider={} Strategy={:?}",
            duration.as_millis(),
            self.provider.whoami(),
            Strategy::Independent,
        );
        loop {
            interval.tick().await;
            step += 1;
            let payload = poll_with_retries(
                &self.provider,
                self.config.retries,
                self.config.retries_delay_ms,
            )
            .await;

            let envelope = Event::PollResult {
                strategy: Strategy::Independent,
                step,
                payload,
            };
            if let Err(e) = self.tx.send(envelope).await {
                error!("Failed to send poll event: {}", e);
            }
        }
    }
}

pub struct SynchronizedPoller {
    providers: Vec<Box<dyn Pollable>>,
    interval: Duration,
    tx: mpsc::Sender<Event>,
}

/*
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
            Strategy::Synchronized,
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
*/
