use crate::config;
use crate::constants::TIME_FMT;
use crate::models::{Event, FetchResult, IndependentPollerConfig, Strategy};
use crate::traits::Pollable;
use chrono::Local;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self as tokio_time, Instant, interval};
use tracing::{error, info};

async fn poll_with_retries(
    provider: &dyn Pollable,
    config: &PollerConfig,
) -> FetchResult {
    let now = Local::now();
    let started_at = now.format(TIME_FMT).to_string();
    let mut details = Vec::with_capacity(config.retries.into());
    let start_point = Instant::now();

    let mut success = false;
    let mut attempts = 0u8;

    for attempt in 1..=config.retries {
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

        if attempt < config.retries {
            tokio::time::sleep(Duration::from_millis(config.retries_delay_ms))
                .await;
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

pub struct IndependentPoller {
    provider: Box<dyn Pollable>,
    config: PollerConfig,
    tx: mpsc::Sender<Event>,
}

impl IndependentPoller {
    pub fn new(
        provider: Box<dyn Pollable>,
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

        info!(
            "IndependentPoller started with interval={}ms. Provider={} Strategy={:?}",
            duration.as_millis(),
            self.provider.whoami(),
            Strategy::Independent,
        );
        loop {
            interval.tick().await;
            step += 1;
            let payload =
                poll_with_retries(self.provider.as_ref(), &self.config).await;

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
    config: PollerConfig,
    tx: mpsc::Sender<Event>,
}

impl SynchronizedPoller {
    pub fn new(
        providers: Vec<Box<dyn Pollable>>,
        config: PollerConfig,
        tx: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            providers: providers,
            config,
            tx,
        }
    }

    pub async fn run(self) {
        let duration = Duration::from_millis(self.config.interval_ms);
        let mut interval = tokio_time::interval(duration);
        let mut step = 0usize;
        // interval.tick().await;

        info!(
            "SynchronizedPoller started with interval={}ms. Count providers={:?} Strategy={:?}",
            duration.as_millis(),
            self.providers.len(),
            Strategy::Synchronized,
        );

        for (i, p) in self.providers.iter().enumerate() {
            info!("Provider {}: {}", i + 1, p.whoami())
        }

        loop {
            interval.tick().await;
            step += 1;
            let mut futures = FuturesUnordered::new();
            for provider in &self.providers {
                futures
                    .push(poll_with_retries(provider.as_ref(), &self.config));
            }

            while let Some(payload) = futures.next().await {
                let envelope = Event::PollResult {
                    strategy: Strategy::Synchronized,
                    step,
                    payload,
                };
                if let Err(e) = self.tx.send(envelope).await {
                    error!("Failed to send poll event: {}", e);
                }
            }
        }
    }
}
