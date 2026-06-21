use crate::config;
use crate::constants::TIME_FMT;
use crate::models::{
    Event, FetchResult, IndependentPollerConfig, Strategy,
    SynchronizedPollerConfig,
};
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
    poll_config: &PollConfig,
) -> FetchResult {
    let now = Local::now();
    let started_at = now.format(TIME_FMT).to_string();
    let mut details = Vec::with_capacity(poll_config.retries.into());
    let start_point = Instant::now();

    let mut success = false;
    let mut attempts = 0u8;

    for attempt in 1..=poll_config.retries {
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

        if attempt < poll_config.retries {
            tokio::time::sleep(Duration::from_millis(
                poll_config.retries_delay_ms,
            ))
            .await;
        }
    }

    let latency_ms = start_point.elapsed().as_secs_f64() * 1000.0;
    let finished_at = Local::now()
        .format(TIME_FMT)
        .to_string();

    FetchResult {
        username: provider.username(),
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
pub struct PollConfig {
    //pub interval_ms: u64,
    pub retries: u8,
    pub retries_delay_ms: u64,
}

pub struct IndependentPoller {
    provider: Box<dyn Pollable>,
    interval_ms: u64,
    poll_config: PollConfig,
    tx: mpsc::Sender<Event>,
}

impl IndependentPoller {
    pub fn new(
        provider: Box<dyn Pollable>,
        interval_ms: u64,
        poll_config: PollConfig,
        tx: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            provider,
            interval_ms,
            poll_config,
            tx,
        }
    }

    pub fn dump(&self) -> IndependentPollerConfig {
        IndependentPollerConfig {
            provider: self.provider.dump(),
            retries: self.poll_config.retries,
            retries_interval_ms: self.poll_config.retries_delay_ms,
            interval_ms: self.interval_ms,
        }
    }

    pub async fn run(self) {
        let mut step = 0usize;
        let duration = Duration::from_millis(self.interval_ms);
        let mut interval = tokio_time::interval(duration);

        info!(
            "IndependentPoller started. Interval={}ms retries={} retries_interval={}ms. Provider={} Strategy={:?}",
            duration.as_millis(),
            self.poll_config.retries,
            self.poll_config.retries_delay_ms,
            self.provider.whoami(),
            Strategy::Independent,
        );

        println!(
            "Опрос {} запущен. Интервал={}мс. Количество попыток в опросе={}. Пауза между попытками: {}мс",
            self.provider.whoami(),
            duration.as_millis(),
            self.poll_config.retries,
            self.poll_config.retries_delay_ms,
        );

        loop {
            interval.tick().await;
            step += 1;
            let payload =
                poll_with_retries(self.provider.as_ref(), &self.poll_config)
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
    providers: Vec<(Box<dyn Pollable>, PollConfig)>,
    interval_ms: u64,
    tx: mpsc::Sender<Event>,
}

impl SynchronizedPoller {
    pub fn new(
        providers: Vec<(Box<dyn Pollable>, PollConfig)>,
        interval_ms: u64,
        tx: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            providers: providers,
            interval_ms,
            tx,
        }
    }

    pub fn dump(&self) -> SynchronizedPollerConfig {
        SynchronizedPollerConfig {
            providers: self
                .providers
                .iter()
                .map(|(provider, config)| provider.dump())
                .collect(),
            interval_ms: self.interval_ms,
            num_providers: self.providers.len() as u8,
        }
    }

    pub fn interval_ms(&self) -> u64 {
        self.interval_ms
    }

    pub async fn run(self) {
        let duration = Duration::from_millis(self.interval_ms);
        let mut interval = tokio_time::interval(duration);
        let mut step = 0usize;
        // interval.tick().await;

        info!(
            "SynchronizedPoller started with interval={}ms. Count providers={:?} Strategy={:?}",
            duration.as_millis(),
            self.providers.len(),
            Strategy::Synchronized,
        );

        let mut oup_message = format!(
            "Запущен синхронизированный опрос c интервалом={}мс. Общее количество опросов={}.",
            duration.as_millis(),
            self.providers.len(),
        );

        for (i, (provider, config)) in self.providers.iter().enumerate() {
            let cnt = i + 1;
            let name = provider.whoami();

            info!("Provider {cnt}: {name} config: {:?}", config);

            let current = format!(
                "\nОпрос № {}: {} запущен. Интервал={}мс. Количество попыток в опросе={}. Пауза между попытками: {}мс",
                cnt,
                name,
                duration.as_millis(),
                config.retries,
                config.retries_delay_ms,
            );
            oup_message.push_str(&current);
        }
        println!("{oup_message}");

        loop {
            interval.tick().await;
            step += 1;
            let mut futures = FuturesUnordered::new();
            for (provider, config) in &self.providers {
                futures.push(poll_with_retries(provider.as_ref(), &config));
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
