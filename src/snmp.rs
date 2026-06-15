use async_snmp::{Auth, Client, Oid, Retry};
use async_trait::async_trait;
use chrono::Local;
use std::net::IpAddr;
use std::sync::mpsc::Sender;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tracing::error;

use crate::constants::{DATE_FMT, TIME_FMT};
use crate::models::{LogEvent, PollType};
use crate::traits::Pollable;
use crate::utils::{get_fmt_current_time, validate_oids};

pub struct SnmpProvider {
    target: IpAddr,
    port: u16,
    client: Client,
    oids: Vec<Oid>,
    // target: IpAddr,
    // port: u16,
    // community: String,
    // oids: Vec<String>,
    // interval_secs: u64,
    // timeout_secs: u64,
    // retries: u32,
}

impl SnmpProvider {
    pub async fn new(
        target: IpAddr,
        port: u16,
        community: String,
        oids: Vec<String>,
        interval_seconds: u64,
        timeout_seconds: u64,
        retries: u32,
    ) -> Result<Self, String> {
        let client = match Client::builder(
            (target.to_string(), port),
            Auth::v2c(&community),
        )
        .timeout(Duration::from_secs(timeout_seconds))
        .retry(Retry::fixed(retries, Duration::ZERO))
        .connect()
        .await
        {
            Ok(c) => c,
            Err(e) => {
                return Err(format!(
                    "SNMP: failed to connect to {}:{} — {}",
                    target, port, e
                ));
            }
        };

        let oids = create_oids(&oids)?;

        Ok(Self {
            target,
            port,
            client,
            oids,
        })
    }

    pub async fn get_many(&self) -> LogEvent {
        let now = Local::now();
        let start = Instant::now();

        let req_start = now.format(TIME_FMT).to_string();
        let result = self.client.get_many(&self.oids).await;
        let req_end = Local::now()
            .format(TIME_FMT)
            .to_string();

        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        let (success, details) = match result {
            Ok(results) => {
                let values = results
                    .iter()
                    .map(|varbind| {
                        format!("{}={:?}", varbind.oid, varbind.value)
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                (true, format!("{}", values))
            }
            Err(e) => (false, format!("SNMP error: {}", e)),
        };

        LogEvent::PollResult {
            target: self.target.to_string(),
            start: req_start,
            end: req_end,
            test_type: PollType::Snmp,
            success,
            latency_ms,
            details: Some(details),
        }
    }
}

#[async_trait]
impl Pollable for SnmpProvider {
    async fn fetch(&self) -> LogEvent {
        self.get_many().await
    }

    fn get_provider_name(&self) -> String {
        "SnmpProvider".to_string()
    }
}

fn create_oids(oids: &[String]) -> Result<Vec<Oid>, String> {
    oids.iter()
        .map(|oid| {
            let parts: Vec<u32> = oid
                .split('.')
                .map(|part| {
                    part.parse::<u32>().map_err(|_| {
                        format!(
                            "Internal error: invalid OID '{}' passed to create_oids",
                            oid
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Oid::from(parts))
        })
        .collect()
}
