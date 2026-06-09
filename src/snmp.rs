use async_snmp::{Auth, Client, Oid, Retry};
use chrono::Local;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tracing::error;

use crate::constants::{DATE_FMT, TIME_FMT};
use crate::models::{TestEvent, TestType};
use crate::utils::{get_fmt_current_time, validate_oids};

pub async fn snmp_task(
    target: String,
    port: u16,
    community: String,
    oids: Vec<String>,
    interval_secs: u64,
    timeout_secs: u64,
    retries: u32,
    tx_log: mpsc::Sender<TestEvent>,
) {
    let mut interval = time::interval(Duration::from_secs(interval_secs));

    if let Err(message) = validate_oids(&oids) {
        error!("{}", message);
        std::process::exit(1);
    }

    let parsed_oids = match create_oids(&oids) {
        Ok(good_oids) => good_oids,
        Err(message) => {
            error!("{}", message);
            std::process::exit(1);
        }
    };

    let client =
        match Client::builder((target.as_str(), port), Auth::v2c(&community))
            .timeout(Duration::from_secs(timeout_secs))
            .retry(Retry::fixed(retries, Duration::ZERO))
            .connect()
            .await
        {
            Ok(c) => c,
            Err(e) => {
                error!(
                    "SNMP: failed to connect to {}:{} — {}",
                    target, port, e
                );
                std::process::exit(1);
            }
        };

    loop {
        interval.tick().await;
        let start = Instant::now();

        let now = Local::now();
        let date = now.format(DATE_FMT).to_string();
        let req_start = now.format(TIME_FMT).to_string();
        let result = client.get_many(&parsed_oids).await;
        let req_end = Local::now()
            .format(TIME_FMT)
            .to_string();
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;

        let (success, details) = match result {
            Ok(results) => {
                let values = results
                    .iter()
                    .map(|r| format!("{}={:?}", r.oid, r.value))
                    .collect::<Vec<_>>()
                    .join(" | ");
                (true, Some(format!("{}", values)))
            }
            Err(e) => (false, Some(format!("SNMP error: {}", e))),
        };

        if tx_log
            .send(TestEvent {
                date,
                target: target.clone(),
                start: req_start,
                end: req_end,
                test_type: TestType::Snmp,
                success,
                latency_ms: elapsed,
                details,
            })
            .await
            .is_err()
        {
            error!("SNMP: channel closed, exiting");
            return;
        }
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
