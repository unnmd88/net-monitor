use async_trait::async_trait;
use chrono::Local;
use ftr::{
    Ftr, ProbeProtocol, TracerouteConfig, TracerouteConfigBuilder, traceroute,
};
use ping_async::{IcmpEchoRequestor, IcmpEchoStatus};
use std::cell::RefCell;
use std::fmt::format;
use std::net::IpAddr;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tracing::error;
use trippy_core::{Builder, ProbeStatus, Protocol};

use crate::constants::{DATE_FMT, TIME_FMT};
use crate::models::{LogEvent, TestType};
use crate::traits::Pollable;

pub struct TracertPoller {
    target: IpAddr,
    max_hops: u8,
    probe_timeout_seconds: u64,
    queries_per_hop: u8,
}

impl TracertPoller {
    pub fn new(
        target: IpAddr,
        max_hops: u8,
        probe_timeout_seconds: u64,
        queries_per_hop: u8,
    ) -> Result<Self, String> {
        Ok(Self {
            target,
            max_hops,
            probe_timeout_seconds,
            queries_per_hop,
        })
    }

    pub async fn traceroute(&self) -> String {
        let mut output = String::new();

        let tracer = match Builder::new(self.target)
            .protocol(Protocol::Icmp)
            .first_ttl(1)
            .max_ttl(self.max_hops)
            .max_rounds(Some(self.queries_per_hop as usize))
            .build()
        {
            Ok(t) => t,
            Err(e) => return format!("Ошибка создания трассировщика: {}", e),
        };

        if let Err(e) = tracer.run() {
            return format!("Ошибка выполнения трассировки: {}", e);
        }

        let state = tracer.snapshot();

        output.push_str(&format!("\n=== Traceroute to {} ===\n", self.target));
        output.push_str(&format!(
            "{:<3} {:<20} {:<15} {:<10}\n",
            "TTL", "Address", "Avg RTT (ms)", "Loss%"
        ));
        output.push_str(&"-".repeat(60));
        output.push('\n');

        for hop in state.hops() {
            let ttl = hop.ttl();

            // addrs() возвращает итератор, берём первый адрес
            let addr = hop
                .addrs()
                .next()
                .map(|a| a.to_string())
                .unwrap_or_else(|| "*".to_string());

            // avg_ms() возвращает f64 напрямую
            let rtt = hop.avg_ms();
            let rtt_str = if rtt > 0.0 {
                format!("{:.2}", rtt)
            } else {
                "---".to_string()
            };

            let loss = hop.loss_pct();

            output.push_str(&format!(
                "{:<3} {:<20} {:<15} {:<10.1}%\n",
                ttl, addr, rtt_str, loss
            ));
        }

        output
    }
}

pub struct IcmpPoller {
    target: IpAddr,
    requestor: IcmpEchoRequestor,
    tracert: Option<TracertPoller>,
}

impl IcmpPoller {
    pub fn new(
        target: IpAddr,
        timeout_seconds: u64,
        tracert: Option<TracertPoller>,
    ) -> Result<Self, String> {
        let requestor = IcmpEchoRequestor::new(
            target,
            None,
            None,
            Some(Duration::from_secs(timeout_seconds)),
        )
        .map_err(|e| format!("Ошибка создания ping-опроса: {e}"))?;

        Ok(Self {
            target,
            requestor,
            tracert,
        })
    }

    pub async fn ping(&self) -> LogEvent {
        let target = self.target.to_string();
        let test_type = TestType::Ping;

        let now = Local::now();
        let started_at = now.format(TIME_FMT).to_string();

        let start_point = Instant::now();
        let request = self.requestor.send().await;
        let latency_ms = start_point.elapsed().as_secs_f64() * 1000.0;
        let finished_at = Local::now()
            .format(TIME_FMT)
            .to_string();

        let reply = match request {
            Ok(reply) => reply,
            Err(e) => {
                return LogEvent::PollResult {
                    target,
                    start: started_at,
                    end: finished_at,
                    test_type,
                    success: false,
                    latency_ms,
                    details: Some(format!("Ошибка: {}", e)),
                };
            }
        };
        let (success, details) = match reply.status() {
            IcmpEchoStatus::Success => {
                (true, format!("Успех. RTT: {:?}", reply.round_trip_time()))
            }
            IcmpEchoStatus::TimedOut => {
                let default_message = format!(
                    "Превышен таймаут ответа.: {:?}",
                    reply.round_trip_time()
                );

                let details = match &self.tracert {
                    Some(tracer) => {
                        let output = tracer.traceroute().await;
                        format!("{}{}", default_message, output)
                    }
                    None => default_message,
                };

                (false, details)
            }
            IcmpEchoStatus::Unreachable => {
                let default_message = format!(
                    "Хост недоступен. RTT: {:?}",
                    reply.round_trip_time()
                );

                let details = match &self.tracert {
                    Some(tracer) => {
                        let output = tracer.traceroute().await;
                        format!("{}{}", default_message, output)
                    }
                    None => default_message,
                };

                (false, details)
            }
            IcmpEchoStatus::Unknown => (false, format!("Ошибка запроса")),
        };
        LogEvent::PollResult {
            target,
            start: started_at,
            end: finished_at,
            test_type,
            success,
            latency_ms,
            details: Some(details),
        }
    }
}

#[async_trait]
impl Pollable for IcmpPoller {
    async fn fetch(&self) -> LogEvent {
        self.ping().await
    }
}
