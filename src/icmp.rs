use async_trait::async_trait;
use chrono::Local;
use ping_async::{IcmpEchoReply, IcmpEchoRequestor, IcmpEchoStatus};
use serde::Serialize;
use serde_json;
use serde_json::json;
use std::net::IpAddr;
use std::time::Duration;
use tokio::time::Instant;
use tokio::time::sleep;
use trippy_core::{Builder, Protocol};

use crate::constants::TIME_FMT;
use crate::models::{Event, PollType, ProviderConfig};
use crate::traits::Pollable;

#[derive(Debug, Serialize)]
pub struct TracertProvider {
    target: IpAddr,
    max_hops: u8,
    // probe_timeout_seconds: u64,
    queries_per_hop: u8,
}

impl TracertProvider {
    pub fn new(
        target: IpAddr,
        max_hops: u8,
        // probe_timeout_seconds: u64,
        queries_per_hop: u8,
    ) -> Result<Self, String> {
        Ok(Self {
            target,
            max_hops,
            // probe_timeout_seconds,
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

pub struct IcmpProvider {
    pub target: IpAddr,
    requestor: IcmpEchoRequestor,
    tracert: Option<TracertProvider>,
    pub timeout_ms: u64,
}

impl IcmpProvider {
    pub fn new(
        target: IpAddr,
        timeout_ms: u64,
        tracert: Option<TracertProvider>,
    ) -> Result<Self, String> {
        let requestor = IcmpEchoRequestor::new(
            target,
            None,
            None,
            //Some(Duration::from_secs(timeout_seconds)),
            Some(Duration::from_millis(timeout_ms)),
        )
        .map_err(|e| format!("Ошибка создания ping-опроса: {e}"))?;

        Ok(Self {
            target,
            requestor,
            tracert,
            timeout_ms,
        })
    }

    /*
    pub async fn ping(&self) -> LogEvent {
        let target = self.target.to_string();
        let test_type = PollType::Ping;

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
    */

    /*
    pub async fn ping2(&self) -> LogEvent {
        let target = self.target.to_string();
        let test_type = PollType::Ping;

        let now = Local::now();
        let started_at = now.format(TIME_FMT).to_string();

        let mut details: Vec<String> = Vec::with_capacity(self.retries);

        let start_point = Instant::now();

        let mut success = false;
        let mut attempts = 0u8;

        while attempts < self.retries {
            attempts += 1;

            let result = match self.ping_once().await {
                Ok(msg) => {
                    success = true;
                    format!("Попытка {attempt}: {msg}")
                }
                Err(e) => format!("Попытка {attempt}: {e}"),
            };
            details.push(result);
            if success {
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }

        for attempt in 0..=4 {
            let result = match self.ping_once().await {
                Ok(msg) => {
                    success = true;
                    format!("Попытка {attempt}: {msg}")
                }
                Err(e) => format!("Попытка {attempt}: {e}"),
            };
            details.push(result);
            if success {
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }

        let latency_ms = start_point.elapsed().as_secs_f64() * 1000.0;
        let finished_at = Local::now()
            .format(TIME_FMT)
            .to_string();

        LogEvent::PollResult {
            target,
            start: started_at,
            end: finished_at,
            test_type,
            success,
            latency_ms,
            details: Some(details.join("; ")),
        }
    }
    */

    async fn ping_once(&self) -> Result<String, String> {
        // let reply = self.requestor.send().await.map_err(|e| format!("Ошибка: {}", e))?;

        let reply = match self.requestor.send().await {
            Ok(reply) => reply,
            Err(e) => return Err(format!("Ошибка: {}", e)),
        };

        let result = match reply.status() {
            IcmpEchoStatus::Success => {
                Ok(format!("Успех. RTT: {:?}", reply.round_trip_time()))
            }
            IcmpEchoStatus::TimedOut => Err(format!(
                "Превышен таймаут ответа.: {:?}",
                reply.round_trip_time()
            )),
            IcmpEchoStatus::Unreachable => Err(format!(
                "Хост недоступен. RTT: {:?}",
                reply.round_trip_time()
            )),
            IcmpEchoStatus::Unknown => Err(format!("Ошибка запроса(Unknown)")),
        };

        result
    }

    fn get_extra(&self) -> Option<serde_json::Value> {
        self.tracert.as_ref().and_then(|t| {
            serde_json::to_value(t)
                .ok()
                .map(|v| json!({"tracert": v}))
        })
    }

    pub fn dump(&self) -> ProviderConfig {
        ProviderConfig {
            name: PollType::Ping,
            target: self.target,
            timeout_ms: self.timeout_ms,
            extra: self.get_extra(),
        }
    }

    pub fn get_name(&self) -> String {
        "IcmpProvider".to_string()
    }
}

#[async_trait]
impl Pollable for IcmpProvider {
    async fn fetch(&self) -> Result<String, String> {
        //self.ping().await
        //self.ping2().await
        self.ping_once().await
    }

    fn dump(&self) -> ProviderConfig {
        self.dump()
    }

    fn target(&self) -> IpAddr {
        self.target
    }

    fn whoami(&self) -> PollType {
        PollType::Ping
    }

    fn get_provider_name(&self) -> String {
        self.get_name()
    }
}
