use async_trait::async_trait;
use chrono::Local;
use ftr::{
    Ftr, ProbeProtocol, TracerouteConfig, TracerouteConfigBuilder, traceroute,
};
use ping_async::{IcmpEchoRequestor, IcmpEchoStatus};
use std::fmt::format;
use std::net::IpAddr;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::{self, Instant};
use tracing::error;

use crate::constants::{DATE_FMT, TIME_FMT};
use crate::models::{TestEvent, TestType};
use crate::traits::Pollable;

pub struct Tracert {
    config: TracerouteConfig,
    target: IpAddr,
    max_hops: u8,
    probe_timeout_seconds: u64,
    queries_per_hop: u8,
}

impl Tracert {
    pub fn new(
        target: IpAddr,
        max_hops: u8,
        probe_timeout_seconds: u64,
        queries_per_hop: u8,
    ) -> Result<Self, String> {
        let config = match TracerouteConfigBuilder::new()
            .target(target.to_string())
            .protocol(ProbeProtocol::Icmp)
            .max_hops(max_hops)
            .queries_per_hop(queries_per_hop) // или .queries(1) в зависимости от версии
            .probe_timeout(Duration::from_millis(probe_timeout_seconds * 2000))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                return Err(format!("Ошибка конфигурации traceroute: {}", e));
            }
        };

        Ok(Self {
            config,
            target,
            max_hops,
            probe_timeout_seconds,
            queries_per_hop,
        })
    }

    pub async fn _traceroute(&self) -> String {
        let ftr = Ftr::new();

        let result = match ftr
            .trace_with_config(self.config.clone())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return format!("\n=== TRACEROUTE ERROR ===\n{}", e);
            }
        };

        if result.hops.is_empty() {
            return "\n=== TRACEROUTE ===\nNo hops recorded".to_string();
        }

        let mut output = String::from("\n=== TRACEROUTE ===\n");

        for hop in result.hops.iter() {
            let rtt_str = match hop.rtt {
                Some(d) => format!("{:.2}", d.as_secs_f64() * 1000.0),
                None => "*".to_string(),
            };

            let addr_str = match hop.addr {
                Some(addr) => addr.to_string(),
                None => "*".to_string(),
            };

            output.push_str(&format!(
                "{:2}. {} ({}ms)\n",
                hop.ttl, addr_str, rtt_str
            ));
        }

        output
    }

    pub async fn traceroute(&self) -> String {
        let target_str = self.target.to_string();

        #[cfg(windows)]
        let output = Command::new("tracert")
            .arg("-d")
            .arg("-h")
            .arg(self.max_hops.to_string())
            .arg("-w")
            .arg((self.probe_timeout_seconds * 1000).to_string())
            .arg(&target_str)
            .output()
            .await;

        #[cfg(target_os = "linux")]
        let output = Command::new("traceroute")
            .arg("-I") // ICMP
            .arg("-m")
            .arg(self.max_hops.to_string())
            .arg("-w")
            .arg(self.probe_timeout_seconds.to_string())
            .arg(&target_str)
            .output()
            .await;

        #[cfg(target_os = "macos")]
        let output = Command::new("traceroute")
            .arg("-I") // ICMP
            .arg("-m")
            .arg(self.max_hops.to_string())
            .arg("-w")
            .arg(self.timeout_secs.to_string())
            .arg(&target_str)
            .output()
            .await;

        match output {
            Ok(out) if out.status.success() => {
                let stdout = String::from_utf8_lossy(&out.stdout);
                format!("\n=== TRACEROUTE ===\n{}", stdout)
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                format!("\n=== TRACEROUTE ERROR ===\n{}", stderr)
            }
            Err(e) => {
                format!("\n=== TRACEROUTE ERROR ===\n{}", e)
            }
        }
    }
}

pub struct IcmpPoller {
    target: IpAddr,
    requestor: IcmpEchoRequestor,
    tracert: Option<Tracert>,
}

impl IcmpPoller {
    pub fn new(
        target: IpAddr,
        timeout_seconds: u64,
        tracert: Option<Tracert>,
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

    pub async fn ping(&self) -> TestEvent {
        let target = self.target.to_string();
        let test_type = TestType::Ping;

        let now = Local::now();
        let date = now.format(DATE_FMT).to_string();
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
                return TestEvent {
                    target,
                    date,
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
        TestEvent {
            target,
            date,
            start: started_at,
            end: finished_at,
            test_type,
            success,
            latency_ms,
            details: Some(details),
        }
    }

    //  async fn poll(
    //      self,
    //      interval_seconds: u64,
    //      tx_log: mpsc::Sender<TestEvent>,
    //      tx_diag: mpsc::Sender<()>,
    //  ) {
    //      let mut interval =
    //          time::interval(Duration::from_secs(interval_seconds));

    //      loop {
    //          let ping_event = self.ping().await;

    //          if let Err(e) = tx_log.send(ping_event).await {
    //              error!("Failed to send log event: {}", e);
    //              break;
    //          }

    //          interval.tick().await;
    //      }
    //  }
}

#[async_trait]
impl Pollable for IcmpPoller {
    async fn fetch(&self) -> TestEvent {
        self.ping().await
    }
}
