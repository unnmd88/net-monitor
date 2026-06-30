use async_snmp::{Auth, Client, Oid, Retry};
use async_trait::async_trait;
use chrono::Local;
use clap::builder::Str;
use serde_json::json;
use std::net::IpAddr;
use std::time::Duration;
use tokio::time::Instant;

use crate::constants::TIME_FMT;
use crate::models::{Event, PollType, ProviderConfig};
use crate::traits::Pollable;

pub struct SnmpProvider {
    username: String,
    pub target: IpAddr,
    pub port: u16,
    client: Client,
    oids: Vec<Oid>,
    pub timeout_ms: u64,
}

impl SnmpProvider {
    pub async fn new(
        username: String,
        target: IpAddr,
        port: u16,
        community: String,
        oids: Vec<String>,
        timeout_ms: u64,
        //retries: u32,
    ) -> Result<Self, String> {
        let client = match Client::builder(
            (target.to_string(), port),
            Auth::v2c(&community),
        )
        .timeout(Duration::from_millis(timeout_ms))
        //.retry(Retry::fixed(retries, Duration::ZERO))
        .retry(Retry::none())
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
            username: username,
            target,
            port,
            client,
            oids,
            timeout_ms,
        })
    }

    pub async fn get_many(&self) -> Result<String, String> {
        match self.client.get_many(&self.oids).await {
            Ok(results) => {
                let values = results
                    .iter()
                    .map(|varbind| {
                        format!("{}={:?}", varbind.oid, varbind.value)
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                Ok(values)
            }
            Err(e) => Err(format!("SNMP error: {}", e)),
        }
    }

    fn get_oids(&self) -> Vec<String> {
        self.oids
            .iter()
            .map(|o| o.to_string())
            .collect()
    }

    fn get_extra(&self) -> Option<serde_json::Value> {
        None
    }

    pub fn dump(&self) -> ProviderConfig {
        ProviderConfig::Snmp {
            username: (self.username()),
            target: self.target,
            timeout_ms: self.timeout_ms,
            port: self.port,
            oids: self.get_oids(),
            extra: self.get_extra(),
        }
    }

    pub fn username(&self) -> String {
        self.username.clone()
    }
}

#[async_trait]
impl Pollable for SnmpProvider {
    async fn fetch(&self) -> Result<String, String> {
        self.get_many().await
    }

    fn dump(&self) -> ProviderConfig {
        self.dump()
    }

    fn target(&self) -> IpAddr {
        self.target
    }

    fn whoami(&self) -> PollType {
        PollType::Snmp
    }

    fn username(&self) -> String {
        self.username()
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
