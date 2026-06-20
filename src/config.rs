use std::net::IpAddr;

use serde::Deserialize;

use crate::models::{PollType, Strategy};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub strategy: Strategy,
    pub log: String,
    pub network: NetworkConfig,
    pub independent: IndependentStrategyConfig,
    pub synchronized: SynchronizedStrategyConfig,
}

// ============================================
// NETWORK
// ============================================

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub target: IpAddr,
}

// ============================================
// INDEPENDENT STRATEGY
// ============================================
#[derive(Debug, Deserialize, Default)]
pub struct IndependentStrategyConfig {
    pub tracert: IndependentTracertConfig,
    pub ping: IndependentPingConfig,
    pub snmp: IndependentSnmpConfig,
}

#[derive(Debug, Deserialize, Default)]
pub struct IndependentTracertConfig {
    pub enabled: bool,
    pub max_hops: u32,
    pub queries_per_hop: u32,
}

// Ping для Independent
#[derive(Debug, Deserialize, Default)]
pub struct IndependentPingConfig {
    pub enabled: bool,
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub fallback_tracert: bool,
    pub retries: u8,
    pub retries_delay_ms: u64,
}

// Snmp для Independent
#[derive(Debug, Deserialize, Default)]
pub struct IndependentSnmpConfig {
    pub enabled: bool,
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub port: u16,
    pub community: String,
    pub retries: u8,
    pub retries_delay_ms: u64,
    pub oids: Vec<String>,
}

// ============================================
// SYNCHRONIZED STRATEGY
// ============================================
#[derive(Debug, Deserialize, Default)]
pub struct SynchronizedStrategyConfig {
    pub interval_ms: u64, // общий для всех провайдеров
    pub ping: SynchronizedPingConfig,
    pub snmp: SynchronizedSnmpConfig,
}

#[derive(Debug, Deserialize, Default)]
pub struct SynchronizedPingConfig {
    pub enabled: bool,
    pub timeout_ms: u64,
    pub fallback_tracert: bool,
    pub retries: u8,
    pub retries_delay_ms: u64,
}

#[derive(Debug, Deserialize, Default)]
pub struct SynchronizedSnmpConfig {
    pub enabled: bool,
    pub timeout_ms: u64,
    pub port: u16,
    pub community: String,
    pub retries: u8,
    pub retries_delay_ms: u64,
    pub oids: Vec<String>,
}
