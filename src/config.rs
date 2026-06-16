use std::net::IpAddr;

use serde::Deserialize;

use crate::models::{PollType, Strategy};

#[derive(Debug, Deserialize)]
pub struct Config {
    //pub output: OutputConfig,
    pub log: String,
    pub network: NetworkConfig,
    pub ping: PingConfig,
    pub tracert: TracertConfig,
    pub snmp: SnmpConfig,
    pub independent: IndependentStrategyConfig,
    pub synchronized: SynchronizedStrategyConfig,
    pub strategy: Strategy,
}

// ============================================
// ВЫХОДНЫЕ ФАЙЛЫ
// ============================================

//#[derive(Debug, Deserialize)]
//pub struct OutputConfig {
//    pub csv_path: String,
//    pub txt_path: String,
//}

// ============================================
// СЕТЕВЫЕ НАСТРОЙКИ
// ============================================

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub target: IpAddr,
}

// ============================================
// НАСТРОЙКИ PING
// ============================================

#[derive(Debug, Deserialize)]
pub struct PingConfig {
    pub timeout_seconds: u64,
    pub fallback_tracert: bool,
    //pub fallback_tracert_delay_seconds: u64,
}

// ============================================
// НАСТРОЙКИ TRACERT
// ============================================

#[derive(Debug, Deserialize)]
pub struct TracertConfig {
    pub max_hops: u8,
    //pub probe_timeout_seconds: u64,
    pub queries_per_hop: u8,
}

// ============================================
// НАСТРОЙКИ SNMP
// ============================================

#[derive(Debug, Deserialize)]
pub struct SnmpConfig {
    pub timeout_seconds: u64,
    pub port: u16,
    pub community: String,
    pub retries: u32,
    pub oids: Vec<String>,
}

// ============================================
// ПАРАЛЛЕЛЬНАЯ СТРАТЕГИЯ
// ============================================

#[derive(Debug, Deserialize)]
pub struct IndependentStrategyConfig {
    pub ping: IndependentPingConfig,
    pub snmp: IndependentSnmpConfig,
}

#[derive(Debug, Deserialize)]
pub struct IndependentPingConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
}

#[derive(Debug, Deserialize)]
pub struct IndependentSnmpConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
}

// ============================================
// ПОСЛЕДОВАТЕЛЬНАЯ СТРАТЕГИЯ
// ============================================

#[derive(Debug, Deserialize)]
pub struct SynchronizedStrategyConfig {
    pub interval_seconds: u64,
    pub jobs: Vec<PollType>,
}
