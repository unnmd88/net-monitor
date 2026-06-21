use serde::Deserialize;
use std::net::IpAddr;
use tracing::{error, info};

use crate::models::Strategy;

// ============================================
// CONFIG
// ============================================
#[derive(Debug, Deserialize)]
pub struct Config {
    pub strategy: Strategy,
    pub log: String,
    pub network: NetworkConfig,
    pub independent: IndependentStrategyConfig,
    pub synchronized: SynchronizedStrategyConfig,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            format!("❌ Не удалось прочитать '{}': {}", path, e)
        })?;

        let config: Config = toml::from_str(&contents).map_err(|e| {
            format!("❌ Ошибка в конфиге: {}", user_friendly_error(&e))
        })?;

        info!("Config '{}' loaded successfully.", path);
        Ok(config)
    }

    pub fn generate_default(
        output: &str,
        force: bool,
        show: bool,
    ) -> Result<(), String> {
        let default = r#"strategy = "independent"
log = "monitor.json"

[network]
target = "10.179.180.190"

# ============================================
# INDEPENDENT STRATEGY
# ============================================

# ---------- PING ----------
[[independent.ping]]
name = "fast"
interval_ms = 1000
timeout_ms = 200
fallback_tracert = false
retries = 3
retries_delay_ms = 200

[[independent.ping]]
name = "slow"
interval_ms = 5000
timeout_ms = 500
fallback_tracert = true
retries = 5
retries_delay_ms = 500

# ---------- SNMP ----------
[[independent.snmp]]
name = "main"
interval_ms = 6000
timeout_ms = 350
port = 161
community = "UTMC"
retries = 2
retries_delay_ms = 300
oids = [
    "1.3.6.1.2.1.1.3.0",
    "1.3.6.1.4.1.13267.3.2.4.1.0",
]

# ---------- TRACERT ----------
[[independent.tracert]]
name = "default"
interval_ms = 30000
max_hops = 30
queries_per_hop = 1

# ============================================
# SYNCHRONIZED STRATEGY
# ============================================

[synchronized]
interval_ms = 4000

# ---------- PING ----------
[[synchronized.ping]]
name = "primary"
timeout_ms = 200
fallback_tracert = true
retries = 3
retries_delay_ms = 200

# ---------- SNMP ----------
[[synchronized.snmp]]
name = "main"
timeout_ms = 350
port = 161
community = "UTMC"
retries = 2
retries_delay_ms = 300
oids = [
    "1.3.6.1.2.1.1.3.0",
]

# ---------- TRACERT ----------
[[synchronized.tracert]]
name = "default"
max_hops = 30
queries_per_hop = 1
"#;

        // Если --show — просто печатаем в консоль
        if show {
            println!("{}", default);
            return Ok(());
        }

        // Проверяем, существует ли файл
        if std::fs::metadata(output).is_ok() && !force {
            return Err(format!(
                "❌ Файл '{}' уже существует.\n\
                 💡 Используйте --force для перезаписи.",
                output
            ));
        }

        // Сохраняем в файл
        std::fs::write(output, default).map_err(|e| {
            format!("❌ Не удалось записать '{}': {}", output, e)
        })?;

        println!("✅ Дефолтный конфиг создан: {}", output);
        Ok(())
    }
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
    pub ping: Vec<IndependentPingInstance>,
    pub snmp: Vec<IndependentSnmpInstance>,
    pub tracert: Vec<IndependentTracertInstance>,
}

// ---------- PING ----------
#[derive(Debug, Deserialize)]
pub struct IndependentPingInstance {
    pub name: String,
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub fallback_tracert: bool,
    pub retries: u8,
    pub retries_delay_ms: u64,
}

// ---------- SNMP ----------
#[derive(Debug, Deserialize)]
pub struct IndependentSnmpInstance {
    pub name: String,
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub port: u16,
    pub community: String,
    pub retries: u8,
    pub retries_delay_ms: u64,
    pub oids: Vec<String>,
}

// ---------- TRACERT ----------
#[derive(Debug, Deserialize)]
pub struct IndependentTracertInstance {
    pub name: String,
    pub interval_ms: u64,
    pub max_hops: u32,
    pub queries_per_hop: u32,
}

// ============================================
// SYNCHRONIZED STRATEGY
// ============================================
#[derive(Debug, Deserialize, Default)]
pub struct SynchronizedStrategyConfig {
    pub interval_ms: u64,
    pub ping: Vec<SynchronizedPingInstance>,
    pub snmp: Vec<SynchronizedSnmpInstance>,
    pub tracert: Vec<SynchronizedTracertInstance>,
}

// ---------- PING ----------
#[derive(Debug, Deserialize)]
pub struct SynchronizedPingInstance {
    pub name: String,
    pub timeout_ms: u64,
    pub fallback_tracert: bool,
    pub retries: u8,
    pub retries_delay_ms: u64,
}

// ---------- SNMP ----------
#[derive(Debug, Deserialize)]
pub struct SynchronizedSnmpInstance {
    pub name: String,
    pub timeout_ms: u64,
    pub port: u16,
    pub community: String,
    pub retries: u8,
    pub retries_delay_ms: u64,
    pub oids: Vec<String>,
}

// ---------- TRACERT ----------
#[derive(Debug, Deserialize)]
pub struct SynchronizedTracertInstance {
    pub name: String,
    pub max_hops: u32,
    pub queries_per_hop: u32,
}

// ============================================
// HELPERS
// ============================================
fn user_friendly_error(e: &toml::de::Error) -> String {
    let msg = e.message();

    if msg.contains("missing field") {
        if let Some(field) = msg.split('`').nth(1) {
            return format!("отсутствует поле '{}'", field);
        }
        return "отсутствует обязательное поле".to_string();
    }

    if msg.contains("unknown field") {
        if let Some(field) = msg.split('`').nth(1) {
            return format!(
                "неизвестное поле '{}' — проверьте название",
                field
            );
        }
    }

    if msg.contains("invalid type") {
        return "неверный тип значения (например, строка вместо числа)"
            .to_string();
    }

    msg.to_string()
}
