use std::{fmt, net::IpAddr};

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub enum Strategy {
    #[serde(rename = "independent")]
    Independent,
    #[serde(rename = "synchronized")]
    Synchronized,
}

impl fmt::Display for Strategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Strategy::Independent => "independent",
            Strategy::Synchronized => "synchronized",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PollType {
    Ping,
    Snmp,
}

impl fmt::Display for PollType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PollType::Ping => write!(f, "PING"),
            PollType::Snmp => write!(f, "SNMP"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "sid")]
    pub session_id: String,
    pub timestamp: String,
    #[serde(flatten)]
    pub event: Event,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResult {
    pub test_type: PollType,
    pub target: IpAddr,
    pub start: String,
    pub end: String,
    pub success: bool,
    pub attempts: u8,
    pub latency_ms: f64,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    PollResult {
        strategy: Strategy,
        step: usize,
        #[serde(flatten)]
        payload: FetchResult,
    },

    Config {
        strategy: Strategy,
        #[serde(flatten)]
        details: ConfigStrategyDetails,
    },

    Error {
        error_type: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConfigStrategyDetails {
    Independent {
        pollers: Vec<IndependentPollerConfig>,
    },
    Synchronized {
        interval_seconds: u64,
        providers: Vec<ProviderConfig>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndependentPollerConfig {
    #[serde(flatten)]
    pub provider: ProviderConfig,
    pub retries: u8,
    pub retries_interval_ms: u64,
    pub interval_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProviderConfig {
    pub name: PollType,
    pub target: IpAddr,
    pub timeout_ms: u64,
    pub extra: Option<serde_json::Value>,
}
