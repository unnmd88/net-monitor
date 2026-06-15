use std::{fmt, task::Poll};

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

#[derive(Debug, Clone)]
pub struct TestEventOld {
    pub date: String,
    pub target: String,
    pub start: String,
    pub end: String,
    pub test_type: PollType,
    pub success: bool,
    pub latency_ms: f64,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "sid")]
    pub session_id: String,
    pub timestamp: String,
    #[serde(flatten)]
    pub event: PollEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogEvent {
    PollResult {
        target: String,
        start: String,
        end: String,
        test_type: PollType,
        success: bool,
        latency_ms: f64,
        details: Option<String>,
    },

    Config {
        poll_interval_secs: u64,
        targets: Vec<String>,
        test_types: Vec<PollType>,
        version: String,
    },

    Status {
        state: State, // Started, Stopped, Paused
        message: Option<String>,
    },

    Error {
        error_type: String,
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Started,
    Stopped,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollEvent {
    pub step: usize,
    #[serde(flatten)]
    pub log_event: LogEvent,
    pub strategy: Strategy,
}
