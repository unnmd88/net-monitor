use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TestType {
    Ping,
    Snmp,
    Trace,
    Error,
}

impl fmt::Display for TestType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestType::Ping => write!(f, "PING"),
            TestType::Snmp => write!(f, "SNMP"),
            TestType::Trace => write!(f, "TRACE"),
            TestType::Error => write!(f, "ERROR"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TestEventOld {
    pub date: String,
    pub target: String,
    pub start: String,
    pub end: String,
    pub test_type: TestType,
    pub success: bool,
    pub latency_ms: f64,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub session_id: String,
    pub timestamp: String,
    #[serde(flatten)]
    pub event: LogEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogEvent {
    PollResult {
        target: String,
        start: String,
        end: String,
        test_type: TestType,
        success: bool,
        latency_ms: f64,
        details: Option<String>,
    },

    Config {
        poll_interval_secs: u64,
        targets: Vec<String>,
        test_types: Vec<TestType>,
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
