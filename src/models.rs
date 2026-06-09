use std::fmt;

#[derive(Debug, Clone, PartialEq)]
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
pub struct TestEvent {
    pub date: String,
    pub target: String,
    pub start: String,
    pub end: String,
    pub test_type: TestType,
    pub success: bool,
    pub latency_ms: f64,
    pub details: Option<String>,
}
