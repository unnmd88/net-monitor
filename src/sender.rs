use std::path::PathBuf;

use crate::models::{Envelope, LogEvent, TestType};
use async_fd_lock::LockWrite;
use async_trait::async_trait;
use tokio::{fs::OpenOptions, io::AsyncWriteExt};

#[async_trait]
pub trait EventSender {
    async fn send(&self, event: LogEvent) -> Result<(), String>;
}

pub struct JsonSender {
    path: PathBuf,
    session_id: String,
}

impl JsonSender {
    pub async fn new(path: &str, session_id: String) -> Result<Self, String> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| format!("Cannot open: {}", e))?;
        Ok(Self {
            path: PathBuf::from(path),
            session_id,
        })
    }
}

#[async_trait]
impl EventSender for JsonSender {
    async fn send(&self, event: LogEvent) -> Result<(), String> {
        let envelope = Envelope {
            session_id: self.session_id.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            event,
        };

        let line = serde_json::to_string(&envelope)
            .map_err(|e| format!("JSON error: {}", e))?;

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|e| format!("Cannot open: {}", e))?;

        tokio::io::AsyncWriteExt::write_all(
            &mut file,
            format!("{}\n", line).as_bytes(),
        )
        .await
        .map_err(|e| format!("Write error: {}", e))?;

        Ok(())
    }
}
