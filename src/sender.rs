use crate::models::{TestEvent, TestType};
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

#[async_trait]
pub trait EventSender {
    async fn send(&mut self, event: TestEvent) -> Result<(), String>;
}

pub struct CsvSender {
    file: tokio::fs::File,
}

impl CsvSender {
    pub async fn new(path: &str) -> Result<Self, String> {
        // Проверяем, существует ли файл и пустой ли он
        let is_empty = match tokio::fs::metadata(path).await {
            Ok(meta) => meta.len() == 0,
            Err(_) => true,
        };

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| format!("Cannot open {}: {}", path, e))?;

        let mut sender = Self { file };

        if is_empty {
            sender
                .write_line(
                    "date,target,start,end,type,result,latency_ms,details",
                )
                .await?;
        }

        Ok(sender)
    }

    pub async fn write_line(&mut self, line: &str) -> Result<(), String> {
        self.file
            .write_all(line.as_bytes())
            .await
            .map_err(|e| format!("Write error: {}", e))?;
        self.file
            .write_all(b"\n")
            .await
            .map_err(|e| format!("Write error: {}", e))?;
        self.file
            .flush()
            .await
            .map_err(|e| format!("Flush error: {}", e))?;
        Ok(())
    }
}

#[async_trait]
impl EventSender for CsvSender {
    async fn send(&mut self, event: TestEvent) -> Result<(), String> {
        let details = event.details.as_deref().unwrap_or("");
        let escaped_details = details.replace('"', "\"\"");

        let line = format!(
            "{},{},{},{},{},{},{:.2},\"{}\"",
            event.date,
            event.target,
            event.start,
            event.end,
            event.test_type,
            if event.success { "OK" } else { "LOSS" },
            event.latency_ms,
            escaped_details,
        );

        self.write_line(&line).await
    }
}

pub struct TxtSender {
    file: tokio::fs::File,
}

impl TxtSender {
    pub async fn new(path: &str) -> Result<Self, String> {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .map_err(|e| format!("Cannot open {}: {}", e, path))?;

        Ok(Self { file })
    }

    pub async fn write_line(&mut self, line: &str) -> Result<(), String> {
        self.file
            .write_all(line.as_bytes())
            .await
            .map_err(|e| format!("Write error: {}", e))?;
        self.file
            .write_all(b"\n")
            .await
            .map_err(|e| format!("Write error: {}", e))?;
        self.file
            .flush()
            .await
            .map_err(|e| format!("Flush error: {}", e))?;
        Ok(())
    }
}

#[async_trait]
impl EventSender for TxtSender {
    async fn send(&mut self, event: TestEvent) -> Result<(), String> {
        let details = event.details.as_deref().unwrap_or("");

        let mut line = format!(
            "[{}] {} {} → {} {:<6} {} ({:.2} ms)",
            event.date,
            event.target,
            event.start,
            event.end,
            event.test_type,
            if event.success { "OK" } else { "LOSS" },
            event.latency_ms,
        );

        if let Some(details) = event.details.as_deref() {
            line.push(' ');
            line.push_str(details);

            // if !details.is_empty() {
            //     if event.test_type == TestType::Trace {
            //         // Trace: на новой строке, сохраняем форматирование
            //         line.push_str(&format!("\n{}", details));
            //     } else {
            //         let clean = details
            //             .replace('\r', "")
            //             .replace('\n', " ")
            //             .split_whitespace()
            //             .collect::<Vec<_>>()
            //             .join(" ");
            //         if !clean.is_empty() {
            //             line.push(' ');
            //             line.push_str(&clean);
            //         }
            //     }
            // }
        }

        self.write_line(&line).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Local;
    use std::fs;

    #[tokio::test]
    async fn test_txt_sender_real_file() {
        let log_path = "test_output.log";
        let mut sender = TxtSender::new(log_path).await.unwrap();

        let now = Local::now();
        let date = now.format("%Y-%m-%d").to_string();
        let time = now.format("%H:%M:%S%.3f").to_string();

        let events = vec![
            TestEvent {
                date: date.clone(),
                target: "8.8.8.8".to_string(),
                start: time.clone(),
                end: time.clone(),
                test_type: TestType::Ping,
                success: true,
                latency_ms: 42.5,
                details: Some("Pinging 8.8.8.8... Reply from 8.8.8.8: time=42ms".to_string()),
            },
            TestEvent {
                date: date.clone(),
                target: "192.168.1.200".to_string(),
                start: time.clone(),
                end: time.clone(),
                test_type: TestType::Ping,
                success: false,
                latency_ms: 2000.0,
                details: Some("Request timed out.".to_string()),
            },
            TestEvent {
                date: date,
                target: "8.8.8.8".to_string(),
                start: time.clone(),
                end: time,
                test_type: TestType::Trace,
                success: false,
                latency_ms: 0.0,
                details: Some(
                    "  1   1 ms   192.168.1.1\n  2    *      Request timed out.".to_string(),
                ),
            },
        ];

        for event in events {
            sender.send(event).await.unwrap();
        }

        let log_content = fs::read_to_string(log_path).unwrap();
        println!("\n=== test_output.log ===\n{}", log_content);
    }
}
