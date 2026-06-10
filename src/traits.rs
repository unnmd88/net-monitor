use async_trait::async_trait;

use crate::models::LogEvent;

#[async_trait]
pub trait Pollable: Send + Sync {
    async fn fetch(&self) -> LogEvent;
}
