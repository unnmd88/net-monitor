use async_trait::async_trait;

use crate::models::TestEvent;

#[async_trait]
pub trait Pollable: Send + Sync {
    async fn fetch(&self) -> TestEvent;
}
