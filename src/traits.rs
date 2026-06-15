use async_trait::async_trait;
use std::any::type_name_of_val;

use crate::models::LogEvent;

#[async_trait]
pub trait Pollable: Send + Sync {
    async fn fetch(&self) -> LogEvent;

    fn get_provider_name(&self) -> String;
}
