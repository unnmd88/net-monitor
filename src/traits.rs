use std::net::IpAddr;

use async_trait::async_trait;

use crate::models::{Event, PollType, ProviderConfig};

#[async_trait]
pub trait Pollable: Send + Sync {
    async fn fetch(&self) -> Result<String, String>;
    fn username(&self) -> String;
    fn target(&self) -> IpAddr;
    fn whoami(&self) -> PollType;
    fn dump(&self) -> ProviderConfig;
}
