use std::net::IpAddr;

use anyhow;
use net_monitor::{
    traceroute::TrippyTracertProvider, traits::TracerouteProvider,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let target = "1.1.1.1".parse::<IpAddr>()?;
    let tracert = TrippyTracertProvider::new(target, 20, 1)
        .map_err(|e| anyhow::anyhow!("Ошибка создания трассировщика: {}", e))?;

    let res = tracert.traceroute().await?;
    println!("ТРАСЕРТ:\n{res}");

    Ok(())
}
