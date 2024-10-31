use anyhow::{anyhow, Result};
use api::symbology::MarketId;
use architect_sdk::{marketdata::l2_client::L2Client, ArchitectClient};
use clap::Parser;
use std::time::Duration;
use tokio::select;

#[derive(Parser)]
struct Cli {
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "coinbase.marketdata.architect.co"
    )]
    endpoint: String,
    market: MarketId,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let Cli { endpoint, market } = Cli::parse();
    let client = ArchitectClient::default();
    println!("Resolving service {endpoint}...");
    let endpoint = client.resolve_service(&endpoint).await?;
    println!("Connecting to endpoint: {endpoint}");
    let mut l2_client = L2Client::connect(endpoint, market).await?;
    println!("Subscribed");
    let mut print_interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        select! {
            _ = print_interval.tick() => {
                let book = l2_client.book();
                println!("{book:?}");
            }
            update = l2_client.next() => {
                let sequence = update.ok_or_else(|| anyhow!("l2 client disconnected"))?;
                println!("sequence: {sequence}");
            }
        }
    }
    #[allow(unreachable_code)]
    Ok(())
}
