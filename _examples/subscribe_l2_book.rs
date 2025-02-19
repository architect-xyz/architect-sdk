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
        default_value = "https://coinbase.marketdata.architect.co"
    )]
    endpoint: String,
    market: MarketId,
    /// Just request a snapshot and return
    #[arg(long, default_value_t = false)]
    snapshot: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let Cli { endpoint, market, snapshot } = Cli::parse();
    let client = ArchitectClient::default();
    println!("Resolving service {endpoint}...");
    let endpoint = client.resolve_service(endpoint.as_str()).await?;
    if snapshot {
        let snapshot = client.l2_book_snapshot_from(&endpoint, market).await?;
        println!("{snapshot:?}");
        return Ok(());
    }
    println!("Connecting to endpoint: {}", endpoint.uri());
    let mut l2_client = L2Client::connect(endpoint, market).await?;
    println!("Subscribed");
    let mut print_interval = tokio::time::interval(Duration::from_secs(1));
    let mut exit_after = Box::pin(tokio::time::sleep(Duration::from_secs(5)));
    loop {
        select! {
            _ = &mut exit_after => break,
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
    drop(l2_client);
    // demonstrate that dropping the client closes the connection
    tokio::time::sleep(Duration::from_secs(5)).await;
    Ok(())
}
