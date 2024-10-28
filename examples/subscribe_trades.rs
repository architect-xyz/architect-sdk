use anyhow::Result;
use api::symbology::MarketId;
use architect_sdk::{
    symbology::{MarketRef, StaticRef},
    ArchitectClient,
};
use clap::Parser;
use futures::StreamExt;
use std::str::FromStr;

#[derive(Parser)]
struct Cli {
    #[arg(value_delimiter = ',', help = "Example: coinbase.marketdata.architect.co")]
    endpoints: Option<Vec<String>>,
    #[arg(value_delimiter = ',', short, long)]
    markets: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    subscribe_to_one_stream(cli.endpoints, &cli.markets[0]).await?;
    Ok(())
}

/// Demonstrate subscribing to a single marketdata stream
async fn subscribe_to_one_stream(
    endpoints: Option<Vec<String>>,
    market_id: &str,
) -> Result<()> {
    let mut client = ArchitectClient::default();
    let endpoint = if let Some(endpoints) = endpoints {
        let endpoint = &endpoints[0];
        println!("Resolving service {endpoint}...");
        client.resolve_service(endpoint).await?
    } else {
        "http://localhost:7777".to_owned()
    };
    println!("Connecting to endpoint: {endpoint}");
    println!("Loading symbology...");
    client.load_symbology_from(&endpoint).await?;
    println!("Subscribing to marketdata...");
    let market_id = MarketId::from_str(market_id)?;
    let market_ref = MarketRef::find_by_id(&market_id)?;
    println!("Subscribing to {} trades...", market_ref.name);
    let mut stream = client.subscribe_trades_from(&endpoint, Some(market_id)).await?;
    while let Some(res) = stream.next().await {
        println!("trade {:?}", res?);
    }
    Ok(())
}
