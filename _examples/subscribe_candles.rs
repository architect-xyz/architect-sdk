use anyhow::Result;
use api::{marketdata::CandleWidth, symbology::MarketId};
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
    #[arg(value_delimiter = ',', short, long)]
    candle_widths: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    subscribe_to_one_stream(cli.endpoints, &cli.markets[0], &cli.candle_widths[0])
        .await?;
    Ok(())
}

/// Demonstrate subscribing to a single marketdata stream
async fn subscribe_to_one_stream(
    endpoints: Option<Vec<String>>,
    market_id: &str,
    candle_width: &str,
) -> Result<()> {
    let client = ArchitectClient::default();
    let endpoint = if let Some(endpoints) = endpoints {
        let endpoint = &endpoints[0];
        println!("Resolving service {endpoint}...");
        client.resolve_service(endpoint.as_str()).await?
    } else {
        unimplemented!()
    };
    println!("Connecting to endpoint: {}", endpoint.uri());
    println!("Loading symbology...");
    client.load_symbology_from(&endpoint).await?;
    let market_id = MarketId::from_str(market_id)?;
    let market_ref = MarketRef::find_by_id(&market_id)?;
    let candle_width = CandleWidth::from_str(candle_width)?;
    println!("Subscribing to {} {} candles...", market_ref.name, candle_width.as_str());
    let mut stream = client
        .subscribe_candles_from(&endpoint, market_ref.id, Some(vec![candle_width]))
        .await?;
    println!("Streaming candles...");
    while let Some(res) = stream.next().await {
        println!("candle {:?}", res?);
    }
    Ok(())
}
