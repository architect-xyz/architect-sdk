use anyhow::Result;
use api::symbology::MarketId;
use architect_sdk::{
    client::ArchitectClientConfig,
    symbology::{MarketRef, StaticRef},
    ArchitectClient,
};
use clap::Parser;
use futures::StreamExt;
use url::Url;

#[derive(Parser)]
struct Cli {
    #[arg(help = "e.g. coinbase.marketdata.architect.co")]
    service: String,
    /// Do not present any authentication to the service.
    #[arg(long, default_value_t = false)]
    no_auth: bool,
    #[arg(long)]
    market: Option<MarketId>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let mut config = ArchitectClientConfig::default();
    if !cli.no_auth {
        let installation: Url = inquire::Text::new("Architect:")
            .with_default("https://app.architect.co")
            .prompt()?
            .parse()?;
        config.hostname = Some(installation.host_str().unwrap().to_string());
        config.port = installation.port();
        config.no_tls = installation.scheme() != "https";
        let api_key = inquire::Text::new("API key:").prompt()?;
        let api_secret =
            inquire::Password::new("API secret:").without_confirmation().prompt()?;
        config.api_key = Some(api_key);
        config.api_secret = Some(api_secret);
    }
    let client = ArchitectClient::new(config)?;
    if !cli.no_auth {
        println!("Retrieving JWT...");
        client.refresh_jwt().await?;
    }
    subscribe_to_one_stream(client, cli.service, cli.market).await?;
    Ok(())
}

/// Demonstrate subscribing to a single marketdata stream
async fn subscribe_to_one_stream(
    client: ArchitectClient,
    service: String,
    market_id: Option<MarketId>,
) -> Result<()> {
    println!("Resolving service {service}...");
    let endpoint = client.resolve_service(service.as_str()).await?;
    println!("Connecting to endpoint: {}", endpoint.uri());
    println!("Loading symbology...");
    client.load_symbology_from(&endpoint).await?;
    if let Some(market_id) = market_id {
        let market_ref = MarketRef::find_by_id(&market_id)?;
        println!("Subscribing to trades for {}...", market_ref.name);
    } else {
        println!("Subscribing to all trades...");
    }
    let mut stream = client.subscribe_trades_from(&endpoint, market_id).await?;
    while let Some(res) = stream.next().await {
        println!("trade {:?}", res?);
    }
    Ok(())
}
