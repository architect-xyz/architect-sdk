use anyhow::Result;
use architect_sdk::{symbology::*, ArchitectClient};
use clap::Parser;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Parser)]
struct Cli {
    #[arg(value_delimiter = ',', default_value = "coinbase.marketdata.architect.co")]
    endpoints: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    if cli.endpoints.is_empty() {
        println!("no endpoints specified");
    } else if cli.endpoints.len() == 1 {
        subscribe_to_one_stream(&cli.endpoints[0]).await?;
    } else {
        subscribe_to_multiple_streams(&cli.endpoints).await?;
    }
    Ok(())
}

/// Demonstrate subscribing to a single marketdata stream
async fn subscribe_to_one_stream(endpoint: &str) -> Result<()> {
    let client = ArchitectClient::default();
    println!("Resolving service {endpoint}...");
    let endpoint = client.resolve_service(endpoint).await?;
    println!("Connecting to endpoint: {}", endpoint.uri());
    println!("Loading symbology...");
    client.load_symbology_from(&endpoint).await?;
    println!("Subscribing to marketdata...");
    let mut stream = client.subscribe_l1_book_snapshots_from(&endpoint, None).await?;
    while let Some(res) = stream.next().await {
        let snap = res?;
        if let Some(market) = MarketRef::get_by_id(&snap.market_id) {
            println!("{} | {snap:?}", market.name);
        } else {
            println!("<unknown> | {snap:?}");
        };
    }
    Ok(())
}

/// Demonstrate subscribing to a variable number of streams using tokio tasks;
/// other ways to subscribe include using a FuturesUnordered. If the number of
/// streams is fixed and known at compile time, async select can be used.
async fn subscribe_to_multiple_streams<S: AsRef<str>>(
    endpoints: impl IntoIterator<Item = S>,
) -> Result<()> {
    let client = ArchitectClient::default();
    let mut resolved_endpoints = vec![];
    for endpoint in endpoints {
        println!("Resolving service {}...", endpoint.as_ref());
        let resolved = client.resolve_service(endpoint.as_ref()).await?;
        resolved_endpoints.push(resolved);
    }
    println!("Loading symbology...");
    client.load_symbology_from_all(&resolved_endpoints).await?;
    // share client with all tasks
    let client = Arc::new(Mutex::new(client));
    for endpoint in &resolved_endpoints {
        let client = client.clone();
        let endpoint = endpoint.clone();
        tokio::task::spawn(async move {
            let mut stream = {
                let client = client.lock().await;
                client.subscribe_l1_book_snapshots_from(&endpoint, None).await?
            };
            while let Some(res) = stream.next().await {
                let snap = res?;
                if let Some(market) = MarketRef::get_by_id(&snap.market_id) {
                    println!("{} | {snap:?}", market.name);
                } else {
                    println!("<unknown> | {snap:?}");
                };
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    std::future::pending::<()>().await; // wait forever don't exit
    Ok(())
}
