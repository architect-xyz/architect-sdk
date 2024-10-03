use anyhow::Result;
use architect_sdk::{symbology::*, ArchitectClient};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    const ENDPOINT: &str = "http://localhost:10000";
    let mut client = ArchitectClient::default();
    println!("Loading symbology...");
    client.load_symbology_from(ENDPOINT).await?;
    println!("Subscribing to marketdata...");
    let mut stream = client.subscribe_l1_book_snapshots_from(ENDPOINT, None).await?;
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
