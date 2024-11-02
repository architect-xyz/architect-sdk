use anyhow::Result;
use api::{symbology::MarketId, Dir};
use architect_sdk::marketdata::{
    l2_client::L2ClientHandle, managed_l2_clients::ManagedL2Clients,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let mut manager = ManagedL2Clients::new();
    manager.set_cooldown_before_unsubscribe(Duration::from_secs(2));
    let btc_usd: MarketId = "BTC Crypto/USD*COINBASE/DIRECT".parse()?;
    let eth_usd: MarketId = "ETH Crypto/USD*COINBASE/DIRECT".parse()?;
    let sol_usd: MarketId = "SOL Crypto/USD*COINBASE/DIRECT".parse()?;
    // we'll drive our manager inside a tokio task, although you could
    // also drive it from a select! or any other async primitive
    //
    // we retain a handle to the manager for use outside
    let (mut manager_handle, manager_task) = manager.run_in_background();
    println!("Starting in 1s...");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let btc_usd_handle = manager_handle.subscribe(btc_usd).await?;
    let eth_usd_handle = manager_handle.subscribe(eth_usd).await?;
    let mut sol_usd_handle: Option<L2ClientHandle> = None;
    for i in 1..=15 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!();
        println!("Iteration {i}");
        if btc_usd_handle.is_ready() {
            // hold the mutex for the minimum duration needed
            let book = btc_usd_handle.book().expect("manager died?");
            let book_ts = book.timestamp;
            let best_bid = book.best(Dir::Buy);
            let best_ask = book.best(Dir::Sell);
            println!("BTC/USD {book_ts}: bid={best_bid:?} ask={best_ask:?}");
        }
        if let Some(book) = eth_usd_handle.book() {
            // this will stop printing after the 7th iteration, because we
            // unsubscribed and therefore the handle is no longer alive
            let book_ts = book.timestamp;
            let best_bid = book.best(Dir::Buy);
            let best_ask = book.best(Dir::Sell);
            println!("ETH/USD {book_ts}: bid={best_bid:?} ask={best_ask:?}");
        }
        // this won't be available until the 4th iteration...
        if let Some(ref sol_usd_handle) = sol_usd_handle {
            if sol_usd_handle.is_ready() {
                // ...but conditional on being available we expect it to be alive
                let book = sol_usd_handle.book().expect("manager died?");
                let book_ts = book.timestamp;
                let best_bid = book.best(Dir::Buy);
                let best_ask = book.best(Dir::Sell);
                println!("SOL/USD {book_ts}: bid={best_bid:?} ask={best_ask:?}");
            }
        }
        if i == 3 {
            // start a sol subscription
            sol_usd_handle = Some(manager_handle.subscribe(sol_usd).await?);
        }
        if i == 7 {
            // stop the eth subscription
            manager_handle.unsubscribe(eth_usd)?;
        }
    }
    // dropping all handles referencing a subscription will unsubscribe
    drop(btc_usd_handle);
    // ...but the cooldown period we set was 2s
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(manager_handle.get(btc_usd).is_some());
    {
        let handle = manager_handle.get(btc_usd).unwrap();
        assert!(handle.is_alive());
        assert!(handle.is_ready());
        let book = handle.book().unwrap();
        let book_ts = book.timestamp;
        let best_bid = book.best(Dir::Buy);
        let best_ask = book.best(Dir::Sell);
        println!();
        println!("Penultimate iteration");
        println!("BTC/USD {book_ts}: bid={best_bid:?} ask={best_ask:?}");
        // handle drops here
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(manager_handle.get(btc_usd).is_none());
    assert!(sol_usd_handle.as_ref().is_some_and(|h| h.is_ready()));
    assert!(sol_usd_handle.as_ref().is_some_and(|h| h.is_alive()));
    {
        let book = sol_usd_handle.as_ref().unwrap().book().unwrap();
        let book_ts = book.timestamp;
        let best_bid = book.best(Dir::Buy);
        let best_ask = book.best(Dir::Sell);
        println!();
        println!("Final iteration");
        println!("SOL/USD {book_ts}: bid={best_bid:?} ask={best_ask:?}");
    }
    // this only returns when the manager itself is dropped or errors
    manager_task.await??;
    Ok(())
}
