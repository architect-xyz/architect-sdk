//! Example demonstrating different approaches to working with orderflow in Architect.
//!

use anyhow::Result;
use architect_api::{
    orderflow::{Orderflow, OrderflowRequest},
    oms::PlaceOrderRequest,
    Dir,
};
use architect_sdk::Architect;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file if present
    dotenvy::dotenv().ok();

    // Get API credentials from environment
    let api_key = env::var("ARCHITECT_API_KEY")
        .expect("ARCHITECT_API_KEY environment variable must be set");
    let api_secret = env::var("ARCHITECT_API_SECRET")
        .expect("ARCHITECT_API_SECRET environment variable must be set");
    let paper_trading = env::var("ARCHITECT_PAPER_TRADING")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(true);

    println!("🚀 Connecting to Architect...");
    let client = Architect::connect(&api_key, &api_secret, paper_trading).await?;
    
    if paper_trading {
        println!("📝 Connected in PAPER TRADING mode");
    } else {
        println!("💰 Connected in LIVE TRADING mode");
    }

    // Get accounts
    let accounts = client.list_accounts(None).await?;
    println!("\n📊 Available accounts:");
    for account in &accounts {
        println!("  - {} ({})", account.account, account.permissions);
    }

    println!("\n=== Method 1: Using individual client methods (RECOMMENDED) ===\n");
    
    // This is the recommended approach for most use cases
    if let Some(account) = accounts.first() {
        // Place an order
        let order = PlaceOrderRequest::builder()
            .symbol("ES 20250620 CME Future/USD")
            .dir(Dir::Buy)
            .quantity("1".parse()?)
            .limit_price("1".parse()?) // Very low price, likely to be rejected
            .account(account.account.clone())
            .build()?;
        
        println!("📤 Placing order using client.place_order()...");
        match client.place_order(order).await {
            Ok(order) => {
                println!("✅ Order placed: {:?}", order);
                
                // Check order status
                println!("\n📋 Checking open orders...");
                let open_orders = client.get_open_orders(
                    Some(&[order.id]),
                    None::<&str>,
                    None,
                    None,
                    None::<&str>,
                    None,
                ).await?;
                
                for order in open_orders {
                    println!("  Order {}: {:?}", order.id, order.status);
                }
            }
            Err(e) => {
                println!("❌ Failed to place order: {}", e);
            }
        }
        
        // Check fills
        println!("\n📊 Checking recent fills...");
        let fills = client.get_fills(Default::default()).await?;
        for fill in fills.iter().take(5) {
            println!("  Fill: {} @ {} ({})", fill.quantity, fill.price, fill.symbol);
        }
    }

    println!("\n=== Method 2: Using bidirectional orderflow stream (ADVANCED) ===\n");
    
    // This is for advanced use cases where you need a persistent bidirectional connection
    use tokio_stream::StreamExt;
    
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    
    println!("🔌 Creating bidirectional orderflow stream...");
    let mut response_stream = client.orderflow(request_stream).await?;
    
    // Spawn a task to handle responses
    let tx_clone = tx.clone();
    let response_task = tokio::spawn(async move {
        while let Some(result) = response_stream.next().await {
            match result {
                Ok(update) => {
                    handle_orderflow_update(update);
                }
                Err(e) => {
                    eprintln!("❌ Stream error: {}", e);
                    break;
                }
            }
        }
        println!("📭 Response stream ended");
    });
    
    // Example: Send a request through the stream
    if let Some(account) = accounts.first() {
        let order = PlaceOrderRequest::builder()
            .symbol("ES 20250620 CME Future/USD")
            .dir(Dir::Sell)
            .quantity("1".parse()?)
            .limit_price("10000".parse()?) // Very high price, likely to be rejected
            .account(account.account.clone())
            .build()?;
        
        println!("📤 Sending order through bidirectional stream...");
        tx_clone.send(OrderflowRequest::PlaceOrder(order)).await?;
    }
    
    // Wait for responses
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Clean up
    drop(tx);
    let _ = tokio::time::timeout(
        tokio::time::Duration::from_secs(1),
        response_task
    ).await;
    
    println!("\n🏁 Example completed");
    Ok(())
}

/// Handle different types of orderflow updates
fn handle_orderflow_update(update: Orderflow) {
    match update {
        Orderflow::Order(order) => {
            println!("📋 Order Update: {} - {:?}", order.id, order.status);
        }
        
        Orderflow::Fill(fill) => {
            println!("✅ Fill: {} @ {} ({})", fill.quantity, fill.price, fill.symbol);
        }
        
        Orderflow::OrderAck(ack) => {
            println!("👍 Order Acknowledged: {}", ack.order_id);
        }
        
        Orderflow::OrderReject(reject) => {
            println!("❌ Order Rejected: {} - {:?}", reject.order_id, reject.reason);
        }
        
        Orderflow::Cancel(cancel) => {
            println!("🚫 Cancel: {} - {:?}", cancel.order_id, cancel.status);
        }
        
        Orderflow::CancelAck(ack) => {
            println!("👍 Cancel Acknowledged: {:?}", ack.cancel_id);
        }
        
        Orderflow::CancelReject(reject) => {
            println!("❌ Cancel Rejected: {:?} - {:?}", reject.cancel_id, reject.reason);
        }
        
        Orderflow::OrderOut(out) => {
            println!("📤 Order Out: {} - {:?}", out.order_id, out.reason);
        }
        
        _ => {
            println!("📨 Other Update: {:?}", update);
        }
    }
}