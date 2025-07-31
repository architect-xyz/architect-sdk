mod common;

use anyhow::{bail, Result};
use architect_api::{
    oms::PlaceOrderRequest,
    orderflow::{
        order_types::{LimitOrderType, OrderType},
        Orderflow, OrderflowRequest, TimeInForce,
    },
    AccountIdOrName, Dir, OrderId,
};
use architect_sdk::Architect;
use common::get_test_credentials;
use std::sync::{Arc, Mutex};
use tokio_stream::StreamExt;

#[tokio::test]
async fn test_orderflow_bidirectional() -> Result<()> {
    let (api_key, api_secret, paper_trading) = match get_test_credentials() {
        Ok(creds) => creds,
        Err(_) => {
            println!("⚠️ Skipping test: API credentials not available");
            return Ok(());
        }
    };

    let client = Architect::connect(&api_key, &api_secret, paper_trading).await?;

    // Get accounts to use for placing orders
    let accounts = client.list_accounts(None).await?;
    if accounts.is_empty() {
        bail!("❌ No accounts available for placing orders");
    }
    let account = &accounts[0].account;

    // Get the front ES future
    let es_futures = client.get_futures_series("ES CME Futures", false).await?;
    if es_futures.is_empty() {
        bail!("❌ No ES futures available");
    }
    let front_es = &es_futures[0];
    let symbol = format!("{}/USD", front_es);
    println!("📈 Using symbol: {}", symbol);

    // Create channels for bidirectional communication
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    // Create orderflow stream
    let mut response_stream = client.orderflow(request_stream).await?;

    println!("✅ Successfully created bidirectional orderflow stream");

    // Track received acknowledgments
    let acks_received = Arc::new(Mutex::new(Vec::new()));
    let acks_clone = acks_received.clone();
    let rejects_received = Arc::new(Mutex::new(Vec::new()));
    let rejects_clone = rejects_received.clone();
    let fills_received = Arc::new(Mutex::new(Vec::new()));
    let fills_clone = fills_received.clone();

    // Spawn a task to handle responses
    let handle = tokio::spawn(async move {
        while let Some(result) = response_stream.next().await {
            match result {
                Ok(update) => {
                    println!("📦 Received orderflow update: {:?}", update);
                    match &update {
                        Orderflow::OrderAck(ack) => {
                            acks_clone.lock().unwrap().push(ack.order_id);
                        }
                        Orderflow::OrderReject(reject) => {
                            rejects_clone.lock().unwrap().push(reject.order_id);
                        }
                        Orderflow::Fill(fill) => {
                            if let Some(order_id) = fill.order_id {
                                fills_clone.lock().unwrap().push(order_id);
                            }
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    println!("❌ Stream error: {}", e);
                    break;
                }
            }
        }
        println!("📭 Response stream ended");
    });

    // Send 5 orders with 20ms delay between each
    let mut order_ids = Vec::new();
    for i in 0..5 {
        let order_id = OrderId::random();
        order_ids.push(order_id);

        let order = PlaceOrderRequest {
            id: Some(order_id),
            parent_id: None,
            symbol: symbol.clone(),
            dir: if i % 2 == 0 { Dir::Buy } else { Dir::Sell },
            quantity: "1".parse()?,
            trader: None,
            account: Some(AccountIdOrName::Id(account.id)),
            order_type: OrderType::Limit(LimitOrderType {
                limit_price: format!("{}", 1000 + i * 100).parse()?,
                post_only: false,
            }),
            time_in_force: TimeInForce::GoodTilCancel,
            source: None,
            execution_venue: None,
        };

        println!("📤 Sending order {} with ID: {}", i + 1, order_id);
        tx.send(OrderflowRequest::PlaceOrder(order)).await?;

        // Wait 20ms between orders
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }

    // Wait for responses
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Clean up
    drop(tx);
    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(1), handle).await;

    // Check results
    let acks = acks_received.lock().unwrap();
    let rejects = rejects_received.lock().unwrap();
    let fills = fills_received.lock().unwrap();

    println!("\n📊 Results:");
    println!("  Orders sent: {}", order_ids.len());
    println!("  Acks received: {}", acks.len());
    println!("  Rejects received: {}", rejects.len());
    println!("  Fills received: {}", fills.len());
    println!("  Total responses: {}", acks.len() + rejects.len() + fills.len());

    // Verify we received responses for all orders (either ack, reject, or immediate fill)
    let total_responses = acks.len() + rejects.len() + fills.len();
    assert!(
        total_responses >= order_ids.len(),
        "Should receive at least one response for each order sent. Got {} responses for {} orders",
        total_responses,
        order_ids.len()
    );

    Ok(())
}
