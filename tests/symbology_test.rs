use anyhow::Result;
use architect_sdk::Architect;
use std::env;

fn get_test_credentials() -> Result<(String, String, bool)> {
    dotenvy::dotenv().ok();

    let api_key = env::var("ARCHITECT_API_KEY")
        .map_err(|_| anyhow::anyhow!("ARCHITECT_API_KEY environment variable not set"))?;
    let api_secret = env::var("ARCHITECT_API_SECRET").map_err(|_| {
        anyhow::anyhow!("ARCHITECT_API_SECRET environment variable not set")
    })?;
    let paper_trading = env::var("ARCHITECT_PAPER_TRADING")
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(true);

    Ok((api_key, api_secret, paper_trading))
}

#[tokio::test]
async fn test_get_execution_info() -> Result<()> {
    let (api_key, api_secret, paper_trading) = match get_test_credentials() {
        Ok(creds) => creds,
        Err(_) => {
            println!("⚠️ Skipping test: API credentials not available");
            return Ok(());
        }
    };

    let client = Architect::connect(&api_key, &api_secret, paper_trading).await?;

    // Test with a common ES future
    let futures = client.get_futures_series("ES CME Futures", false).await?;
    let test_symbol = futures.first().expect("ES futures should exist");

    // Create a TradableProduct (with quote)
    let tradable_symbol = format!("{}/USD", test_symbol);

    let execution_info_response =
        client.get_execution_info(&tradable_symbol, Some("CME".into())).await?;

    // Check if we have execution info for CME venue
    assert!(
        !execution_info_response.execution_info.is_empty(),
        "Execution info should exist for ES futures"
    );
    let cme_venue: architect_api::symbology::ExecutionVenue = "CME".into();
    assert!(
        execution_info_response.execution_info.contains_key(&cme_venue),
        "Should have CME execution info"
    );

    let info = &execution_info_response.execution_info[&cme_venue];
    println!("Execution info for {}: {:?}", tradable_symbol, info);

    // Verify expected fields are present
    assert!(info.step_size > rust_decimal::Decimal::ZERO, "Step size should be positive");
    assert!(
        info.min_order_quantity > rust_decimal::Decimal::ZERO,
        "Min order quantity should be positive"
    );

    // Test with non-existent venue
    let execution_info_invalid =
        client.get_execution_info(&tradable_symbol, Some("INVALID_VENUE".into())).await?;
    assert!(
        execution_info_invalid.execution_info.is_empty(),
        "Execution info should not exist for invalid venue"
    );

    // Test that the method works with Product format (the validation was removed)
    let result = client.get_execution_info(test_symbol, Some("CME".into())).await;
    assert!(result.is_ok(), "Should work with Product format as well");

    Ok(())
}

#[tokio::test]
async fn test_get_execution_info_with_tradable_product() -> Result<()> {
    let (api_key, api_secret, paper_trading) = match get_test_credentials() {
        Ok(creds) => creds,
        Err(_) => {
            println!("⚠️ Skipping test: API credentials not available");
            return Ok(());
        }
    };

    let client = Architect::connect(&api_key, &api_secret, paper_trading).await?;

    // Test with a tradable product (symbol/quote)
    let futures = client.get_futures_series("ES CME Futures", false).await?;
    let test_symbol = futures.first().expect("ES futures should exist");

    // Create a tradable product string (with quote)
    let tradable_product = format!("{}/USD", test_symbol);

    let execution_info_response =
        client.get_execution_info(&tradable_product, Some("CME".into())).await?;

    // The method should handle tradable products (with quotes) correctly
    assert!(
        !execution_info_response.execution_info.is_empty(),
        "Execution info should exist for ES tradable product"
    );
    let cme_venue: architect_api::symbology::ExecutionVenue = "CME".into();
    assert!(
        execution_info_response.execution_info.contains_key(&cme_venue),
        "Should have CME execution info"
    );

    let info = &execution_info_response.execution_info[&cme_venue];
    println!("Execution info for tradable product {}: {:?}", tradable_product, info);

    Ok(())
}
