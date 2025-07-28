use anyhow::Result;
use architect_api::symbology::Product;
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
async fn test_connect_with_invalid_credentials() -> Result<()> {
    let result = Architect::connect("invalid_api_key", "invalid_api_secret", false).await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn test_connect_paper_trading_mode() -> Result<()> {
    let (api_key, api_secret, _) = match get_test_credentials() {
        Ok(creds) => creds,
        Err(_) => {
            println!("⚠️ Skipping test: API credentials not available");
            return Ok(());
        }
    };

    let result = Architect::connect(&api_key, &api_secret, true).await;

    assert!(
        result.is_ok(),
        "Paper trading connection should succeed with real credentials"
    );
    let client = result?;

    let futures: Vec<Product> =
        client.get_futures_series("ES CME Futures", false).await?;

    for future in futures {
        println!("Future: {:?}", future);
    }

    Ok(())
}

#[tokio::test]
async fn test_connect_live_trading_mode() -> Result<()> {
    let (api_key, api_secret, _) = match get_test_credentials() {
        Ok(creds) => creds,
        Err(_) => {
            println!("⚠️ Skipping test: API credentials not available");
            return Ok(());
        }
    };

    let result = Architect::connect(&api_key, &api_secret, false).await;

    assert!(
        result.is_ok(),
        "Live trading connection should succeed with real credentials"
    );
    let client = result?;

    let futures: Vec<Product> =
        client.get_futures_series("ES CME Futures", false).await?;

    for future in futures {
        println!("Future: {:?}", future);
    }

    Ok(())
}
