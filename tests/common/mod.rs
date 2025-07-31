use anyhow::Result;
use std::env;

pub fn get_test_credentials() -> Result<(String, String, bool)> {
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
