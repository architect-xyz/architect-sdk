use crate::Common;
use anyhow::{bail, Result};
use api::{
    marketdata::snapshots::{MarketSnapshot, OptionsMarketSnapshot},
    symbology::{MarketId, ProductId},
};
use chrono::{DateTime, Utc};
use netidx::subscriber::{FromValue, Value};
use netidx_protocols::{call_rpc, rpc::client::Proc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub const USD_EQUIVALENTS: [&'static str; 8] = [
    "USD",
    "USDC Crypto",
    "USDT Crypto",
    "TUSD Crypto",
    "PYUSD Crypto",
    "BUSD Crypto",
    "USDP Crypto",
    "FDUSD Crypto",
];

pub const USD_QUOTE_CURRENCIES: [(&'static str, Decimal); 4] = [
    ("USD", dec!(1)),
    ("USDT Crypto", dec!(1)),
    ("USDC Crypto", dec!(1)),
    ("USDcent", dec!(100)),
];

pub const ROUTES: [&'static str; 2] = ["DIRECT", "DATABENTO"];

pub async fn get_market_snapshot(
    common: &Common,
    local: bool,
    market: MarketId,
    latest_at_or_before: DateTime<Utc>,
) -> Result<Option<MarketSnapshot>> {
    let proc = Proc::new(
        &common.subscriber,
        common.paths.marketdata_snapshots(local).append("get-market-snapshot"),
    )?;
    match call_rpc!(
        proc,
        market: market,
        latest_at_or_before: latest_at_or_before
    )
    .await?
    {
        Value::Null => Ok(None),
        Value::Error(e) => bail!("rpc error: {}", e),
        x => Ok(Some(MarketSnapshot::from_value(x)?)),
    }
}

pub async fn get_market_snapshots(
    common: &Common,
    local: bool,
    latest_at_or_before: DateTime<Utc>,
) -> Result<Vec<MarketSnapshot>> {
    let proc = Proc::new(
        &common.subscriber,
        common.paths.marketdata_snapshots(local).append("get-market-snapshots"),
    )?;
    match call_rpc!(proc, latest_at_or_before: latest_at_or_before).await? {
        Value::Array(xs) => {
            let mut res = Vec::with_capacity(xs.len());
            for x in xs.iter() {
                res.push(MarketSnapshot::from_value(x.clone())?);
            }
            Ok(res)
        }
        Value::Error(e) => bail!("rpc error: {}", e),
        other => bail!("unexpected rpc response: {:?}", other),
    }
}

pub async fn get_options_market_snapshots(
    common: &Common,
    local: bool,
    underlying: ProductId,
    latest_at_or_before: DateTime<Utc>,
) -> Result<Vec<OptionsMarketSnapshot>> {
    let proc = Proc::new(
        &common.subscriber,
        common.paths.marketdata_snapshots(local).append("get-options-market-snapshots"),
    )?;
    match call_rpc!(
        proc,
        underlying: underlying,
        latest_at_or_before: latest_at_or_before
    )
    .await?
    {
        Value::Array(xs) => {
            let mut res = Vec::with_capacity(xs.len());
            for x in xs.iter() {
                res.push(OptionsMarketSnapshot::from_value(x.clone())?);
            }
            Ok(res)
        }
        Value::Error(e) => bail!("rpc error: {}", e),
        other => bail!("unexpected rpc response: {:?}", other),
    }
}
