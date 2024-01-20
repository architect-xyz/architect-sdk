use crate::{symbology::{self, static_ref::StaticRef, Cpty, Market}, Common};
use super::utils::{
    apply_oneshot, legacy_hist_ohlc_path_by_name, legacy_ohlc_path_by_name,
};
use anyhow::{anyhow, Result};
use api::{
    marketdata::{CandleV1, CandleWidth, NetidxFeedPaths}, symbology::MarketId,
};
use chrono::{DateTime, Utc};
use netidx::{
    chars::Chars,
    path::Path,
    resolver_client::{Glob, GlobSet},
    subscriber::Event,
};
use netidx_archive::recorder_client;

pub fn live_candles_base_path(common: &Common, market: Market) -> Path {
    let cpty = Cpty { route: market.route, venue: market.venue };
    if common.config.use_legacy_marketdata_paths {
        let base_path = common.paths.marketdata(cpty);
        legacy_ohlc_path_by_name(base_path, market)
    } else {
        let base_path = common.paths.marketdata_ohlc(cpty);
        market.path_by_name(&base_path)
    }
}

pub fn recorder_base_path(common: &Common, market: Market) -> Path {
    let cpty = Cpty { route: market.route, venue: market.venue };
    if common.config.use_legacy_marketdata_paths {
        let base_path = common.paths.marketdata(cpty);
        legacy_hist_ohlc_path_by_name(base_path, market)
    } else {
        let base_path = common.paths.marketdata_hist_ohlc(cpty);
        market.path_by_name(&base_path)
    }
}

pub async fn get(
    common: &Common,
    id: MarketId,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    width: CandleWidth,
) -> Result<Vec<CandleV1>> {
    let market =
        symbology::Market::get_by_id(&id).ok_or_else(|| anyhow!("unknown market"))?;
    let live_base = live_candles_base_path(common, market.clone());
    let recorder_base = recorder_base_path(common, market);
    let recorder_client =
        recorder_client::Client::new(&common.subscriber, &recorder_base)?;
    get_from_recorder(recorder_client, live_base, start, end, width).await
}

pub async fn get_from_recorder(
    recorder_client: recorder_client::Client,
    live_candles_base_path: Path,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    width: CandleWidth,
) -> Result<Vec<CandleV1>> {
    let mut candles: Vec<CandleV1> = vec![];
    // CR alee: ** is a bit of a hack to include .lagging if possible from the blockchain QFs
    // we should unwind this degeneracy immediately/asap
    let filter = format!("{live_candles_base_path}/**/finalized/{}/candle_v1", width.as_str());
    let filter = Glob::new(Chars::from(filter))?;
    let filter = GlobSet::new(true, std::iter::once(filter))?;
    apply_oneshot(recorder_client, Some(start), Some(end), &filter, |_, up| {
        if let Event::Update(v) = up {
            if let Ok(candle) = v.cast_to::<CandleV1>() {
                candles.push(candle);
            }
        }
    })
    .await?;
    Ok(candles)
}
