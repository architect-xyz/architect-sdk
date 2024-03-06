use super::utils::apply_oneshot;
use crate::{symbology, Common};
use anyhow::Result;
use api::marketdata::{CandleV1, CandleWidth};
use chrono::{DateTime, Utc};
use log::debug;
use netidx::{
    chars::Chars,
    path::Path,
    resolver_client::{Glob, GlobSet},
    subscriber::Event,
};
use netidx_archive::recorder_client;

pub async fn get(
    common: &Common,
    market: symbology::Market,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    width: CandleWidth,
) -> Result<Vec<CandleV1>> {
    let live_base = common.paths.marketdata_ohlc_by_name(market, true);
    debug!(
        "requesting historical {} candles for {}: from {} to {} via {}",
        width.as_str(),
        market.name,
        start,
        end,
        live_base
    );
    let recorder_base = common.paths.marketdata_hist_ohlc(market.cpty());
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
    let filter =
        format!("{live_candles_base_path}/**/finalized/{}/candle_v1", width.as_str());
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
