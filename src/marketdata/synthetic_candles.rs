use architect_api::marketdata::{Candle, CandleWidth};

/// Synthesize 2-minute candles from 1-minute candles
pub fn synthesize_2m_candles(one_min_candles: Vec<Candle>) -> Vec<Candle> {
    let mut two_min_candles = Vec::new();
    let mut iter = one_min_candles.chunks(2);

    while let Some(chunk) = iter.next() {
        if chunk.len() == 2 {
            let c1 = &chunk[0];
            let c2 = &chunk[1];

            let composed = Candle {
                symbol: c1.symbol.clone(),
                timestamp: c1.timestamp,
                timestamp_ns: c1.timestamp_ns,
                width: CandleWidth::TwoMinute,
                open: c1.open,
                high: c1.high.max(c2.high),
                low: c1.low.min(c2.low),
                close: c2.close,
                volume: c1.volume + c2.volume,
                buy_volume: c1.buy_volume + c2.buy_volume,
                sell_volume: c1.sell_volume + c2.sell_volume,
                mid_open: c1.mid_open,
                mid_close: c2.mid_close,
                mid_high: c1.mid_high.max(c2.mid_high),
                mid_low: c1.mid_low.min(c2.mid_low),
                bid_open: c1.bid_open,
                bid_close: c2.bid_close,
                bid_high: c1.bid_high.max(c2.bid_high),
                bid_low: c1.bid_low.min(c2.bid_low),
                ask_open: c1.ask_open,
                ask_close: c2.ask_close,
                ask_high: c1.ask_high.max(c2.ask_high),
                ask_low: c1.ask_low.min(c2.ask_low),
            };

            two_min_candles.push(composed);
        }
    }

    two_min_candles
}

/// Synthesize 3-minute candles from 1-minute candles
pub fn synthesize_3m_candles(one_min_candles: Vec<Candle>) -> Vec<Candle> {
    let mut three_min_candles = Vec::new();
    let mut iter = one_min_candles.chunks(3);

    while let Some(chunk) = iter.next() {
        if chunk.len() == 3 {
            let c1 = &chunk[0];
            let c2 = &chunk[1];
            let c3 = &chunk[2];

            let composed = Candle {
                symbol: c1.symbol.clone(),
                timestamp: c1.timestamp,
                timestamp_ns: c1.timestamp_ns,
                width: CandleWidth::ThreeMinute,
                open: c1.open,
                high: c1.high.max(c2.high).max(c3.high),
                low: c1.low.min(c2.low).min(c3.low),
                close: c3.close,
                volume: c1.volume + c2.volume + c3.volume,
                buy_volume: c1.buy_volume + c2.buy_volume + c3.buy_volume,
                sell_volume: c1.sell_volume + c2.sell_volume + c3.sell_volume,
                mid_open: c1.mid_open,
                mid_close: c3.mid_close,
                mid_high: c1.mid_high.max(c2.mid_high).max(c3.mid_high),
                mid_low: c1.mid_low.min(c2.mid_low).min(c3.mid_low),
                bid_open: c1.bid_open,
                bid_close: c3.bid_close,
                bid_high: c1.bid_high.max(c2.bid_high).max(c3.bid_high),
                bid_low: c1.bid_low.min(c2.bid_low).min(c3.bid_low),
                ask_open: c1.ask_open,
                ask_close: c3.ask_close,
                ask_high: c1.ask_high.max(c2.ask_high).max(c3.ask_high),
                ask_low: c1.ask_low.min(c2.ask_low).min(c3.ask_low),
            };

            three_min_candles.push(composed);
        }
    }

    three_min_candles
}
