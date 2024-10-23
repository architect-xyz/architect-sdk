//! External/plugin marketdata client

use crate::{
    external_driver::ExternalDriver, marketdata::book_client::LevelBook,
    symbology::MarketRef,
};
use anyhow::Result;
use api::{external::marketdata::*, marketdata::TradeV1};
use futures::Stream;
use std::{pin::Pin, sync::Arc};

#[derive(Debug)]
pub struct ExternalMarketdataClient {
    driver: Arc<ExternalDriver>,
    market: MarketRef,
    book: LevelBook,
}

impl ExternalMarketdataClient {
    pub fn new(driver: Arc<ExternalDriver>, market: MarketRef) -> Self {
        Self { driver, market, book: LevelBook::default() }
    }

    pub async fn subscribe_trades(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<TradeV1>>>>> {
        self.driver
            .subscribe::<TradeV1>(&format!("marketdata/trades/{}", self.market.id))
            .await
    }

    pub fn book(&self) -> &LevelBook {
        &self.book
    }

    pub async fn poll_snapshot(&mut self) -> Result<()> {
        let res: ExternalL2BookSnapshot = self
            .driver
            .query(
                "marketdata/book/l2/snapshot",
                Some(QueryExternalL2BookSnapshot { market_id: self.market.id }),
            )
            .await?;
        self.book.clear();
        self.book.timestamp = res.timestamp;
        for (px, sz) in res.bids {
            self.book.buy.insert(px, sz);
        }
        for (px, sz) in res.asks {
            self.book.sell.insert(px, sz);
        }
        Ok(())
    }
}
