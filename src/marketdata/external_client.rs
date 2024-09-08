//! External/plugin marketdata client

use crate::{
    external_driver::ExternalDriver, marketdata::book_client::LevelBook,
    symbology::MarketRef,
};
use anyhow::Result;
use api::{external::marketdata::*, marketdata::TradeV1};
use futures::Stream;
use std::pin::Pin;
use url::Url;

#[derive(Debug)]
pub struct ExternalMarketdataClient {
    driver: ExternalDriver,
    market: MarketRef,
    book: LevelBook,
}

impl ExternalMarketdataClient {
    pub async fn new(url: Url, market: MarketRef) -> Result<Self> {
        let driver = ExternalDriver::connect(url).await?;
        Ok(Self { driver, market, book: LevelBook::default() })
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
        let res: L2BookSnapshot = self
            .driver
            .query(
                "marketdata/book/l2/snapshot",
                Some(QueryL2BookSnapshot { market_id: self.market.id }),
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
