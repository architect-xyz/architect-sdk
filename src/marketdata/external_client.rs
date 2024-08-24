//! External/plugin marketdata client

use crate::{
    external_driver::ExternalDriver, marketdata::book_client::LevelBook,
    symbology::MarketRef,
};
use anyhow::Result;
use api::external::marketdata::*;
use url::Url;

#[derive(Debug)]
pub struct ExternalBookClient {
    driver: ExternalDriver,
    market: MarketRef,
    book: LevelBook,
}

impl ExternalBookClient {
    pub async fn new(url: Url, market: MarketRef) -> Result<Self> {
        let driver = ExternalDriver::connect(url).await?;
        Ok(Self { driver, market, book: LevelBook::default() })
    }

    pub fn book(&self) -> &LevelBook {
        &self.book
    }

    pub async fn poll_snapshot(&mut self) -> Result<()> {
        self.driver
            .send_query(
                "marketdata/book/snapshot",
                Some(QueryBookSnapshot { market_id: self.market.id }),
            )
            .await?;
        let res: BookSnapshot = self.driver.next_response().await?;
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
