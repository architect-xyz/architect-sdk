//! General purpose client for Architect

#[cfg(feature = "grpc")]
use anyhow::{anyhow, Result};
#[cfg(feature = "grpc")]
use api::marketdata::CandleWidth;
#[cfg(feature = "grpc")]
use api::{
    external::{marketdata::*, symbology::*},
    grpc::json_service::{marketdata_client::*, symbology_client::*},
};
#[cfg(feature = "grpc")]
use hickory_resolver::{config::*, TokioAsyncResolver};
use std::net::SocketAddr;
#[cfg(feature = "grpc")]
use tonic::codec::Streaming;

#[derive(Default, Debug)]
pub struct ArchitectClient {}

impl ArchitectClient {
    #[cfg(feature = "grpc")]
    pub async fn resolve_service(&self, endpoint: &str) -> Result<String> {
        // CR alee: also check for localhost, and parse as Url first
        // CR alee: preserve http/https scheme if present, default to http 
        // if the endpoint is already an IP address, return it as is
        if let Ok(_) = endpoint.parse::<SocketAddr>() {
            return Ok(format!("dns://{endpoint}"));
        }
        let resolver =
            TokioAsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default());
        let records = resolver.srv_lookup(endpoint).await?;
        let rec = records
            .iter()
            .next()
            .ok_or_else(|| anyhow!("no SRV records found for domain: {endpoint}"))?;
        Ok(format!("dns://{}:{}", rec.target(), rec.port()))
    }

    /// Load symbology from the given endpoint into global memory.
    #[cfg(feature = "grpc")]
    pub async fn load_symbology_from(&self, endpoint: impl AsRef<str>) -> Result<()> {
        use crate::symbology::Txn;
        let mut client = SymbologyClient::connect(endpoint.as_ref().to_string()).await?;
        let snap =
            client.symbology_snapshot(SymbologySnapshotRequest {}).await?.into_inner();
        let mut txn = Txn::begin();
        for route in snap.routes {
            txn.add_route(route)?;
        }
        for venue in snap.venues {
            txn.add_venue(venue)?;
        }
        for product in snap.products {
            txn.add_product(product)?;
        }
        for market in snap.markets {
            txn.add_market(market)?;
        }
        txn.commit()?;
        Ok(())
    }

    #[cfg(feature = "grpc")]
    pub async fn load_symbology_from_all<S: AsRef<str>>(
        &self,
        endpoints: impl IntoIterator<Item = S>,
    ) -> Result<()> {
        for endpoint in endpoints.into_iter() {
            self.load_symbology_from(endpoint).await?;
        }
        Ok(())
    }

    // NB alee: keeping all the [subscribe_*] function take a mut self for now in case we mux clients
    #[cfg(feature = "grpc")]
    pub async fn subscribe_l1_book_snapshots_from(
        &mut self,
        endpoint: impl AsRef<str>,
        // if None, subscribe to all L1 books for all markets available
        market_ids: Option<Vec<MarketId>>,
    ) -> Result<Streaming<L1BookSnapshot>> {
        let mut client = MarketdataClient::connect(endpoint.as_ref().to_string()).await?;
        let stream = client
            .subscribe_l1_book_snapshots(SubscribeL1BookSnapshotsRequest { market_ids })
            .await?
            .into_inner();
        Ok(stream)
    }

    #[cfg(feature = "grpc")]
    pub async fn subscribe_candles_from(
        &mut self,
        endpoint: impl AsRef<str>,
        market_id: MarketId,
        // if None, subscribe for all widths available
        candle_width: Option<Vec<CandleWidth>>,
    ) -> Result<Streaming<Candle>> {
        let mut client = MarketdataClient::connect(endpoint.as_ref().to_string()).await?;
        let stream = client
            .subscribe_candles(SubscribeCandlesRequest { market_id, candle_width })
            .await?
            .into_inner();
        Ok(stream)
    }

    #[cfg(feature = "grpc")]
    pub async fn subscribe_many_candles_from(
        &mut self,
        endpoint: impl AsRef<str>,
        // if None, subscribe for all markets available
        market_ids: Option<Vec<MarketId>>,
        candle_width: CandleWidth,
    ) -> Result<Streaming<Candle>> {
        let mut client = MarketdataClient::connect(endpoint.as_ref().to_string()).await?;
        let stream = client
            .subscribe_many_candles(SubscribeManyCandlesRequest {
                market_ids,
                candle_width,
            })
            .await?
            .into_inner();
        Ok(stream)
    }

    #[cfg(feature = "grpc")]
    pub async fn subscribe_trades_from(
        &mut self,
        endpoint: impl AsRef<str>,
        // if None, subscribe for all markets available
        market_id: Option<MarketId>,
    ) -> Result<Streaming<Trade>> {
        let mut client = MarketdataClient::connect(endpoint.as_ref().to_string()).await?;
        let stream = client
            .subscribe_trades(SubscribeTradesRequest { market_id })
            .await?
            .into_inner();
        Ok(stream)
    }
}
