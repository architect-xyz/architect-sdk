//! General purpose client for Architect

#[cfg(feature = "grpc")]
use anyhow::Result;
#[cfg(feature = "grpc")]
use api::{
    external::{marketdata::*, symbology::*},
    grpc::json_service::{marketdata_client::*, symbology_client::*},
};
#[cfg(feature = "grpc")]
use tonic::codec::Streaming;

#[derive(Default, Debug)]
pub struct ArchitectClient {}

impl ArchitectClient {
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
    pub async fn subscribe_l1_book_snapshots_from(
        // NB alee: keeping this mut for now in case we mux clients
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
}
