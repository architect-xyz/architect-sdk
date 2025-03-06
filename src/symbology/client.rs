use super::store::SymbologyStore;
use crate::{
    grpc::GrpcClientConfig,
    synced::{Synced, SyncedHandle},
};
use anyhow::{bail, Result};
use api::{
    grpc::json_service::symbology_client::SymbologyClient as SymbologyGrpcClient,
    symbology::protocol::{SubscribeSymbology, SymbologyUpdate},
    utils::sequence::SequenceIdAndNumber,
};
use log::{debug, trace};
use tonic::{
    transport::{Channel, Endpoint},
    Streaming,
};
use url::Url;

pub struct SymbologyClient {
    grpc_endpoint: Endpoint,
    grpc_config: GrpcClientConfig,
    grpc: SymbologyGrpcClient<Channel>,
    upstream_seqno: Option<SequenceIdAndNumber>,
    updates: Streaming<SymbologyUpdate>,
    pub store: SymbologyStore,
    pub ready: Synced<bool>,
}

#[derive(Clone)]
pub struct SymbologyClientHandle {
    pub store: SymbologyStore,
    pub ready: SyncedHandle<bool>,
}

impl SymbologyClient {
    pub async fn connect(url: &Url, grpc_config: GrpcClientConfig) -> Result<Self> {
        let endpoint = Endpoint::try_from(url.to_string())?;
        debug!("connecting to {}...", endpoint.uri());
        let channel = grpc_config.connect_to(endpoint.clone()).await?;
        let mut grpc = SymbologyGrpcClient::new(channel)
            .max_decoding_message_size(100 * 1024 * 1024)
            .max_encoding_message_size(100 * 1024 * 1024);
        debug!("subscribing to symbology updates...");
        let updates = grpc.subscribe_symbology(SubscribeSymbology {}).await?.into_inner();
        Ok(Self {
            grpc_endpoint: endpoint,
            grpc_config,
            grpc,
            upstream_seqno: None,
            updates,
            store: SymbologyStore::new(),
            ready: Synced::new(false),
        })
    }

    pub async fn reconnect(&mut self) -> Result<()> {
        debug!("reconnecting to {}...", self.grpc_endpoint.uri());
        let channel = self.grpc_config.connect_to(self.grpc_endpoint.clone()).await?;
        self.grpc = SymbologyGrpcClient::new(channel)
            .max_decoding_message_size(100 * 1024 * 1024)
            .max_encoding_message_size(100 * 1024 * 1024);
        debug!("subscribing to symbology updates...");
        self.grpc.subscribe_symbology(SubscribeSymbology {}).await?.into_inner();
        Ok(())
    }

    pub fn handle(&self) -> SymbologyClientHandle {
        SymbologyClientHandle { store: self.store.clone(), ready: self.ready.handle() }
    }

    pub async fn next(&mut self) -> Result<()> {
        match self.updates.message().await {
            Err(e) => bail!("error reading from stream: {:?}", e),
            Ok(None) => bail!("symbology updates stream ended"),
            Ok(Some(update)) => {
                trace!("received update: {:?}", update);
                if let Some(upstream_seqno) = self.upstream_seqno {
                    if !update.sequence.is_next_in_sequence(&upstream_seqno) {
                        bail!(
                            "skipped sequence number: {} -> {}",
                            upstream_seqno,
                            update.sequence
                        );
                    }
                }
                self.upstream_seqno = Some(update.sequence);
                self.store.apply_update(update)?;
                self.ready.set(true);
            }
        }
        Ok(())
    }
}
