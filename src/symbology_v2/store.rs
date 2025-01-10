use crate::grpc::GrpcClientConfig;
use anyhow::{bail, Result};
use api::{
    grpc::json_service::symbology_v2_client::SymbologyV2Client as SymbologyV2GrpcClient,
    symbology_v2::protocol::*, utils::sequence::SequenceIdAndNumber,
};
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::broadcast;
use url::Url;

#[derive(Clone)]
pub struct SymbologyStore {
    inner: Arc<Mutex<SymbologySnapshot>>,
    pub updates: broadcast::Sender<SymbologyUpdate>,
}

impl std::ops::Deref for SymbologyStore {
    type Target = Arc<Mutex<SymbologySnapshot>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl SymbologyStore {
    pub fn new() -> Self {
        let (updates, _) = broadcast::channel(1000);
        Self {
            inner: Arc::new(Mutex::new(SymbologySnapshot {
                sequence: SequenceIdAndNumber::new_random(),
                products: BTreeMap::new(),
                tradable_products: BTreeMap::new(),
                options_series: BTreeMap::new(),
                execution_info: BTreeMap::new(),
                marketdata_info: BTreeMap::new(),
            })),
            updates,
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.products.clear();
        inner.tradable_products.clear();
        inner.options_series.clear();
        inner.execution_info.clear();
        inner.marketdata_info.clear();
    }

    /// Connect to and download a symbology snapshot from a URL.
    ///
    /// Useful if you don't need live subscription to symbols, just
    /// a snapshot to reference.
    pub async fn download_from(
        url: &Url,
        grpc_config: &GrpcClientConfig,
    ) -> Result<Self> {
        let channel = grpc_config.connect(url).await?;
        let mut grpc = SymbologyV2GrpcClient::new(channel);
        let mut updates =
            grpc.subscribe_symbology_v2(SubscribeSymbologyV2 {}).await?.into_inner();
        let t = Self::new();
        match updates.message().await {
            Err(e) => bail!("error reading from stream: {:?}", e),
            Ok(None) => bail!("symbology updates stream ended prematurely"),
            Ok(Some(update)) => {
                t.apply_update(update)?;
            }
        }
        Ok(t)
    }

    pub fn apply_update(&self, update: SymbologyUpdate) -> Result<SequenceIdAndNumber> {
        let mut inner = self.inner.lock();
        inner.sequence.advance();
        let update_to_send = update.clone();
        if let Some(action) = update.products {
            action.apply(&mut inner.products);
        }
        if let Some(action) = update.tradable_products {
            action.apply(&mut inner.tradable_products);
        }
        if let Some(action) = update.options_series {
            action.apply(&mut inner.options_series);
        }
        match update.execution_info {
            Some(SnapshotOrUpdate2::Snapshot { snapshot }) => {
                inner.execution_info = snapshot;
            }
            Some(SnapshotOrUpdate2::Update { updates }) => {
                for action in updates {
                    match action {
                        AddOrRemove2::Add { symbol, info } => {
                            inner
                                .execution_info
                                .entry(symbol)
                                .or_insert_with(BTreeMap::new)
                                .insert(info.execution_venue.clone(), info);
                        }
                        AddOrRemove2::Remove { symbol, venue } => {
                            if let Some(by_venue) = inner.execution_info.get_mut(&symbol)
                            {
                                by_venue.remove(&venue);
                            }
                        }
                    }
                }
            }
            None => {}
        }
        match update.marketdata_info {
            Some(SnapshotOrUpdate2::Snapshot { snapshot }) => {
                inner.marketdata_info = snapshot;
            }
            Some(SnapshotOrUpdate2::Update { updates }) => {
                for action in updates {
                    match action {
                        AddOrRemove2::Add { symbol, info } => {
                            inner
                                .marketdata_info
                                .entry(symbol)
                                .or_insert_with(BTreeMap::new)
                                .insert(info.marketdata_venue.clone(), info);
                        }
                        AddOrRemove2::Remove { symbol, venue } => {
                            if let Some(by_venue) = inner.marketdata_info.get_mut(&symbol)
                            {
                                by_venue.remove(&venue);
                            }
                        }
                    }
                }
            }
            None => {}
        }
        let _ = self
            .updates
            .send(SymbologyUpdate { sequence: inner.sequence, ..update_to_send });
        Ok(inner.sequence)
    }

    pub fn snapshot(&self) -> SymbologySnapshot {
        let inner = self.inner.lock();
        (*inner).clone()
    }

    pub fn snapshot_update(&self) -> (SymbologyUpdate, SequenceIdAndNumber) {
        let inner = self.inner.lock();
        let mut update = SymbologyUpdate::default();
        update.sequence = inner.sequence;
        update.products =
            Some(SnapshotOrUpdate::Snapshot { snapshot: inner.products.clone() });
        update.tradable_products = Some(SnapshotOrUpdate::Snapshot {
            snapshot: inner.tradable_products.clone(),
        });
        update.options_series =
            Some(SnapshotOrUpdate::Snapshot { snapshot: inner.options_series.clone() });
        update.execution_info =
            Some(SnapshotOrUpdate2::Snapshot { snapshot: inner.execution_info.clone() });
        update.marketdata_info =
            Some(SnapshotOrUpdate2::Snapshot { snapshot: inner.marketdata_info.clone() });
        (update, inner.sequence)
    }
}
