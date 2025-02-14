use crate::grpc::GrpcClientConfig;
use anyhow::{bail, Result};
use api::{
    grpc::json_service::symbology_client::SymbologyClient as SymbologyGrpcClient,
    symbology::protocol::*, utils::sequence::SequenceIdAndNumber,
};
use derive_more::{Deref, DerefMut};
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::broadcast;
use url::Url;

#[derive(Clone, Deref)]
pub struct SymbologyStore {
    #[deref]
    pub(super) inner: Arc<Mutex<IndexedSymbology>>,
    pub updates: broadcast::Sender<SymbologyUpdate>,
}

impl std::fmt::Debug for SymbologyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let product_count = self.inner.lock().snapshot.products.len();
        write!(f, "SymbologyStore[{} products]", product_count)
    }
}

#[derive(Deref, DerefMut)]
pub struct IndexedSymbology {
    #[deref]
    #[deref_mut]
    pub snapshot: SymbologySnapshot,
}

impl SymbologyStore {
    pub fn new() -> Self {
        let (updates, _) = broadcast::channel(1000);
        let snapshot = SymbologySnapshot {
            sequence: SequenceIdAndNumber::new_random(),
            products: BTreeMap::new(),
            product_aliases: BTreeMap::new(),
            options_series: BTreeMap::new(),
            execution_info: BTreeMap::new(),
        };
        Self { inner: Arc::new(Mutex::new(IndexedSymbology { snapshot })), updates }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.products.clear();
        inner.product_aliases.clear();
        inner.options_series.clear();
        inner.execution_info.clear();
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
        let mut grpc = SymbologyGrpcClient::new(channel);
        let mut updates =
            grpc.subscribe_symbology(SubscribeSymbology {}).await?.into_inner();
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
        if let Some(action) = update.product_aliases {
            action.apply2(&mut inner.product_aliases);
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
            Some(SnapshotOrUpdate1::Snapshot { snapshot: inner.products.clone() });
        update.product_aliases = Some(inner.product_aliases.clone().into());
        update.options_series =
            Some(SnapshotOrUpdate1::Snapshot { snapshot: inner.options_series.clone() });
        update.execution_info =
            Some(SnapshotOrUpdate2::Snapshot { snapshot: inner.execution_info.clone() });
        (update, inner.sequence)
    }
}
