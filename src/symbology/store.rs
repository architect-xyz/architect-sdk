use crate::grpc::GrpcClientConfig;
use anyhow::{bail, Result};
use api::{
    grpc::json_service::symbology_client::SymbologyClient as SymbologyGrpcClient,
    symbology::{
        protocol::*, ExecutionInfo, ExecutionVenue, ProductCatalogInfo, TradableProduct,
    },
    utils::sequence::SequenceIdAndNumber,
};
use derive_more::{Deref, DerefMut};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
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

impl Default for SymbologyStore {
    fn default() -> Self {
        Self::new()
    }
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
            product_catalog: BTreeMap::new(),
        };
        Self { inner: Arc::new(Mutex::new(IndexedSymbology { snapshot })), updates }
    }

    pub fn clear(&mut self) {
        let mut inner = self.inner.lock();
        inner.products.clear();
        inner.product_aliases.clear();
        inner.options_series.clear();
        inner.execution_info.clear();
        inner.product_catalog.clear();
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
        let mut grpc = SymbologyGrpcClient::new(channel)
            .max_decoding_message_size(100 * 1024 * 1024);
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
        if let Some(action) = update.product_catalog {
            action.apply2(&mut inner.product_catalog);
        }
        if let Some(action) = update.options_series {
            action.apply(&mut inner.options_series);
        }
        if let Some(action) = update.execution_info {
            action.apply2(&mut inner.execution_info);
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
        let update = SymbologyUpdate {
            sequence: inner.sequence,
            products: Some(inner.products.clone().into()),
            product_aliases: Some(inner.product_aliases.clone().into()),
            product_catalog: Some(inner.product_catalog.clone().into()),
            options_series: Some(inner.options_series.clone().into()),
            execution_info: Some(inner.execution_info.clone().into()),
        };
        (update, inner.sequence)
    }

    pub fn product_catalog(&self, exchange: &str) -> Option<Vec<ProductCatalogInfo>> {
        let inner = self.inner.lock();
        if let Some(items) = inner.product_catalog.get(exchange) {
            let mut catalog = vec![];
            for item in items.values() {
                catalog.push(item.clone());
            }
            Some(catalog)
        } else {
            None
        }
    }

    pub fn symbols(&self) -> Vec<String> {
        let mut symbols = BTreeSet::default();
        symbols.extend(self.inner.lock().products.keys().map(|k| k.to_string()));
        symbols.extend(self.inner.lock().execution_info.keys().map(|k| k.to_string()));
        symbols.into_iter().collect()
    }

    pub fn execution_info(
        &self,
        symbol: impl AsRef<str>,
        execution_venue: Option<&ExecutionVenue>,
    ) -> BTreeMap<ExecutionVenue, ExecutionInfo> {
        let mut res = BTreeMap::new();
        if let Some(infos) = self.inner.lock().execution_info.get(symbol.as_ref()) {
            if let Some(venue) = execution_venue {
                if let Some(info) = infos.get(venue) {
                    res.insert(venue.clone(), info.clone());
                }
            } else {
                for (venue, info) in infos.iter() {
                    res.insert(venue.clone(), info.clone());
                }
            }
        }
        res
    }

    // CR alee: not really sure this is a great idea in general without
    // thinking harder about what the sequence number means.  But for now
    // it's useful.
    pub fn add_execution_info(
        &self,
        execution_info: BTreeMap<
            TradableProduct,
            BTreeMap<ExecutionVenue, ExecutionInfo>,
        >,
    ) {
        let mut inner = self.inner.lock();
        // inner.sequence.advance();
        inner.execution_info.extend(execution_info);
    }
}
