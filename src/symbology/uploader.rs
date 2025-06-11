use super::store::SymbologyStore;
use crate::grpc::GrpcClientConfig;
use anyhow::Result;
use api::{
    grpc::service::symbology_client::SymbologyClient as SymbologyGrpcClient,
    symbology::{protocol::*, *},
};
use std::collections::BTreeMap;
use url::Url;

pub struct SymbologyUploader {
    pub products: BTreeMap<Product, ProductInfo>,
    pub product_aliases: BTreeMap<AliasKind, BTreeMap<String, Product>>,
    pub options_series: BTreeMap<OptionsSeries, OptionsSeriesInfo>,
    pub execution_info:
        BTreeMap<TradableProduct, BTreeMap<ExecutionVenue, ExecutionInfo>>,
}

impl Default for SymbologyUploader {
    fn default() -> Self {
        Self::new()
    }
}

impl From<SymbologySnapshot> for SymbologyUploader {
    fn from(snapshot: SymbologySnapshot) -> Self {
        Self {
            products: snapshot.products,
            product_aliases: snapshot.product_aliases,
            options_series: snapshot.options_series,
            execution_info: snapshot.execution_info,
        }
    }
}

impl SymbologyUploader {
    pub fn new() -> Self {
        Self {
            products: BTreeMap::new(),
            product_aliases: BTreeMap::new(),
            options_series: BTreeMap::new(),
            execution_info: BTreeMap::new(),
        }
    }

    pub async fn upload_to(
        self,
        url: &Url,
        grpc_config: &GrpcClientConfig,
    ) -> Result<()> {
        let channel = grpc_config.connect(url).await?;
        let mut grpc = SymbologyGrpcClient::new(channel)
            .max_encoding_message_size(100 * 1024 * 1024);
        grpc.upload_symbology(UploadSymbologyRequest {
            products: self.products,
            product_aliases: self.product_aliases,
            options_series: self.options_series,
            execution_info: self.execution_info,
        })
        .await?;
        Ok(())
    }

    pub fn into_store(self) -> SymbologyStore {
        let store = SymbologyStore::new();
        let mut inner = store.inner.lock();
        inner.products = self.products;
        inner.product_aliases = self.product_aliases;
        inner.options_series = self.options_series;
        inner.execution_info = self.execution_info;
        drop(inner);
        store
    }

    pub fn add_product_alias(
        &mut self,
        alias_kind: AliasKind,
        symbol_alias: String,
        symbol: Product,
    ) {
        self.product_aliases.entry(alias_kind).or_default().insert(symbol_alias, symbol);
    }

    pub fn add_execution_info(
        &mut self,
        symbol: TradableProduct,
        venue: ExecutionVenue,
        info: ExecutionInfo,
    ) {
        self.execution_info.entry(symbol).or_default().insert(venue, info);
    }
}

pub async fn upload_product_catalog(
    url: &Url,
    grpc_config: &GrpcClientConfig,
    exchange: ExecutionVenue,
    product_catalog: Vec<ProductCatalogInfo>,
) -> Result<()> {
    let channel = grpc_config.connect(url).await?;
    let mut grpc =
        SymbologyGrpcClient::new(channel).max_encoding_message_size(100 * 1024 * 1024);
    grpc.upload_product_catalog(UploadProductCatalogRequest {
        exchange,
        product_catalog,
    })
    .await?;
    Ok(())
}
