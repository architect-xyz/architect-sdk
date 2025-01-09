use crate::grpc::GrpcClientConfig;
use anyhow::Result;
use api::{
    grpc::json_service::symbology_v2_client::SymbologyV2Client as SymbologyV2GrpcClient,
    symbology_v2::{protocol::*, *},
};
use std::collections::BTreeMap;
use url::Url;

pub struct SymbologyUploader {
    pub products: BTreeMap<Product, ProductInfo>,
    pub tradable_products: BTreeMap<TradableProduct, TradableProductInfo>,
    pub options_series: BTreeMap<OptionsSeries, OptionsSeriesInfo>,
    pub execution_info: BTreeMap<String, BTreeMap<ExecutionVenue, ExecutionInfo>>,
    pub marketdata_info: BTreeMap<String, BTreeMap<MarketdataVenue, MarketdataInfo>>,
}

impl SymbologyUploader {
    pub fn new() -> Self {
        Self {
            products: BTreeMap::new(),
            tradable_products: BTreeMap::new(),
            options_series: BTreeMap::new(),
            execution_info: BTreeMap::new(),
            marketdata_info: BTreeMap::new(),
        }
    }

    pub async fn upload_to(
        self,
        url: &Url,
        grpc_config: &GrpcClientConfig,
    ) -> Result<()> {
        let channel = grpc_config.connect(url).await?;
        let mut grpc = SymbologyV2GrpcClient::new(channel);
        grpc.upload_symbology_v2(UploadSymbologyV2Request {
            products: self.products,
            tradable_products: self.tradable_products,
            options_series: self.options_series,
            execution_info: self.execution_info,
            marketdata_info: self.marketdata_info,
        })
        .await?;
        Ok(())
    }

    pub fn add_execution_info<S: AsRef<str>>(
        &mut self,
        symbol: S,
        venue: ExecutionVenue,
        info: ExecutionInfo,
    ) {
        self.execution_info
            .entry(symbol.as_ref().to_string())
            .or_insert_with(BTreeMap::new)
            .insert(venue, info);
    }

    pub fn add_marketdata_info<S: AsRef<str>>(
        &mut self,
        symbol: S,
        venue: MarketdataVenue,
        info: MarketdataInfo,
    ) {
        self.marketdata_info
            .entry(symbol.as_ref().to_string())
            .or_insert_with(BTreeMap::new)
            .insert(venue, info);
    }
}
