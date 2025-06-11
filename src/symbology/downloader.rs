use crate::grpc::GrpcClientConfig;
use anyhow::Result;
use api::{
    grpc::service::symbology_client::SymbologyClient as SymbologyGrpcClient,
    symbology::{protocol::*, *},
};
use url::Url;

pub async fn download_product_catalog(
    url: &Url,
    grpc_config: &GrpcClientConfig,
    exchange: ExecutionVenue,
) -> Result<Vec<ProductCatalogInfo>> {
    let channel = grpc_config.connect(url).await?;
    let mut grpc =
        SymbologyGrpcClient::new(channel).max_encoding_message_size(100 * 1024 * 1024);
    let response = grpc
        .download_product_catalog(DownloadProductCatalogRequest { exchange })
        .await?
        .into_inner();
    Ok(response.product_catalog)
}
