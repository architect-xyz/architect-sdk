//! General purpose client for Architect

#[cfg(feature = "graphql")]
use anyhow::anyhow;
use anyhow::{bail, Result};
#[cfg(feature = "grpc")]
use api::marketdata::CandleWidth;
#[cfg(feature = "grpc")]
use api::{
    external::{marketdata::*, symbology::*},
    grpc::json_service::{marketdata_client::*, symbology_client::*},
};
use arc_swap::ArcSwapOption;
#[cfg(feature = "graphql")]
use graphql_client::{GraphQLQuery, Response};
#[cfg(feature = "grpc")]
use hickory_resolver::{config::*, TokioAsyncResolver};
#[cfg(feature = "graphql")]
use log::debug;
use std::sync::Arc;
#[cfg(feature = "grpc")]
use tonic::{
    codec::Streaming,
    metadata::MetadataValue,
    transport::{Certificate, ClientTlsConfig, Endpoint},
    Request,
};
#[cfg(feature = "grpc")]
use url::Host;
use url::Url;

const ARCHITECT_CA: &[u8] = include_bytes!("ca.crt");

#[derive(Debug)]
pub struct ArchitectClientConfig {
    pub hostname: Option<String>,
    pub port: Option<u16>,
    pub no_tls: bool,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
}

impl Default for ArchitectClientConfig {
    fn default() -> Self {
        Self {
            hostname: Some("app.architect.co".to_string()),
            port: None,
            no_tls: false,
            api_key: None,
            api_secret: None,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ArchitectClient {
    inner: Arc<ArchitectClientInner>,
    #[cfg(feature = "grpc")]
    ca: Arc<Certificate>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct ArchitectClientInner {
    graphql_endpoint: Option<Url>,
    #[cfg(feature = "graphql")]
    graphql_client: reqwest::Client,
    graphql_authorization: Option<String>,
    architect_jwt: ArcSwapOption<String>,
}

impl Default for ArchitectClient {
    fn default() -> Self {
        Self::new(ArchitectClientConfig::default()).unwrap()
    }
}

impl ArchitectClient {
    pub fn new(config: ArchitectClientConfig) -> Result<Self> {
        let installation_base = if let Some(hostname) = config.hostname.as_ref() {
            let mut base = String::new();
            base.push_str(if config.no_tls { "http://" } else { "https://" });
            base.push_str(hostname);
            if let Some(port) = config.port {
                base.push_str(&format!(":{port}"));
            }
            Some(Url::parse(&base)?)
        } else {
            None
        };
        let graphql_endpoint = if let Some(base) = installation_base.as_ref() {
            Some(base.join("graphql")?)
        } else {
            None
        };
        #[cfg(feature = "graphql")]
        let graphql_client = reqwest::Client::new();
        let graphql_authorization = match (config.api_key, config.api_secret) {
            (Some(key), Some(secret)) => Some(format!("Basic {key} {secret}")),
            (Some(_), None) => bail!("api_key is provided but api_secret is not"),
            (None, Some(_)) => bail!("api_secret is provided but api_key is not"),
            (None, None) => None,
        };
        let inner = ArchitectClientInner {
            graphql_endpoint,
            #[cfg(feature = "graphql")]
            graphql_client,
            graphql_authorization,
            architect_jwt: ArcSwapOption::empty(),
        };
        Ok(Self {
            inner: Arc::new(inner),
            #[cfg(feature = "grpc")]
            ca: Arc::new(Certificate::from_pem(ARCHITECT_CA)),
        })
    }

    #[cfg(feature = "graphql")]
    fn graphql_endpoint(&self) -> Result<&str> {
        self.inner
            .graphql_endpoint
            .as_ref()
            .map(|url| url.as_str())
            .ok_or_else(|| anyhow!("graphql_endpoint not set"))
    }

    #[cfg(feature = "graphql")]
    fn graphql_authorization(&self) -> Result<&str> {
        self.inner
            .graphql_authorization
            .as_deref()
            .ok_or_else(|| anyhow!("graphql authorization not set"))
    }

    #[cfg(feature = "graphql")]
    pub async fn refresh_jwt(&self) -> Result<()> {
        use super::graphql::{
            create_jwt::{ResponseData, Variables},
            CreateJwt,
        };
        let authorization = self.graphql_authorization()?;
        let body = CreateJwt::build_query(Variables);
        let res = self
            .inner
            .graphql_client
            .post(self.graphql_endpoint()?)
            .json(&body)
            .header(reqwest::header::AUTHORIZATION, authorization)
            .send()
            .await?;
        let res_body: Response<ResponseData> = res.json().await?;
        if let Some(errors) = res_body.errors {
            bail!("error in response: {errors:?}");
        }
        let res_data = res_body.data.ok_or_else(|| anyhow!("no data in response"))?;
        debug!("refreshed jwt: {}", res_data.create_jwt);
        self.inner
            .architect_jwt
            .store(Some(Arc::new(format!("Bearer {}", res_data.create_jwt))));
        Ok(())
    }

    /// Get the JWT authorization header value
    fn jwt_authorization(&self) -> Result<Arc<String>> {
        self.inner
            .architect_jwt
            .load_full()
            .ok_or_else(|| anyhow!("no JWT bearer token set"))
    }

    #[cfg(feature = "grpc")]
    /// Resolve a service gRPC endpoint given its URL.
    ///
    /// If localhost or an IP address is given, it will be returned as is.
    ///
    /// If a domain name is given, it will be resolved to an IP address and
    /// port using SRV records.  If a port is specified in `url`, it always
    /// takes precedence over the port found in SRV records.
    pub async fn resolve_service<U>(&self, url: U) -> Result<Endpoint>
    where
        U: TryInto<Url>,
        U::Error: std::error::Error + Send + Sync + 'static,
    {
        let url: Url = url.try_into()?;
        let mut resolved = String::new();
        resolved.push_str(url.scheme());
        resolved.push_str("://");
        let mut port = url.port();
        match url.host() {
            None => bail!("no host name or ip address in endpoint"),
            Some(Host::Ipv4(addr)) => resolved.push_str(&addr.to_string()),
            Some(Host::Ipv6(addr)) => resolved.push_str(&addr.to_string()),
            Some(Host::Domain(domain)) if domain == "localhost" => {
                resolved.push_str("localhost")
            }
            Some(Host::Domain(domain)) => {
                let resolver = TokioAsyncResolver::tokio(
                    ResolverConfig::default(),
                    ResolverOpts::default(),
                );
                let records = resolver.srv_lookup(domain).await?;
                let rec = records.iter().next().ok_or_else(|| {
                    anyhow!("no SRV records found for domain: {domain}")
                })?;
                resolved.push_str(&rec.target().to_string());
                if port.is_none() {
                    port = Some(rec.port());
                }
            }
        }
        if let Some(port) = port {
            resolved.push_str(&format!(":{port}"));
        }
        let mut endpoint = Endpoint::try_from(resolved)?
            .connect_timeout(std::time::Duration::from_secs(3));
        if url.scheme() == "https" {
            endpoint = endpoint.tls_config(
                ClientTlsConfig::new()
                    .domain_name("service.architect.xyz")
                    .ca_certificate((*self.ca).clone()),
            )?;
        }
        Ok(endpoint)
    }

    /// Load symbology from the given endpoint into global memory.
    #[cfg(feature = "grpc")]
    pub async fn load_symbology_from(&self, endpoint: &Endpoint) -> Result<()> {
        use crate::symbology::Txn;
        let channel = endpoint.connect().await?;
        let mut client = SymbologyClient::new(channel);
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
    pub async fn load_symbology_from_all(
        &self,
        endpoints: impl IntoIterator<Item = &Endpoint>,
    ) -> Result<()> {
        for endpoint in endpoints.into_iter() {
            self.load_symbology_from(endpoint).await?;
        }
        Ok(())
    }

    // CR alee: probably the right abstraction is to have a method `marketdata()`
    // that returns an inner client which owns the channel, configures the
    // authorizaiton header, etc.  Then you call subscribe_*/etc methods on that,
    // which have to be &mut.
    //
    // This lets the caller decide how to mux, while still letting the simple
    // case be easy, just call `marketdata().method()`.
    #[cfg(feature = "grpc")]
    pub async fn subscribe_l1_book_snapshots_from(
        &self,
        endpoint: &Endpoint,
        // if None, subscribe to all L1 books for all markets available
        market_ids: Option<Vec<MarketId>>,
    ) -> Result<Streaming<L1BookSnapshot>> {
        let channel = endpoint.connect().await?;
        let mut client = MarketdataClient::new(channel);
        let stream = client
            .subscribe_l1_book_snapshots(SubscribeL1BookSnapshotsRequest { market_ids })
            .await?
            .into_inner();
        Ok(stream)
    }

    #[cfg(feature = "grpc")]
    pub async fn subscribe_candles_from(
        &self,
        endpoint: &Endpoint,
        market_id: MarketId,
        // if None, subscribe for all widths available
        candle_width: Option<Vec<CandleWidth>>,
    ) -> Result<Streaming<Candle>> {
        let token: MetadataValue<_> = self.jwt_authorization()?.parse()?;
        let channel = endpoint.connect().await?;
        let mut client =
            MarketdataClient::with_interceptor(channel, move |mut req: Request<_>| {
                req.metadata_mut().insert("authorization", token.clone());
                Ok(req)
            });
        let stream = client
            .subscribe_candles(SubscribeCandlesRequest { market_id, candle_width })
            .await?
            .into_inner();
        Ok(stream)
    }

    #[cfg(feature = "grpc")]
    pub async fn subscribe_many_candles_from(
        &self,
        endpoint: &Endpoint,
        // if None, subscribe for all markets available
        market_ids: Option<Vec<MarketId>>,
        candle_width: CandleWidth,
    ) -> Result<Streaming<Candle>> {
        let token: MetadataValue<_> = self.jwt_authorization()?.parse()?;
        let channel = endpoint.connect().await?;
        let mut client =
            MarketdataClient::with_interceptor(channel, move |mut req: Request<_>| {
                req.metadata_mut().insert("authorization", token.clone());
                Ok(req)
            });
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
        &self,
        endpoint: &Endpoint,
        // if None, subscribe for all markets available
        market_id: Option<MarketId>,
    ) -> Result<Streaming<Trade>> {
        let token: MetadataValue<_> = self.jwt_authorization()?.parse()?;
        let channel = endpoint.connect().await?;
        let mut client =
            MarketdataClient::with_interceptor(channel, move |mut req: Request<_>| {
                req.metadata_mut().insert("authorization", token.clone());
                Ok(req)
            });
        let stream = client
            .subscribe_trades(SubscribeTradesRequest { market_id })
            .await?
            .into_inner();
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that the default client can be created and doesn't panic
    #[test]
    fn test_new() {
        let _client = ArchitectClient::default();
    }
}
