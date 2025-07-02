//! General purpose client for Architect.
//!
//! Provides a convenience interface for the underlying gRPC calls, handles
//! service discovery and authentication in the background, and also implements
//! some useful utilities and patterns for marketdata and orderflow.

use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwapOption;
use architect_api::{
    accounts::*,
    auth::*,
    core::*,
    folio::*,
    grpc::service::{
        accounts_client::*, auth_client::*, core_client::*, folio_client::*,
        marketdata_client::*, oms_client::*, symbology_client::*,
    },
    marketdata::{CandleWidth, *},
    oms::*,
    orderflow::*,
    symbology::{protocol::*, *},
    utils::pagination::OffsetAndLimit,
    *,
};
use arcstr::ArcStr;
use chrono::{DateTime, NaiveTime, Utc};
use hickory_resolver::TokioResolver;
use log::{debug, error, info};
use parking_lot::RwLock;
use std::{collections::HashMap, str::FromStr, sync::Arc};
use tonic::{
    codec::Streaming,
    metadata::MetadataValue,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Uri},
    IntoRequest, Request,
};

const ARCHITECT_CA: &[u8] = include_bytes!("ca.crt");

// convenience re-exports
pub use architect_api::{
    folio::{HistoricalFillsRequest, HistoricalOrdersRequest},
    oms::{CancelOrderRequest, PlaceOrderRequest},
};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Architect {
    core: Channel,
    symbology: Arc<RwLock<Option<Channel>>>,
    marketdata: Arc<RwLock<HashMap<MarketdataVenue, Channel>>>,
    hmart: Channel,
    ca: Arc<Certificate>,
    api_key: ArcStr,
    api_secret: ArcStr,
    jwt: Arc<ArcSwapOption<(ArcStr, DateTime<Utc>)>>,
}

impl Architect {
    pub async fn connect(
        api_key: impl AsRef<str>,
        api_secret: impl AsRef<str>,
    ) -> Result<Self> {
        Self::connect_to("app.architect.co", api_key, api_secret).await
    }

    pub async fn connect_to(
        endpoint: impl AsRef<str>,
        api_key: impl AsRef<str>,
        api_secret: impl AsRef<str>,
    ) -> Result<Self> {
        let endpoint = Self::resolve_endpoint(endpoint).await?;
        let hmart_endpoint =
            Self::resolve_endpoint("https://historical.marketdata.architect.co").await?;
        let core = endpoint.connect().await?;
        let marketdata = Arc::new(RwLock::new(HashMap::new()));
        let hmart = hmart_endpoint.connect_lazy();
        let t = Self {
            core,
            symbology: Arc::new(RwLock::new(None)),
            marketdata,
            hmart,
            ca: Arc::new(Certificate::from_pem(ARCHITECT_CA)),
            api_key: ArcStr::from(api_key.as_ref()),
            api_secret: ArcStr::from(api_secret.as_ref()),
            jwt: Arc::new(ArcSwapOption::empty()),
        };
        t.refresh_jwt(false).await?;
        t.discover_services().await?;
        Ok(t)
    }

    /// Resolve a service gRPC endpoint given its URL.
    ///
    /// If localhost or an IP address is given, it will be returned as is.
    ///
    /// If a domain name is given, it will be resolved to an IP address and
    /// port using SRV records.  If a port is specified in `url`, it always
    /// takes precedence over the port found in SRV records.
    pub async fn resolve_endpoint(endpoint: impl AsRef<str>) -> Result<Endpoint> {
        let uri: Uri = endpoint.as_ref().parse()?;
        let host = uri
            .host()
            .ok_or_else(|| anyhow!("no host name or ip address in endpoint"))?;
        let use_ssl = uri.scheme_str() == Some("https")
            || (uri.scheme_str() != Some("http") && host.ends_with(".architect.co"));
        let scheme = if use_ssl { "https" } else { "http" };
        let resolved = match uri.port() {
            Some(port) => {
                format!("{scheme}://{host}:{port}")
            }
            None => {
                // no port provided, lookup SRV records
                let resolver = TokioResolver::builder_tokio()?.build();
                let records = resolver.srv_lookup(host).await?;
                let rec = records
                    .iter()
                    .next()
                    .ok_or_else(|| anyhow!("no SRV records found for host: {host}"))?;
                let target = rec.target();
                let port = rec.port();
                format!("{scheme}://{target}:{port}")
            }
        };
        debug!("resolved endpoint: {} -> {}", endpoint.as_ref(), resolved);
        let mut endpoint = Endpoint::try_from(resolved)?
            .connect_timeout(std::time::Duration::from_secs(3));
        if use_ssl {
            endpoint =
                endpoint.tls_config(ClientTlsConfig::new().with_enabled_roots())?;
        }
        Ok(endpoint)
    }

    /// Refresh the JWT if it's nearing expiration (within 1 minute) or if force is true
    pub async fn refresh_jwt(&self, force: bool) -> Result<()> {
        if !force {
            if let Some(jwt_and_expiration) = self.jwt.load_full() {
                let (_jwt, expiration) = &*jwt_and_expiration;
                let now = Utc::now();
                if (*expiration - now).num_seconds() > 60 {
                    return Ok(());
                }
            }
        }
        info!("refreshing JWT...");
        let mut client = AuthClient::new(self.core.clone());
        let req = CreateJwtRequest {
            api_key: self.api_key.to_string(),
            api_secret: self.api_secret.to_string(),
            grants: None,
        };
        let res = client.create_jwt(req).await?;
        let jwt: ArcStr = format!("Bearer {}", res.into_inner().jwt).into();
        let expiration = Utc::now() + chrono::Duration::seconds(3600);
        self.jwt.store(Some(Arc::new((jwt, expiration))));
        Ok(())
    }

    async fn with_jwt<R, T>(&self, request: R) -> Result<Request<T>>
    where
        R: IntoRequest<T>,
    {
        if let Err(e) = self.refresh_jwt(false).await {
            error!("failed to refresh JWT: {e:?}");
        }
        match self.jwt.load_full() {
            Some(jwt_and_expiration) => {
                let (jwt, _expiration) = &*jwt_and_expiration;
                let mut req = request.into_request();
                req.metadata_mut()
                    .insert("authorization", MetadataValue::from_str(jwt.as_str())?);
                Ok(req)
            }
            _ => Ok(request.into_request()),
        }
    }

    /// Discover service endpoints from Architect.
    ///
    /// The Architect core is responsible for telling you where to find services
    /// like symbology and marketdata as per its configuration.  You can also
    /// manually set endpoints by calling set_symbology and set_marketdata
    /// directly.
    pub async fn discover_services(&self) -> Result<()> {
        info!("discovering service endpoints...");
        let mut client = CoreClient::new(self.core.clone());
        let req = ConfigRequest {};
        let res = client.config(req).await?;
        let config = res.into_inner();
        if let Some(symbology) = config.symbology {
            info!("setting symbology endpoint: {}", symbology);
            let endpoint = Self::resolve_endpoint(symbology).await?;
            let channel = endpoint.connect_lazy();
            let mut symbology = self.symbology.write();
            *symbology = Some(channel);
        }
        for (venue, endpoint) in config.marketdata {
            info!("setting marketdata endpoint for {venue}: {endpoint}");
            let endpoint = Self::resolve_endpoint(endpoint).await?;
            let channel = endpoint.connect_lazy();
            let mut marketdata = self.marketdata.write();
            marketdata.insert(venue, channel);
        }
        Ok(())
    }

    fn symbology(&self) -> Result<Channel> {
        let symbology = self.symbology.read();
        if let Some(channel) = &*symbology {
            Ok(channel.clone())
        } else {
            bail!("no symbology endpoint");
        }
    }

    /// Manually set the symbology endpoint.
    pub async fn set_symbology(&self, endpoint: impl AsRef<str>) -> Result<()> {
        let endpoint = Self::resolve_endpoint(endpoint).await?;
        info!("setting symbology endpoint: {}", endpoint.uri());
        let channel = endpoint.connect_lazy();
        let mut symbology = self.symbology.write();
        *symbology = Some(channel);
        Ok(())
    }

    /// Manually set the marketdata endpoint for a venue.
    pub async fn set_marketdata(
        &self,
        venue: MarketdataVenue,
        endpoint: impl AsRef<str>,
    ) -> Result<()> {
        let endpoint = Self::resolve_endpoint(endpoint).await?;
        info!("setting marketdata endpoint for {venue}: {}", endpoint.uri());
        let channel = endpoint.connect_lazy();
        let mut marketdata = self.marketdata.write();
        marketdata.insert(venue, channel);
        Ok(())
    }

    fn marketdata(&self, venue: impl AsRef<str>) -> Result<Channel> {
        let venue = venue.as_ref();
        let channel = self
            .marketdata
            .read()
            .get(venue)
            .ok_or_else(|| anyhow!("no marketdata endpoint set for {venue}"))?
            .clone();
        Ok(channel)
    }

    /// Manually set the hmart (historical marketdata service) endpoint.
    pub async fn set_hmart(&mut self, endpoint: impl AsRef<str>) -> Result<()> {
        let endpoint = Self::resolve_endpoint(endpoint).await?;
        info!("setting hmart endpoint: {}", endpoint.uri());
        self.hmart = endpoint.connect_lazy();
        Ok(())
    }

    /// List all symbols.
    ///
    /// If marketdata is specified, query the marketdata endpoint directly;
    /// this may give different answers than the OMS.
    pub async fn list_symbols(&self, marketdata: Option<&str>) -> Result<Vec<String>> {
        let channel = match marketdata {
            Some(venue) => self.marketdata(venue)?,
            None => self.core.clone(),
        };
        let mut client = SymbologyClient::new(channel);
        let req = SymbolsRequest {};
        let req = self.with_jwt(req).await?;
        let res = client.symbols(req).await?;
        let symbols = res.into_inner().symbols;
        Ok(symbols)
    }

    pub async fn get_futures_series(
        &self,
        series_symbol: impl AsRef<str>,
        include_expired: bool,
    ) -> Result<Vec<Product>> {
        let channel = self.symbology()?;
        let mut client = SymbologyClient::new(channel);
        let req = FuturesSeriesRequest {
            series_symbol: series_symbol.as_ref().to_string(),
            include_expired,
        };
        let req = self.with_jwt(req).await?;
        let res = client.futures_series(req).await?;
        Ok(res.into_inner().futures)
    }

    pub async fn get_market_status(
        &self,
        symbol: impl AsRef<str>,
        venue: impl AsRef<str>,
    ) -> Result<MarketStatus> {
        let symbol = symbol.as_ref();
        let venue = venue.as_ref();
        let channel = self.marketdata(venue)?;
        let mut client = MarketdataClient::new(channel);
        let req =
            MarketStatusRequest { symbol: symbol.to_string(), venue: Some(venue.into()) };
        let req = self.with_jwt(req).await?;
        let res = client.market_status(req).await?;
        Ok(res.into_inner())
    }

    pub async fn get_historical_candles(
        &self,
        symbol: impl AsRef<str>,
        venue: impl AsRef<str>,
        candle_width: CandleWidth,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<Vec<Candle>> {
        let symbol = symbol.as_ref();
        let venue = venue.as_ref();
        let channel = self.hmart.clone();
        let mut client = MarketdataClient::new(channel);
        let req = HistoricalCandlesRequest {
            symbol: symbol.to_string(),
            venue: Some(venue.into()),
            candle_width,
            start_date,
            end_date,
        };
        let req = self.with_jwt(req).await?;
        let res = client.historical_candles(req).await?;
        Ok(res.into_inner().candles)
    }

    pub async fn get_l1_book_snapshot(
        &self,
        symbol: impl AsRef<str>,
        venue: impl AsRef<str>,
    ) -> Result<L1BookSnapshot> {
        let symbol = symbol.as_ref();
        let venue = venue.as_ref();
        let channel = self.marketdata(venue)?;
        let mut client = MarketdataClient::new(channel);
        let req = L1BookSnapshotRequest {
            symbol: symbol.to_string(),
            venue: Some(venue.into()),
        };
        let req = self.with_jwt(req).await?;
        let res = client.l1_book_snapshot(req).await?;
        Ok(res.into_inner())
    }

    pub async fn get_l1_book_snapshots(
        &self,
        symbols: impl IntoIterator<Item = impl AsRef<str>>,
        venue: impl AsRef<str>,
    ) -> Result<Vec<L1BookSnapshot>> {
        let symbols =
            symbols.into_iter().map(|s| s.as_ref().to_string()).collect::<Vec<_>>();
        let venue = venue.as_ref();
        let channel = self.marketdata(venue)?;
        let mut client = MarketdataClient::new(channel);
        let req =
            L1BookSnapshotsRequest { symbols: Some(symbols), venue: Some(venue.into()) };
        let req = self.with_jwt(req).await?;
        let res = client.l1_book_snapshots(req).await?;
        Ok(res.into_inner())
    }

    pub async fn get_l2_book_snapshot(
        &self,
        symbol: impl AsRef<str>,
        venue: impl AsRef<str>,
    ) -> Result<L2BookSnapshot> {
        let symbol = symbol.as_ref();
        let venue = venue.as_ref();
        let channel = self.marketdata(venue)?;
        let mut client = MarketdataClient::new(channel);
        let req = L2BookSnapshotRequest {
            symbol: symbol.to_string(),
            venue: Some(venue.into()),
        };
        let req = self.with_jwt(req).await?;
        let res = client.l2_book_snapshot(req).await?;
        Ok(res.into_inner())
    }

    pub async fn get_ticker(
        &self,
        symbol: impl AsRef<str>,
        venue: impl AsRef<str>,
    ) -> Result<Ticker> {
        let symbol = symbol.as_ref();
        let venue = venue.as_ref();
        let channel = self.marketdata(venue)?;
        let mut client = MarketdataClient::new(channel);
        let req = TickerRequest { symbol: symbol.to_string(), venue: Some(venue.into()) };
        let req = self.with_jwt(req).await?;
        let res = client.ticker(req).await?;
        Ok(res.into_inner())
    }

    pub async fn get_tickers(
        &self,
        venue: impl AsRef<str>,
        options: GetTickersOptions,
        sort_tickers_by: Option<SortTickersBy>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> Result<Vec<Ticker>> {
        let venue = venue.as_ref();
        let channel = self.marketdata(venue)?;
        let mut client = MarketdataClient::new(channel);
        let req = TickersRequest {
            symbols: options.symbols,
            venue: Some(venue.into()),
            pagination: OffsetAndLimit { offset, limit, sort_by: sort_tickers_by },
            include_options: Some(options.include_options),
        };
        let req = self.with_jwt(req).await?;
        let res = client.tickers(req).await?;
        Ok(res.into_inner().tickers)
    }

    pub async fn stream_l1_book_snapshots(
        &self,
        symbols: impl IntoIterator<Item = impl AsRef<str>>,
        venue: impl AsRef<str>,
        send_initial_snapshots: bool,
    ) -> Result<Streaming<L1BookSnapshot>> {
        let symbols =
            symbols.into_iter().map(|s| s.as_ref().to_string()).collect::<Vec<_>>();
        let venue = venue.as_ref();
        let channel = self.marketdata(venue)?;
        let mut client = MarketdataClient::new(channel);
        let req = SubscribeL1BookSnapshotsRequest {
            symbols: Some(symbols),
            venue: Some(venue.into()),
            send_initial_snapshots,
        };
        let req = self.with_jwt(req).await?;
        let res = client.subscribe_l1_book_snapshots(req).await?;
        Ok(res.into_inner())
    }

    pub async fn stream_l2_book_updates(
        &self,
        symbol: impl AsRef<str>,
        venue: impl AsRef<str>,
    ) -> Result<Streaming<L2BookUpdate>> {
        let symbol = symbol.as_ref();
        let venue = venue.as_ref();
        let channel = self.marketdata(venue)?;
        let mut client = MarketdataClient::new(channel);
        let req = SubscribeL2BookUpdatesRequest {
            symbol: symbol.to_string(),
            venue: Some(venue.into()),
        };
        let req = self.with_jwt(req).await?;
        let res = client.subscribe_l2_book_updates(req).await?;
        Ok(res.into_inner())
    }

    pub async fn stream_trades(
        &self,
        symbol: Option<impl AsRef<str>>,
        venue: impl AsRef<str>,
    ) -> Result<Streaming<Trade>> {
        let symbol = symbol.as_ref();
        let venue = venue.as_ref();
        let channel = self.marketdata(venue)?;
        let mut client = MarketdataClient::new(channel);
        let req = SubscribeTradesRequest {
            symbol: symbol.map(|s| s.as_ref().to_string()),
            venue: Some(venue.into()),
        };
        let req = self.with_jwt(req).await?;
        let res = client.subscribe_trades(req).await?;
        Ok(res.into_inner())
    }

    pub async fn stream_candles(
        &self,
        symbol: impl AsRef<str>,
        venue: impl AsRef<str>,
        candle_widths: Option<impl IntoIterator<Item = &CandleWidth>>,
    ) -> Result<Streaming<Candle>> {
        let symbol = symbol.as_ref();
        let venue = venue.as_ref();
        let channel = self.marketdata(venue)?;
        let mut client = MarketdataClient::new(channel);
        let req = SubscribeCandlesRequest {
            symbol: symbol.to_string(),
            venue: Some(venue.into()),
            candle_widths: candle_widths.map(|c| c.into_iter().copied().collect()),
        };
        let req = self.with_jwt(req).await?;
        let res = client.subscribe_candles(req).await?;
        Ok(res.into_inner())
    }

    pub async fn list_accounts(
        &self,
        paper: bool,
        trader: Option<TraderIdOrEmail>,
    ) -> Result<Vec<AccountWithPermissions>> {
        let mut client = AccountsClient::new(self.core.clone());
        let req = AccountsRequest { paper, trader };
        let req = self.with_jwt(req).await?;
        let res = client.accounts(req).await?;
        Ok(res.into_inner().accounts)
    }

    pub async fn get_account_summary(
        &self,
        account: AccountIdOrName,
    ) -> Result<AccountSummary> {
        let mut client = FolioClient::new(self.core.clone());
        let req = AccountSummaryRequest { account };
        let req = self.with_jwt(req).await?;
        let res = client.account_summary(req).await?;
        Ok(res.into_inner())
    }

    pub async fn get_account_summaries(
        &self,
        account: Option<impl IntoIterator<Item = AccountIdOrName>>,
        trader: Option<TraderIdOrEmail>,
    ) -> Result<Vec<AccountSummary>> {
        let mut client = FolioClient::new(self.core.clone());
        let req = AccountSummariesRequest {
            accounts: account.map(|a| a.into_iter().collect()),
            trader,
        };
        let req = self.with_jwt(req).await?;
        let res = client.account_summaries(req).await?;
        Ok(res.into_inner().account_summaries)
    }

    pub async fn get_account_history(
        &self,
        account: AccountIdOrName,
        from_inclusive: Option<DateTime<Utc>>,
        to_exclusive: Option<DateTime<Utc>>,
        granularity: Option<AccountHistoryGranularity>,
        limit: Option<i32>,
        time_of_day: Option<NaiveTime>,
    ) -> Result<Vec<AccountSummary>> {
        let mut client = FolioClient::new(self.core.clone());
        let req = AccountHistoryRequest {
            account,
            from_inclusive,
            to_exclusive,
            granularity,
            limit,
            time_of_day,
        };
        let req = self.with_jwt(req).await?;
        let res = client.account_history(req).await?;
        Ok(res.into_inner().history)
    }

    pub async fn get_open_orders(
        &self,
        order_ids: Option<impl IntoIterator<Item = &OrderId>>,
        venue: Option<impl AsRef<str>>,
        account: Option<AccountIdOrName>,
        trader: Option<TraderIdOrEmail>,
        symbol: Option<impl AsRef<str>>,
        parent_order_id: Option<OrderId>,
    ) -> Result<Vec<Order>> {
        let mut client = OmsClient::new(self.core.clone());
        let req = OpenOrdersRequest {
            order_ids: order_ids.map(|o| o.into_iter().copied().collect()),
            venue: venue.map(|v| v.as_ref().into()),
            account,
            trader,
            symbol: symbol.map(|s| s.as_ref().to_string()),
            parent_order_id,
        };
        let req = self.with_jwt(req).await?;
        let res = client.open_orders(req).await?;
        Ok(res.into_inner().open_orders)
    }

    pub async fn get_all_open_orders(&self) -> Result<Vec<Order>> {
        self.get_open_orders(
            None::<&[OrderId]>,
            None::<&str>,
            None,
            None,
            None::<&str>,
            None,
        )
        .await
    }

    pub async fn get_historical_orders(
        &self,
        query: HistoricalOrdersRequest,
    ) -> Result<Vec<Order>> {
        let mut client = FolioClient::new(self.core.clone());
        let req = self.with_jwt(query).await?;
        let res = client.historical_orders(req).await?;
        Ok(res.into_inner().orders)
    }

    pub async fn get_fills(&self, query: HistoricalFillsRequest) -> Result<Vec<Fill>> {
        let mut client = FolioClient::new(self.core.clone());
        let req = self.with_jwt(query).await?;
        let res = client.historical_fills(req).await?;
        Ok(res.into_inner().fills)
    }

    pub async fn place_order(&self, place_order: PlaceOrderRequest) -> Result<Order> {
        let mut client = OmsClient::new(self.core.clone());
        let req = self.with_jwt(place_order).await?;
        let res = client.place_order(req).await?;
        Ok(res.into_inner())
    }

    pub async fn cancel_order(&self, cancel_order: CancelOrderRequest) -> Result<Cancel> {
        let mut client = OmsClient::new(self.core.clone());
        let req = self.with_jwt(cancel_order).await?;
        let res = client.cancel_order(req).await?;
        Ok(res.into_inner())
    }
}

#[derive(Default, Debug, Clone)]
pub struct GetTickersOptions {
    pub symbols: Option<Vec<String>>,
    pub include_options: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test deterministic endpoint resolution
    #[tokio::test]
    async fn test_resolve_endpoint() -> Result<()> {
        let e = Architect::resolve_endpoint("127.0.0.1:8081").await?;
        assert_eq!(e.uri().to_string(), "http://127.0.0.1:8081/");
        let e = Architect::resolve_endpoint("https://localhost:8081").await?;
        assert_eq!(e.uri().to_string(), "https://localhost:8081/");
        Ok(())
    }
}
