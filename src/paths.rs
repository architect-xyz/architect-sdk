use crate::symbology::{market::ExchangeMarketKind, Cpty, Market, MarketKind};
use anyhow::{bail, Result};
use api::{marketdata::NetidxFeedPaths, symbology::CptyId, ComponentId};
use fxhash::{FxHashMap, FxHashSet};
use netidx::path::Path;

/// Paths to services in netidx, "config-as-service-discovery"
#[derive(Debug, Clone)]
pub struct Paths {
    pub hosted_base: Path,
    pub local_base: Path,
    pub core_base: Path,
    pub local_components: FxHashSet<ComponentId>,
    pub remote_components: FxHashMap<ComponentId, Path>,
    pub use_local_symbology: bool,
    pub use_local_userdb: bool,
    pub use_local_marketdata: FxHashSet<CptyId>,
    pub use_legacy_marketdata: FxHashSet<CptyId>,
    pub use_legacy_hist_marketdata: FxHashSet<CptyId>,
}

impl Paths {
    /// Symbology server
    pub fn sym(&self) -> Path {
        let base =
            if self.use_local_symbology { &self.local_base } else { &self.hosted_base };
        base.append("sym")
    }

    /// Marketdata feeds
    pub fn marketdata(&self, cpty: Cpty, hist: bool) -> Path {
        let use_local = self.use_local_marketdata.contains(&cpty.id());
        let use_legacy = self.use_legacy_marketdata.contains(&cpty.id())
            || (hist && self.use_legacy_hist_marketdata.contains(&cpty.id()));
        let base = if use_local { &self.local_base } else { &self.hosted_base };
        if use_legacy {
            base.append("qf")
        } else {
            base.append("marketdata")
        }
    }

    /// Realtime marketdata feed
    pub fn marketdata_rt(&self, cpty: Cpty) -> Path {
        self.marketdata(cpty, false)
            .append("rt")
            .append(&cpty.venue.name)
            .append(&cpty.route.name)
    }

    /// Realtime marketdata feed for a specific market, referenced by-id
    pub fn marketdata_rt_by_id(&self, market: Market) -> Path {
        let cpty = market.cpty();
        if self.use_legacy_marketdata.contains(&cpty.id()) {
            self.marketdata(cpty, false)
                .append("rt")
                .append("by-id")
                .append(&market.id.to_string())
        } else {
            market.path_by_id(&self.marketdata_rt(cpty))
        }
    }

    /// Realtime marketdata feed for a specific market, aliased by-name
    pub fn marketdata_rt_by_name(&self, market: Market) -> Path {
        let cpty = market.cpty();
        if self.use_legacy_marketdata.contains(&cpty.id()) {
            match market.kind {
                MarketKind::Exchange(ExchangeMarketKind { base, quote }) => self
                    .marketdata(cpty, false)
                    .append("rt")
                    .append("by-name")
                    .append(&market.venue.name)
                    .append(&market.route.name)
                    .append(base.name.as_str())
                    .append(quote.name.as_str()),
                _ => {
                    panic!("legacy_marketdata_paths only supported for Exchange markets");
                }
            }
        } else {
            market.path_by_name(&self.marketdata_rt(cpty))
        }
    }

    /// Realtime marketdata candles base
    pub fn marketdata_ohlc(&self, cpty: Cpty) -> Path {
        self.marketdata(cpty, false)
            .append("ohlc")
            .append(&cpty.venue.name)
            .append(&cpty.route.name)
    }

    /// Realtime marketdata candles, aliased by-name
    /// If hist is true, pull the name as if we were querying historical data
    pub fn marketdata_ohlc_by_name(&self, market: Market, hist: bool) -> Path {
        let cpty = market.cpty();
        if self.use_legacy_marketdata.contains(&cpty.id())
            || (hist && self.use_legacy_hist_marketdata.contains(&cpty.id()))
        {
            match market.kind {
                MarketKind::Exchange(ExchangeMarketKind { base, quote }) => self
                    .marketdata(cpty, hist)
                    .append("ohlc")
                    .append("by-name")
                    .append(&market.venue.name)
                    .append(&market.route.name)
                    .append(base.name.as_str())
                    .append(quote.name.as_str()),
                _ => {
                    panic!("legacy_marketdata_paths only supported for Exchange markets");
                }
            }
        } else {
            let base = self.marketdata_ohlc(cpty);
            market.path_by_name(&base)
        }
    }

    /// Historical marketdata candles recorder
    pub fn marketdata_hist_ohlc(&self, cpty: Cpty) -> Path {
        if self.use_legacy_marketdata.contains(&cpty.id())
            || self.use_legacy_hist_marketdata.contains(&cpty.id())
        {
            self.marketdata(cpty, true)
                .append("hist")
                .append("ohlc")
                .append("by-name")
                .append(&cpty.venue.name)
                .append(&cpty.route.name)
        } else {
            self.marketdata(cpty, true)
                .append("hist")
                .append("ohlc")
                .append(&cpty.venue.name)
                .append(&cpty.route.name)
        }
    }

    /// Marketdata APIs (RPCs)
    pub fn marketdata_api(&self, cpty: Cpty) -> Path {
        self.marketdata(cpty, false)
            .append("api")
            .append(&cpty.venue.name)
            .append(&cpty.route.name)
    }

    /// RFQ feeds
    pub fn marketdata_rfq(&self, cpty: Cpty) -> Path {
        self.marketdata(cpty, false)
            .append("rfq")
            .append(&cpty.venue.name)
            .append(&cpty.route.name)
    }

    /// Marketdata snaphots and USD marks service
    pub fn marketdata_snapshots(&self, local: bool) -> Path {
        let base = if local { &self.local_base } else { &self.hosted_base };
        base.append("marketdata").append("snapshots")
    }

    pub fn marketdata_marks(&self) -> Path {
        self.local_base.append("qf").append("marks")
    }

    /// Core RPCs base path
    pub fn core(&self) -> Path {
        self.core_base.clone()
    }

    /// Channel for the core started by this config file
    pub fn channel(&self) -> Path {
        self.core_base.append("channel")
    }

    /// One-way write-only channel for core-to-core communication
    pub fn in_channel(&self) -> Path {
        self.core_base.append("in-channel")
    }

    /// Find the most direct channel for a given component
    pub fn component(&self, com: ComponentId) -> Result<Path> {
        if self.local_components.contains(&com) {
            Ok(self.core_base.append("channel"))
        } else if let Some(base) = self.remote_components.get(&com) {
            Ok(base.append("channel"))
        } else {
            bail!("no path to component {}", com);
        }
    }

    /// UserDB (licensing, registration, etc.)
    pub fn userdb(&self) -> Path {
        let base =
            if self.use_local_userdb { &self.local_base } else { &self.hosted_base };
        base.append("userdb")
    }
}
