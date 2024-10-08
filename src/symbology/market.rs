use super::{
    allocator::StaticBumpAllocator, static_ref::StaticRef, Cpty, ProductRef, RouteRef,
    VenueRef,
};
use crate::static_ref;
use anyhow::{bail, Result};
#[cfg(feature = "netidx")]
use api::marketdata::NetidxFeedPaths;
use api::{
    symbology::{
        market::{MarketId, MarketInfo},
        Symbolic,
    },
    Str,
};
use arc_swap::ArcSwap;
use immutable_chunkmap::map::MapL as Map;
#[cfg(feature = "netidx")]
use netidx::path::Path;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::Serialize;
use smallvec::SmallVec;
use std::sync::{atomic::AtomicUsize, Arc};

static_ref!(MarketRef, MarketInner, 512);

impl MarketRef {
    /// Forward the new impl of the inner type as a convenience
    pub fn exchange(
        base: ProductRef,
        quote: ProductRef,
        venue: VenueRef,
        route: RouteRef,
        exchange_symbol: &str,
        extra_info: api::symbology::MarketInfo,
    ) -> Result<api::symbology::Market> {
        api::symbology::Market::exchange(
            &api::symbology::Product::from(&base),
            &api::symbology::Product::from(&quote),
            &*venue,
            &*route,
            exchange_symbol,
            extra_info,
        )
    }

    /// Forward the new impl of the inner type as a convenience
    pub fn pool(
        products: impl Iterator<Item = ProductRef>,
        venue: VenueRef,
        route: RouteRef,
        exchange_symbol: &str,
        extra_info: api::symbology::MarketInfo,
    ) -> Result<api::symbology::Market> {
        api::symbology::Market::pool(
            products.map(|p| (&p).into()),
            &venue,
            &route,
            exchange_symbol,
            extra_info,
        )
    }

    pub fn cpty(&self) -> Cpty {
        Cpty { venue: self.venue, route: self.route }
    }

    pub fn base(&self) -> Option<ProductRef> {
        if let MarketKind::Exchange(ExchangeMarketKind { base, .. }) = &self.kind {
            Some(*base)
        } else {
            None
        }
    }
}

impl From<MarketRef> for api::symbology::Market {
    fn from(m: MarketRef) -> api::symbology::Market {
        api::symbology::Market {
            id: m.id,
            name: m.name,
            kind: (&m.kind).into(),
            venue: m.venue.id,
            route: m.route.id,
            exchange_symbol: m.exchange_symbol,
            extra_info: m.extra_info.clone(),
        }
    }
}

#[cfg(feature = "netidx")]
impl NetidxFeedPaths for MarketRef {
    fn path_by_id(&self, base: &Path) -> Path {
        base.append("by-id").append(&self.id.to_string())
    }

    fn path_by_name(&self, base: &Path) -> Path {
        let path = base.append("by-name");
        match &self.kind {
            MarketKind::Exchange(emk) => {
                path.append(&emk.base.name).append(&emk.quote.name)
            }
            MarketKind::Pool(_) | MarketKind::Unknown => path.append(&self.name),
        }
    }

    fn unalias_id(&self) -> Option<String> {
        Some(self.id.to_string())
    }
}

/// Derivation of `api::symbology::Market` where ids are replaced with StaticRef's.
#[derive(Debug, Clone, Serialize)]
pub struct MarketInner {
    pub id: MarketId,
    pub name: Str,
    pub kind: MarketKind,
    pub venue: VenueRef,
    pub route: RouteRef,
    pub exchange_symbol: Str,
    pub extra_info: MarketInfo,
}

impl MarketInner {
    pub fn iter_references(&self, mut f: impl FnMut(ProductRef)) {
        match &self.kind {
            MarketKind::Exchange(emk) => {
                f(emk.base);
                f(emk.quote);
            }
            MarketKind::Pool(pmk) => {
                for p in &pmk.products {
                    f(*p);
                }
            }
            MarketKind::Unknown => {}
        }
    }
}

impl Symbolic for MarketInner {
    type Id = MarketId;

    fn type_name() -> &'static str {
        "market"
    }

    fn id(&self) -> Self::Id {
        self.id
    }

    fn name(&self) -> Str {
        self.name
    }
}

/// Derivation of `api::symbology::MarketKind` where ids are replaced with StaticRef's.
#[derive(Debug, Clone, Serialize)]
pub enum MarketKind {
    Exchange(ExchangeMarketKind),
    Pool(PoolMarketKind),
    Unknown,
}

impl From<&MarketKind> for api::symbology::MarketKind {
    fn from(mk: &MarketKind) -> api::symbology::MarketKind {
        match mk {
            MarketKind::Exchange(emk) => {
                api::symbology::MarketKind::Exchange((*emk).into())
            }
            MarketKind::Pool(pmk) => api::symbology::MarketKind::Pool(pmk.clone().into()),
            MarketKind::Unknown => api::symbology::MarketKind::Unknown,
        }
    }
}

/// Derivation of `api::symbology::ExchangeMarketKind` where ids are replaced with StaticRef's.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ExchangeMarketKind {
    pub base: ProductRef,
    pub quote: ProductRef,
}

impl From<ExchangeMarketKind> for api::symbology::market::ExchangeMarketKind {
    fn from(emk: ExchangeMarketKind) -> api::symbology::market::ExchangeMarketKind {
        api::symbology::market::ExchangeMarketKind {
            base: emk.base.id,
            quote: emk.quote.id,
        }
    }
}

/// Derivation of `api::symbology::PoolMarketKind` where ids are replaced with StaticRef's.
#[derive(Debug, Clone, Serialize)]
pub struct PoolMarketKind {
    pub products: SmallVec<[ProductRef; 2]>,
}

impl From<PoolMarketKind> for api::symbology::market::PoolMarketKind {
    fn from(pmk: PoolMarketKind) -> api::symbology::market::PoolMarketKind {
        api::symbology::market::PoolMarketKind {
            products: pmk.products.into_iter().map(|p| p.id).collect(),
        }
    }
}

/// E.g. CQG markets should actually display using Databento marketdata instead of
/// CQG marketdata;  this functions gives the preferred display market for a given
/// trading market.
pub fn preferred_marketdata_market(trading_market: MarketRef) -> Result<MarketRef> {
    if trading_market.route.name.as_str() == "CQG" {
        let trading_name = trading_market.name.as_str();
        let data_name = trading_name.replace("CQG", "DATABENTO");
        return MarketRef::find_by_name_or_id(&data_name);
    }
    Ok(trading_market)
}

pub fn preferred_trading_market(marketdata_market: MarketRef) -> Result<MarketRef> {
    if marketdata_market.venue.name.as_str() == "CME" {
        let display_name = marketdata_market.name.as_str();
        let trading_name = display_name.replace("DATABENTO", "CQG");
        return MarketRef::find_by_name_or_id(&trading_name);
    }
    Ok(marketdata_market)
}
