use super::{
    allocator::StaticBumpAllocator, static_ref::StaticRef, Cpty, ProductRef, RouteRef,
    VenueRef,
};
use crate::static_ref;
use anyhow::{bail, Result};
use api::{
    marketdata::NetidxFeedPaths,
    symbology::{
        market::{MarketId, MarketInfo},
        Symbolic,
    },
    Str,
};
use arc_swap::ArcSwap;
use immutable_chunkmap::map::MapL as Map;
use netidx::path::Path;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
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
            &api::symbology::Product::from(&*base),
            &api::symbology::Product::from(&*quote),
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
            products.map(|p| (&*p).into()),
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
            MarketKind::Pool(pmk) => {
                let mut path = path;
                for p in &pmk.products {
                    path = path.append(&p.name);
                }
                path
            }
            MarketKind::Unknown => path.append("UNKNOWN"),
        }
    }

    fn unalias_id(&self) -> Option<String> {
        Some(self.id.to_string())
    }
}

/// Derivation of `api::symbology::Market` where ids are replaced with StaticRef's.
#[derive(Debug, Clone)]
pub struct MarketInner {
    pub id: MarketId,
    pub name: Str,
    pub kind: MarketKind,
    pub venue: VenueRef,
    pub route: RouteRef,
    pub exchange_symbol: Str,
    pub extra_info: MarketInfo,
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

impl From<MarketInner> for api::symbology::Market {
    fn from(m: MarketInner) -> api::symbology::Market {
        api::symbology::Market {
            id: m.id,
            name: m.name,
            kind: m.kind.into(),
            venue: m.venue.id,
            route: m.route.id,
            exchange_symbol: m.exchange_symbol,
            extra_info: m.extra_info,
        }
    }
}

/// Derivation of `api::symbology::MarketKind` where ids are replaced with StaticRef's.
#[derive(Debug, Clone)]
pub enum MarketKind {
    Exchange(ExchangeMarketKind),
    Pool(PoolMarketKind),
    Unknown,
}

impl From<MarketKind> for api::symbology::MarketKind {
    fn from(mk: MarketKind) -> api::symbology::MarketKind {
        match mk {
            MarketKind::Exchange(emk) => api::symbology::MarketKind::Exchange(emk.into()),
            MarketKind::Pool(pmk) => api::symbology::MarketKind::Pool(pmk.into()),
            MarketKind::Unknown => api::symbology::MarketKind::Unknown,
        }
    }
}

/// Derivation of `api::symbology::ExchangeMarketKind` where ids are replaced with StaticRef's.
#[derive(Debug, Clone, Copy)]
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
#[derive(Debug, Clone)]
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
pub fn preferred_marketdata_market(
    trading_market: MarketRef,
) -> anyhow::Result<MarketRef> {
    if trading_market.route.name.as_str() == "CQG" {
        let trading_name = trading_market.name.as_str();
        let data_name = trading_name.replace("CQG", "DATABENTO");
        return MarketRef::find_by_name_or_id(&data_name);
    }
    Ok(trading_market)
}

pub fn preferred_trading_market(
    marketdata_market: MarketRef,
) -> anyhow::Result<MarketRef> {
    if marketdata_market.venue.name.as_str() == "CME" {
        let display_name = marketdata_market.name.as_str();
        let trading_name = display_name.replace("DATABENTO", "CQG");
        return MarketRef::find_by_name_or_id(&trading_name);
    }
    Ok(marketdata_market)
}
