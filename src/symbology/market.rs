use super::{allocator::StaticBumpAllocator, hcstatic::Hcstatic, Product, Route, Venue};
use crate::hcstatic;
use anyhow::{bail, Result};
use api::{
    qf::NetidxQfPaths,
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

hcstatic!(Market, MarketInner, 512);

impl Market {
    /// Forward the new impl of the inner type as a convenience
    pub fn exchange(
        base: Product,
        quote: Product,
        venue: Venue,
        route: Route,
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
        products: impl Iterator<Item = Product>,
        venue: Venue,
        route: Route,
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
}

impl NetidxQfPaths for Market {
    fn path_by_id(&self, base: &Path) -> Path {
        base.append("by-id").append(&self.id.to_string())
    }

    fn path_by_name(&self, base: &Path) -> Path {
        base.append("by-name").append(&self.name)
    }

    fn unalias_id(&self) -> Option<String> {
        Some(self.id.to_string())
    }
}

/// Derivation of `api::symbology::Market` where ids are replaced with hcstatics.
#[derive(Debug, Clone)]
pub struct MarketInner {
    pub id: MarketId,
    pub name: Str,
    pub kind: MarketKind,
    pub venue: Venue,
    pub route: Route,
    pub exchange_symbol: Str,
    pub extra_info: MarketInfo,
}

impl Symbolic for MarketInner {
    type Id = MarketId;

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

/// Derivation of `api::symbology::MarketKind` where ids are replaced with hcstatics.
#[derive(Debug, Clone)]
pub enum MarketKind {
    Exchange { base: Product, quote: Product },
    Pool(SmallVec<[Product; 2]>),
    Unknown,
}

impl From<MarketKind> for api::symbology::MarketKind {
    fn from(mk: MarketKind) -> api::symbology::MarketKind {
        match mk {
            MarketKind::Exchange { base, quote } => {
                api::symbology::MarketKind::Exchange { base: base.id, quote: quote.id }
            }
            MarketKind::Pool(products) => api::symbology::MarketKind::Pool(
                products.into_iter().map(|p| p.id).collect(),
            ),
            MarketKind::Unknown => api::symbology::MarketKind::Unknown,
        }
    }
}
