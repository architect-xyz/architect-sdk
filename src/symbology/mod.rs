use crate::hcstatic;
use allocator::StaticBumpAllocator;
use anyhow::{bail, Result};
use api::{qf::NetidxQfPaths, symbology::Symbolic, Str};
use arc_swap::ArcSwap;
use hcstatic::Hcstatic;
use immutable_chunkmap::map::MapL as Map;
use netidx::path::Path;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::sync::Arc;

mod allocator;
pub mod client;
pub mod hcstatic;
pub mod txn;

pub use txn::Txn;

hcstatic!(Venue, api::symbology::Venue, 64);
hcstatic!(Route, api::symbology::Route, 64);
hcstatic!(Product, api::symbology::Product, 512);
hcstatic!(Market, api::symbology::Market, 512);

/// Commonly used compound type
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct Cpty {
    pub venue: Option<Venue>,
    pub route: Route,
}

// forward mount the [new] impls as a convenience

impl Venue {
    pub fn new(name: &str) -> Result<api::symbology::Venue> {
        api::symbology::Venue::new(name)
    }
}

impl Route {
    pub fn new(name: &str) -> Result<api::symbology::Route> {
        api::symbology::Route::new(name)
    }
}

impl Product {
    pub fn new(
        name: &str,
        kind: api::symbology::ProductKind,
    ) -> Result<api::symbology::Product> {
        api::symbology::Product::new(name, kind)
    }
}

impl Market {
    pub fn exchange(
        base: &api::symbology::Product,
        quote: &api::symbology::Product,
        venue: &api::symbology::Venue,
        route: &api::symbology::Route,
        exchange_symbol: &str,
        extra_info: api::symbology::MarketInfo,
    ) -> Result<api::symbology::Market> {
        api::symbology::Market::exchange(
            base,
            quote,
            venue,
            route,
            exchange_symbol,
            extra_info,
        )
    }

    pub fn pool(
        products: &[api::symbology::Product],
        venue: &api::symbology::Venue,
        route: &api::symbology::Route,
        exchange_symbol: &str,
        extra_info: api::symbology::MarketInfo,
    ) -> Result<api::symbology::Market> {
        api::symbology::Market::pool(products, venue, route, exchange_symbol, extra_info)
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
