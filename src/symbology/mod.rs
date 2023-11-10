use crate::hcstatic;
use allocator::StaticBumpAllocator;
use anyhow::{bail, Result};
use api::{symbology::Symbolic, Str};
use arc_swap::ArcSwap;
use hcstatic::Hcstatic;
use immutable_chunkmap::map::MapL as Map;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::sync::Arc;

mod allocator;
pub mod client;
mod hcstatic;
pub mod txn;

pub use txn::Txn;

hcstatic!(Venue, api::symbology::Venue, 64);
hcstatic!(Route, api::symbology::Route, 64);
hcstatic!(Product, api::symbology::Product, 512);
hcstatic!(Market, api::symbology::Market, 512);

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
    pub fn new(
        name: &str,
        kind: api::symbology::MarketKind,
        venue: &api::symbology::Venue,
        route: &api::symbology::Route,
        exchange_symbol: &str,
        extra_info: api::symbology::MarketInfo,
    ) -> Result<api::symbology::Market> {
        api::symbology::Market::new(name, kind, venue, route, exchange_symbol, extra_info)
    }
}
