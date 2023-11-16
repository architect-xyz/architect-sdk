use crate::static_ref;
use allocator::StaticBumpAllocator;
use anyhow::{bail, Result};
use api::{symbology::Symbolic, Str};
use arc_swap::ArcSwap;
use immutable_chunkmap::map::MapL as Map;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use static_ref::StaticRef;
use std::sync::{atomic::AtomicUsize, Arc};

pub(self) mod allocator;
pub mod client;
pub mod index;
pub mod market;
pub mod product;
pub mod static_ref;
pub mod txn;

pub use market::{Market, MarketKind};
pub use product::{Product, ProductKind};
pub use txn::Txn;

// CR alee: TryFrom/Into api types for Product and Market could be optimized
// to avoid unnec clone; make the Inner types Copy

static_ref!(Venue, api::symbology::Venue, 64);
static_ref!(Route, api::symbology::Route, 64);

/// Commonly used compound type
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct Cpty {
    pub venue: Option<Venue>,
    pub route: Route,
}

// forward the inner [new] impls as a convenience

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
