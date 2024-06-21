use crate::static_ref;
use allocator::StaticBumpAllocator;
use anyhow::{anyhow, bail, Result};
use api::{
    symbology::{CptyId, Symbolic},
    Str,
};
use arc_swap::ArcSwap;
use immutable_chunkmap::map::MapL as Map;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::{
    fmt,
    sync::{atomic::AtomicUsize, Arc},
};

pub(self) mod allocator;
pub mod client;
pub mod index;
pub mod market;
pub mod product;
pub mod static_ref;
pub mod txn;

pub use index::MarketIndex;
pub use market::{MarketKind, MarketRef};
pub use product::{ProductKind, ProductRef};
pub use static_ref::StaticRef;
pub use txn::Txn;

// CR alee: TryFrom/Into api types for Product and Market could be optimized
// to avoid unnecessary clone; make the Inner types Copy

static_ref!(VenueRef, api::symbology::Venue, 64);
static_ref!(RouteRef, api::symbology::Route, 64);

// forward the inner [new] impls as a convenience

impl VenueRef {
    pub fn new(name: &str) -> Result<api::symbology::Venue> {
        api::symbology::Venue::new(name)
    }

    pub fn is_rfq(&self) -> bool {
        match self.name.as_str() {
            "WINTERMUTE" | "B2C2" | "GALAXY" | "CUMBERLAND" | "DVCHAIN" | "SFOX"
            | "FALCONX" => true,
            _ => false,
        }
    }

    pub fn is_otc(&self) -> bool {
        self.is_rfq()
    }
}

impl RouteRef {
    pub fn new(name: &str) -> Result<api::symbology::Route> {
        api::symbology::Route::new(name)
    }
}

/// Commonly used compound type
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct Cpty {
    pub venue: VenueRef,
    pub route: RouteRef,
}

impl Cpty {
    pub fn get(s: &str) -> Option<Self> {
        if let Some((vstr, rstr)) = s.split_once('/') {
            let venue = VenueRef::get(vstr)?;
            let route = RouteRef::get(rstr)?;
            Some(Self { venue, route })
        } else {
            None
        }
    }

    pub fn find(s: &str) -> Result<Self> {
        Self::get(s).ok_or_else(|| anyhow!("missing cpty: {}", s))
    }

    pub fn get_by_id(id: &CptyId) -> Option<Self> {
        let venue = VenueRef::get_by_id(&id.venue)?;
        let route = RouteRef::get_by_id(&id.route)?;
        Some(Self { venue, route })
    }

    pub fn find_by_id(id: &CptyId) -> Result<Self> {
        let venue = VenueRef::find_by_id(&id.venue)?;
        let route = RouteRef::find_by_id(&id.route)?;
        Ok(Self { venue, route })
    }

    pub fn id(&self) -> CptyId {
        CptyId { venue: self.venue.id, route: self.route.id }
    }

    pub fn name(&self) -> String {
        format!("{}/{}", self.venue.name, self.route.name)
    }
}

impl fmt::Display for Cpty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.venue.name, self.route.name)
    }
}
