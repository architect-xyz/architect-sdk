use super::{allocator::StaticBumpAllocator, StaticRef};
use crate::static_ref;
use anyhow::{bail, Result};
use api::{symbology::Symbolic, Str};
use arc_swap::ArcSwap;
use immutable_chunkmap::map::MapL as Map;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::sync::{atomic::AtomicUsize, Arc};

static_ref!(VenueRef, api::symbology::Venue, 64);

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
