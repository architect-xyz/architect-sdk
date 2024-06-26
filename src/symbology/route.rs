use super::{allocator::StaticBumpAllocator, StaticRef};
use crate::static_ref;
use anyhow::{bail, Result};
use api::{symbology::Symbolic, Str};
use arc_swap::ArcSwap;
use immutable_chunkmap::map::MapL as Map;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::sync::{atomic::AtomicUsize, Arc};

static_ref!(RouteRef, api::symbology::Route, 64);

impl RouteRef {
    pub fn new(name: &str) -> Result<api::symbology::Route> {
        api::symbology::Route::new(name)
    }
}
