use crate::hcstatic;
use allocator::StaticBumpAllocator;
use anyhow::bail;
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
// pub mod txn;

hcstatic!(Venue, api::symbology::Venue, 64);
hcstatic!(Route, api::symbology::Route, 64);
hcstatic!(Product, api::symbology::Product, 512);
hcstatic!(Market, api::symbology::Market, 512);
