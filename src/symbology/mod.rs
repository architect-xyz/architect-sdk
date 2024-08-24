use arc_swap::ArcSwap;
use once_cell::sync::Lazy;

pub(self) mod allocator;
pub mod client;
pub mod cpty;
pub mod external_client;
pub mod index;
pub mod market;
pub mod product;
pub mod route;
pub mod static_ref;
pub mod txn;
pub mod venue;

pub use cpty::Cpty;
pub use index::MarketIndex;
pub use market::{MarketKind, MarketRef};
pub use product::{ProductKind, ProductRef};
pub use route::RouteRef;
pub use static_ref::StaticRef;
pub use txn::Txn;
pub use venue::VenueRef;

pub static GLOBAL_INDEX: Lazy<ArcSwap<MarketIndex>> =
    Lazy::new(|| ArcSwap::from_pointee(MarketIndex::new()));
