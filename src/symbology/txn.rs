use anyhow::{anyhow, bail, Result};
use api::{
    symbology::{MarketId, ProductId, RouteId, VenueId},
    Str,
};
use bytes::{Buf, Bytes, BytesMut};
use fxhash::{FxHashMap, FxHashSet};
use immutable_chunkmap::map::MapL as Map;
use log::{debug, warn};
use netidx::{
    pack::{Pack, PackError},
    pool::Pooled,
};
use parking_lot::{Mutex, MutexGuard};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{
    borrow::Borrow, cell::RefCell, collections::BTreeMap, fmt, ops::Deref, sync::Arc,
};
use uuid::Uuid;

static TXN_LOCK: Mutex<()> = Mutex::new(());

/// A symbology update transaction. This is not a traditional ACID
/// transaction. Specifically dropping a Txn after making changes to
/// products or tradable products that exist in the global symbology
/// will not undo those changes.
pub struct Txn {
    venue_by_name: Arc<Map<Str, Venue>>,
    venue_by_id: Arc<Map<VenueId, Venue>>,
    route_by_name: Arc<Map<Str, Route>>,
    route_by_id: Arc<Map<RouteId, Route>>,
    product_by_name: Arc<Map<Str, Product>>,
    product_by_id: Arc<Map<ProductId, Product>>,
    market_by_name: Arc<Map<Str, Market>>,
    market_by_id: Arc<Map<MarketId, Market>>,
}

impl Drop for Txn {
    fn drop(&mut self) {
        unsafe { TXN_LOCK.force_unlock() }
    }
}

impl Txn {
    /// Commit the transaction to the symbology. This will replace the
    /// global symbology with whatever is in the transaction, so keep
    /// that in mind if you started from empty.
    pub fn commit(self) {
        let Self {
            venue_by_name,
            venue_by_id,
            route_by_name,
            route_by_id,
            product_by_name,
            product_by_id,
            market_by_name,
            market_by_id,
        } = &self;
        VENUE_BY_NAME.store(Arc::clone(venue_by_name));
        VENUE_BY_ID.store(Arc::clone(venue_by_id));
        ROUTE_BY_NAME.store(Arc::clone(route_by_name));
        ROUTE_BY_ID.store(Arc::clone(route_by_id));
        QUOTE_SYMBOL_BY_NAME.store(Arc::clone(quote_symbol_by_name));
        QUOTE_SYMBOL_BY_ID.store(Arc::clone(quote_symbol_by_id));
        TRADING_SYMBOL_BY_NAME.store(Arc::clone(trading_symbol_by_name));
        TRADING_SYMBOL_BY_ID.store(Arc::clone(trading_symbol_by_id));
        PRODUCT_BY_NAME.store(Arc::clone(product_by_name));
        PRODUCT_BY_ID.store(Arc::clone(product_by_id));
        TRADABLE_PRODUCT.store(Arc::clone(tradable_product_by_name));
        TRADABLE_PRODUCT_BY_ID.store(Arc::clone(tradable_product_by_id));
    }

    /// Add the symbology in the transaction to the global symbology,
    /// but don't remove anything from the global symbology. If
    /// something exists in both places the transaction version will
    /// overwrite the global version.
    pub fn merge(self) {
        let Self {
            venue_by_name,
            venue_by_id,
            route_by_name,
            route_by_id,
            quote_symbol_by_name,
            quote_symbol_by_id,
            trading_symbol_by_name,
            trading_symbol_by_id,
            product_by_name,
            product_by_id,
            tradable_product_by_name,
            tradable_product_by_id,
        } = &self;
        VENUE_BY_NAME.store(Arc::new(
            (**VENUE_BY_NAME.load()).union(venue_by_name, |_, _, v| Some(*v)),
        ));
        VENUE_BY_ID.store(Arc::new(
            (**VENUE_BY_ID.load()).union(venue_by_id, |_, _, v| Some(*v)),
        ));
        ROUTE_BY_NAME.store(Arc::new(
            (**ROUTE_BY_NAME.load()).union(route_by_name, |_, _, v| Some(*v)),
        ));
        ROUTE_BY_ID.store(Arc::new(
            (**ROUTE_BY_ID.load()).union(route_by_id, |_, _, v| Some(*v)),
        ));
        QUOTE_SYMBOL_BY_NAME.store(Arc::new(
            (**QUOTE_SYMBOL_BY_NAME.load())
                .union(quote_symbol_by_name, |_, _, v| Some(*v)),
        ));
        QUOTE_SYMBOL_BY_ID.store(Arc::new(
            (**QUOTE_SYMBOL_BY_ID.load()).union(quote_symbol_by_id, |_, _, v| Some(*v)),
        ));
        TRADING_SYMBOL_BY_NAME.store(Arc::new(
            (**TRADING_SYMBOL_BY_NAME.load())
                .union(trading_symbol_by_name, |_, _, v| Some(*v)),
        ));
        TRADING_SYMBOL_BY_ID.store(Arc::new(
            (**TRADING_SYMBOL_BY_ID.load())
                .union(trading_symbol_by_id, |_, _, v| Some(*v)),
        ));
        PRODUCT_BY_NAME.store(Arc::new(
            (**PRODUCT_BY_NAME.load()).union(product_by_name, |_, _, v| Some(*v)),
        ));
        PRODUCT_BY_ID.store(Arc::new(
            (**PRODUCT_BY_ID.load()).union(product_by_id, |_, _, v| Some(*v)),
        ));
        TRADABLE_PRODUCT.store(Arc::new(
            (**TRADABLE_PRODUCT.load())
                .union(tradable_product_by_name, |_, _, v| Some(*v)),
        ));
        TRADABLE_PRODUCT_BY_ID.store(Arc::new(
            (**TRADABLE_PRODUCT_BY_ID.load())
                .union(tradable_product_by_id, |_, _, v| Some(*v)),
        ));
    }

    fn begin_inner(lock: MutexGuard<'static, ()>) -> Self {
        MutexGuard::leak(lock);
        // we must do this so the Txn remains Send and Sync
        Self {
            venue_by_name: VENUE_BY_NAME.load_full(),
            venue_by_id: VENUE_BY_ID.load_full(),
            route_by_name: ROUTE_BY_NAME.load_full(),
            route_by_id: ROUTE_BY_ID.load_full(),
            quote_symbol_by_name: QUOTE_SYMBOL_BY_NAME.load_full(),
            quote_symbol_by_id: QUOTE_SYMBOL_BY_ID.load_full(),
            trading_symbol_by_name: TRADING_SYMBOL_BY_NAME.load_full(),
            trading_symbol_by_id: TRADING_SYMBOL_BY_ID.load_full(),
            product_by_name: PRODUCT_BY_NAME.load_full(),
            product_by_id: PRODUCT_BY_ID.load_full(),
            tradable_product_by_name: TRADABLE_PRODUCT.load_full(),
            tradable_product_by_id: TRADABLE_PRODUCT_BY_ID.load_full(),
        }
    }

    /// Start a new transaction based on the current global
    /// symbology. Once the transaction starts, the symbology can't
    /// change underneath you because only one transaction may be
    /// active at a time. `begin` will block until there are no other
    /// transactions outstanding. If you want to avoid blocking use
    /// try_begin.
    pub fn begin() -> Self {
        Self::begin_inner(TXN_LOCK.lock())
    }

    /// Same as begin, but if a transaction is already in progress
    /// return None instead of blocking.
    pub fn try_begin() -> Option<Self> {
        TXN_LOCK.try_lock().map(Self::begin_inner)
    }

    fn empty_inner(lock: MutexGuard<'static, ()>) -> Self {
        MutexGuard::leak(lock);
        Self {
            venue_by_name: Arc::new(Map::new()),
            venue_by_id: Arc::new(Map::new()),
            route_by_name: Arc::new(Map::new()),
            route_by_id: Arc::new(Map::new()),
            quote_symbol_by_name: Arc::new(Map::new()),
            quote_symbol_by_id: Arc::new(Map::new()),
            trading_symbol_by_name: Arc::new(Map::new()),
            trading_symbol_by_id: Arc::new(Map::new()),
            product_by_name: Arc::new(Map::new()),
            product_by_id: Arc::new(Map::new()),
            tradable_product_by_name: Arc::new(Map::new()),
            tradable_product_by_id: Arc::new(Map::new()),
        }
    }

    /// Begin an empty transaction. Normally transactions snapshot the
    /// current state of the global symbology, however in some cases
    /// (like symbology loaders) you want to start from an empty
    /// universe regardless of what is already loaded globally.
    pub fn empty() -> Self {
        Self::empty_inner(TXN_LOCK.lock())
    }

    /// Same as empty, but doesn't wait if it can't get the lock.
    pub fn try_empty() -> Option<Self> {
        TXN_LOCK.try_lock().map(Self::empty_inner)
    }
}
