use super::*;
use anyhow::{anyhow, bail, Result};
use api::{
    pool,
    symbology::{MarketId, ProductId, RouteId, SymbologyUpdateKind, VenueId},
    Str,
};
use bytes::{Buf, Bytes, BytesMut};
use fxhash::FxHashSet;
use immutable_chunkmap::map::MapL as Map;
use log::warn;
use md5::{Digest, Md5};
use netidx::{pack::Pack, pool::Pooled};
use parking_lot::{Mutex, MutexGuard};
use std::{cell::RefCell, sync::Arc};

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
        PRODUCT_BY_NAME.store(Arc::clone(product_by_name));
        PRODUCT_BY_ID.store(Arc::clone(product_by_id));
        MARKET_BY_NAME.store(Arc::clone(market_by_name));
        MARKET_BY_ID.store(Arc::clone(market_by_id));
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
            product_by_name,
            product_by_id,
            market_by_name,
            market_by_id,
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
        PRODUCT_BY_NAME.store(Arc::new(
            (**PRODUCT_BY_NAME.load()).union(product_by_name, |_, _, v| Some(*v)),
        ));
        PRODUCT_BY_ID.store(Arc::new(
            (**PRODUCT_BY_ID.load()).union(product_by_id, |_, _, v| Some(*v)),
        ));
        MARKET_BY_NAME.store(Arc::new(
            (**MARKET_BY_NAME.load()).union(market_by_name, |_, _, v| Some(*v)),
        ));
        MARKET_BY_ID.store(Arc::new(
            (**MARKET_BY_ID.load()).union(market_by_id, |_, _, v| Some(*v)),
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
            product_by_name: PRODUCT_BY_NAME.load_full(),
            product_by_id: PRODUCT_BY_ID.load_full(),
            market_by_name: MARKET_BY_NAME.load_full(),
            market_by_id: MARKET_BY_ID.load_full(),
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
            product_by_name: Arc::new(Map::new()),
            product_by_id: Arc::new(Map::new()),
            market_by_name: Arc::new(Map::new()),
            market_by_id: Arc::new(Map::new()),
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

    pub fn add_route(&mut self, route: api::symbology::Route) -> Result<Route> {
        Route::insert(
            Arc::make_mut(&mut self.route_by_name),
            Arc::make_mut(&mut self.route_by_id),
            route,
            true,
        )
    }

    pub fn remove_route(&mut self, route: &RouteId) -> Result<()> {
        let route = self
            .route_by_id
            .get(route)
            .copied()
            .ok_or_else(|| anyhow!("no such route"))?;
        Ok(route.remove(
            Arc::make_mut(&mut self.route_by_name),
            Arc::make_mut(&mut self.route_by_id),
        ))
    }

    pub fn add_venue(&mut self, venue: api::symbology::Venue) -> Result<Venue> {
        Venue::insert(
            Arc::make_mut(&mut self.venue_by_name),
            Arc::make_mut(&mut self.venue_by_id),
            venue,
            true,
        )
    }

    pub fn remove_venue(&mut self, venue: &VenueId) -> Result<()> {
        let venue = self
            .venue_by_id
            .get(venue)
            .copied()
            .ok_or_else(|| anyhow!("no such venue"))?;
        Ok(venue.remove(
            Arc::make_mut(&mut self.venue_by_name),
            Arc::make_mut(&mut self.venue_by_id),
        ))
    }

    pub fn add_product(&mut self, product: api::symbology::Product) -> Result<Product> {
        Product::insert(
            Arc::make_mut(&mut self.product_by_name),
            Arc::make_mut(&mut self.product_by_id),
            product,
            true,
        )
    }

    pub fn remove_product(&mut self, product: &ProductId) -> Result<()> {
        let product = self
            .product_by_id
            .get(product)
            .copied()
            .ok_or_else(|| anyhow!("no such product"))?;
        Ok(product.remove(
            Arc::make_mut(&mut self.product_by_name),
            Arc::make_mut(&mut self.product_by_id),
        ))
    }

    pub fn add_market(&mut self, market: api::symbology::Market) -> Result<Market> {
        Market::insert(
            Arc::make_mut(&mut self.market_by_name),
            Arc::make_mut(&mut self.market_by_id),
            market,
            true,
        )
    }

    pub fn remove_market(&mut self, market: &MarketId) -> Result<()> {
        let market = self
            .market_by_id
            .get(market)
            .copied()
            .ok_or_else(|| anyhow!("no such market"))?;
        Ok(market.remove(
            Arc::make_mut(&mut self.market_by_name),
            Arc::make_mut(&mut self.market_by_id),
        ))
    }

    /// Updates are idempotent; symbology update replays should be harmless
    pub fn apply(&mut self, up: &SymbologyUpdateKind) -> Result<()> {
        use api::symbology::SymbologyUpdateKind::*;
        match up {
            AddRoute(route) => self.add_route(route.clone()).map(|_| ()),
            RemoveRoute(route) => self.remove_route(route),
            AddVenue(venue) => self.add_venue(venue.clone()).map(|_| ()),
            RemoveVenue(venue) => self.remove_venue(venue),
            AddProduct(product) => self.add_product(product.clone()).map(|_| ()),
            RemoveProduct(product) => self.remove_product(product),
            AddMarket(market) => self.add_market(market.clone()).map(|_| ()),
            RemoveMarket(market) => self.remove_market(market),
            Snapshot { original_length, compressed } => {
                thread_local! {
                    static BUF: RefCell<BytesMut> = RefCell::new(BytesMut::new());
                }
                pool!(pool_updates, Vec<SymbologyUpdateKind>, 10, 100_000);
                let original_length = *original_length;
                if original_length > compressed.len() << 6
                    || original_length < compressed.len()
                {
                    bail!("suspicious looking original length {}", original_length)
                }
                let mut updates = BUF.with(|buf| {
                    let mut buf = buf.borrow_mut();
                    buf.resize(original_length, 0u8);
                    let len =
                        zstd::bulk::decompress_to_buffer(&compressed[..], &mut buf[..])?;
                    if len != original_length {
                        bail!(
                            "unexpected decompressed length {} expected {}",
                            len,
                            original_length
                        )
                    }
                    let mut updates = pool_updates().take();
                    loop {
                        let rem = buf.remaining();
                        if rem == 0 {
                            break Ok::<_, anyhow::Error>(updates)
                        }
                        match Pack::decode(&mut *buf) {
                            Ok(up) => updates.push(up),
                            Err(e) => {
                                // make sure len_wrapped_decode skipped the bad message
                                if buf.remaining() < rem {
                                    warn!("failed to unpack symbology update {:?}, skipping", e);
                                } else {
                                    break Err(e.into())
                                }
                            }
                        }
                    }
                })?;
                for up in updates.drain(..) {
                    if let Err(e) = self.apply(&up) {
                        warn!(
                            "could not apply symbology update from snapshot {:?} {:?}",
                            up, e
                        )
                    }
                }
                Ok(())
            }
            Unknown => Ok(()),
        }
    }

    /// dump the current symbology as of Txn to a series of symbology updates
    pub fn dump(&self) -> Pooled<Vec<SymbologyUpdateKind>> {
        pool!(pool_pset, FxHashSet<Product>, 2, 1_000_000);
        pool!(pool_update, Vec<SymbologyUpdateKind>, 2, 1_000_000);
        let mut updates = pool_update().take();
        for (_, venue) in &*self.venue_by_id {
            updates.push(SymbologyUpdateKind::AddVenue((**venue).clone()));
        }
        for (_, route) in &*self.route_by_id {
            updates.push(SymbologyUpdateKind::AddRoute((**route).clone()));
        }
        for (_, product) in &*self.product_by_id {
            updates.push(SymbologyUpdateKind::AddProduct((**product).clone()));
        }
        for (_, market) in &*self.market_by_id {
            updates.push(SymbologyUpdateKind::AddMarket((**market).clone()));
        }
        updates
    }

    /// dump the symbology db in a squashed form, return the md5 sum and
    /// the squashed update. The squashed update is compressed with zstd.
    pub fn dump_squashed(&self) -> Result<(Bytes, SymbologyUpdateKind)> {
        thread_local! {
            static BUF: RefCell<BytesMut> = RefCell::new(BytesMut::new());
            static COMP: RefCell<Result<zstd::bulk::Compressor<'static>>> = RefCell::new({
                match zstd::bulk::Compressor::new(16) {
                    Err(e) => Err(e.into()),
                    Ok(mut comp) => match comp.multithread(num_cpus::get() as u32) {
                        Err(e) => Err(e.into()),
                        Ok(()) => Ok(comp)
                    }
                }
            });
        }
        let updates = self.dump();
        BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.clear();
            for up in &*updates {
                Pack::encode(up, &mut *buf)?;
            }
            let original = buf.split().freeze();
            buf.resize(original.len(), 0u8);
            let clen = COMP.with(|comp| {
                let mut comp = comp.borrow_mut();
                let comp = comp.as_mut().map_err(|e| anyhow!("{:?}", e))?;
                let len = comp.compress_to_buffer(&original[..], &mut buf[..])?;
                Ok::<_, anyhow::Error>(len)
            })?;
            if clen > u32::MAX as usize {
                bail!("snapshot is too big")
            }
            buf.resize(clen, 0u8);
            let compressed = buf.split().freeze();
            // TODO: not sure what the point of the commented out stuff was
            // it even seems wrong?
            // I guess it was to give a stable thing to md5,
            // but self.dump() is already deterministically ordered pretty sure
            // updates.sort();
            // for up in &*updates {
            //     Pack::encode(up, &mut *buf)?;
            // }
            let mut md5 = Md5::default();
            md5.update(&*buf);
            let md5_hash = md5.finalize();
            buf.clear();
            buf.extend_from_slice(&md5_hash);
            let md5 = buf.split().freeze();
            let up = SymbologyUpdateKind::Snapshot {
                original_length: original.len(),
                compressed,
            };
            Ok((md5, up))
        })
    }
}
