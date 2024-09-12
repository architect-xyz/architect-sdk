use super::{
    market::*, product::*, route::*, venue::*, MarketIndex, StaticRef, GLOBAL_INDEX,
};
use anyhow::{anyhow, bail, Result};
use api::{
    pool,
    symbology::{MarketId, ProductId, RouteId, SymbologyUpdateKind, VenueId},
    Str,
};
use bytes::{Buf, Bytes, BytesMut};
use fxhash::FxHashSet;
use immutable_chunkmap::{map::MapL as Map, set};
use log::warn;
use md5::{Digest, Md5};
use netidx::{pack::Pack, pool::Pooled};
use parking_lot::{Mutex, MutexGuard};
use smallvec::SmallVec;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
};

static TXN_LOCK: Mutex<()> = Mutex::new(());

/// A symbology update transaction.
pub struct Txn {
    venue_by_name: Arc<Map<Str, VenueRef>>,
    venue_by_id: Arc<Map<VenueId, VenueRef>>,
    route_by_name: Arc<Map<Str, RouteRef>>,
    route_by_id: Arc<Map<RouteId, RouteRef>>,
    product_by_name: Arc<Map<Str, ProductRef>>,
    product_by_id: Arc<Map<ProductId, ProductRef>>,
    market_by_name: Arc<Map<Str, MarketRef>>,
    market_by_id: Arc<Map<MarketId, MarketRef>>,
    index: Arc<MarketIndex>,
    corrupted: bool,
    num_products_added: usize,
    num_markets_added: usize,
    product_ref_count_at_begin: usize,
    market_ref_count_at_begin: usize,
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
    ///
    /// If the transaction was corrupted, does not commit and instead
    /// returns error.
    pub fn commit(self) -> Result<()> {
        let Self {
            venue_by_name,
            venue_by_id,
            route_by_name,
            route_by_id,
            product_by_name,
            product_by_id,
            market_by_name,
            market_by_id,
            index,
            corrupted,
            num_products_added,
            num_markets_added,
            product_ref_count_at_begin,
            market_ref_count_at_begin,
        } = &self;
        if *corrupted {
            bail!("corrupted symbology transaction--cannot commit");
        }
        let product_ref_count = (*PRODUCT_REF_COUNT).load(Ordering::SeqCst);
        let market_ref_count = (*MARKET_REF_COUNT).load(Ordering::SeqCst);
        let product_refs_allocated = product_ref_count - *product_ref_count_at_begin;
        let market_refs_allocated = market_ref_count - *market_ref_count_at_begin;
        if *num_products_added > 10 && product_refs_allocated >= 2 * *num_products_added {
            warn!(
                "large symbology update cascade: allocated {} product refs for {} products",
                product_refs_allocated, num_products_added
            );
        }
        if *num_markets_added > 10 && market_refs_allocated >= 2 * *num_markets_added {
            warn!(
                "large symbology update cascade: allocated {} market refs for {} markets",
                market_refs_allocated, num_markets_added
            );
        }
        VENUE_REF_BY_NAME.store(Arc::clone(venue_by_name));
        VENUE_REF_BY_ID.store(Arc::clone(venue_by_id));
        ROUTE_REF_BY_NAME.store(Arc::clone(route_by_name));
        ROUTE_REF_BY_ID.store(Arc::clone(route_by_id));
        PRODUCT_REF_BY_NAME.store(Arc::clone(product_by_name));
        PRODUCT_REF_BY_ID.store(Arc::clone(product_by_id));
        MARKET_REF_BY_NAME.store(Arc::clone(market_by_name));
        MARKET_REF_BY_ID.store(Arc::clone(market_by_id));
        GLOBAL_INDEX.store(Arc::clone(index));
        Ok(())
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

    fn begin_inner(lock: MutexGuard<'static, ()>) -> Self {
        MutexGuard::leak(lock);
        // we must do this so the Txn remains Send and Sync
        Self {
            venue_by_name: VENUE_REF_BY_NAME.load_full(),
            venue_by_id: VENUE_REF_BY_ID.load_full(),
            route_by_name: ROUTE_REF_BY_NAME.load_full(),
            route_by_id: ROUTE_REF_BY_ID.load_full(),
            product_by_name: PRODUCT_REF_BY_NAME.load_full(),
            product_by_id: PRODUCT_REF_BY_ID.load_full(),
            market_by_name: MARKET_REF_BY_NAME.load_full(),
            market_by_id: MARKET_REF_BY_ID.load_full(),
            index: GLOBAL_INDEX.load_full(),
            corrupted: false,
            num_products_added: 0,
            num_markets_added: 0,
            product_ref_count_at_begin: (*PRODUCT_REF_COUNT).load(Ordering::SeqCst),
            market_ref_count_at_begin: (*MARKET_REF_COUNT).load(Ordering::SeqCst),
        }
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
            index: Arc::new(MarketIndex::new()),
            corrupted: false,
            num_products_added: 0,
            num_markets_added: 0,
            product_ref_count_at_begin: 0,
            market_ref_count_at_begin: 0,
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

    pub fn get_route_by_id(&self, id: &RouteId) -> Option<RouteRef> {
        self.route_by_id.get(id).copied()
    }

    pub fn get_venue_by_id(&self, id: &VenueId) -> Option<VenueRef> {
        self.venue_by_id.get(id).copied()
    }

    pub fn get_product_by_id(&self, id: &ProductId) -> Option<ProductRef> {
        self.product_by_id.get(id).copied()
    }

    pub fn get_market_by_id(&self, id: &MarketId) -> Option<MarketRef> {
        self.market_by_id.get(id).copied()
    }

    pub fn find_route_by_id(&self, id: &RouteId) -> Result<RouteRef> {
        self.get_route_by_id(id).ok_or_else(|| anyhow!("no such route"))
    }

    pub fn find_venue_by_id(&self, id: &VenueId) -> Result<VenueRef> {
        self.get_venue_by_id(id).ok_or_else(|| anyhow!("no such venue"))
    }

    pub fn find_product_by_id(&self, id: &ProductId) -> Result<ProductRef> {
        self.get_product_by_id(id).ok_or_else(|| anyhow!("no such product"))
    }

    pub fn find_market_by_id(&self, id: &MarketId) -> Result<MarketRef> {
        self.get_market_by_id(id).ok_or_else(|| anyhow!("no such market"))
    }

    pub fn add_route(&mut self, route: api::symbology::Route) -> Result<RouteRef> {
        RouteRef::insert(
            Arc::make_mut(&mut self.route_by_name),
            Arc::make_mut(&mut self.route_by_id),
            route,
            true,
        )
    }

    pub fn remove_route(&mut self, route: &RouteId) -> Result<()> {
        let route = self.find_route_by_id(route)?;
        Ok(route.remove(
            Arc::make_mut(&mut self.route_by_name),
            Arc::make_mut(&mut self.route_by_id),
        ))
    }

    pub fn add_venue(&mut self, venue: api::symbology::Venue) -> Result<VenueRef> {
        VenueRef::insert(
            Arc::make_mut(&mut self.venue_by_name),
            Arc::make_mut(&mut self.venue_by_id),
            venue,
            true,
        )
    }

    pub fn remove_venue(&mut self, venue: &VenueId) -> Result<()> {
        let venue = self.find_venue_by_id(venue)?;
        Ok(venue.remove(
            Arc::make_mut(&mut self.venue_by_name),
            Arc::make_mut(&mut self.venue_by_id),
        ))
    }

    pub fn add_product(
        &mut self,
        product: api::symbology::Product,
    ) -> Result<ProductRef> {
        // manually construct the inner ref type, because we are inside a transaction
        // and the TryFrom impl might not know all the refs yet
        let existing = self.get_product_by_id(&product.id);
        let inner = self.hydrate_product_inner(product)?;
        // atomic section--all operations must succeed for txn to be considered uncorrupted
        self.corrupted = true;
        // check if this is equivalent to the existing product, if so, just return it
        let to_update = if let Some(existing) = existing {
            if *existing == inner {
                self.corrupted = false;
                return Ok(existing);
            } else {
                // going to update this product, remove this version from the index
                // and return its referers for update as well; this operation should
                // cascade through all referer products and markets
                let index = Arc::make_mut(&mut self.index);
                let mut to_update_p = set::SetM::new();
                // unrolled BFS over by_pointee_p
                let mut to_chase_p: FxHashSet<ProductRef> = FxHashSet::default();
                to_chase_p.insert(existing);
                while !to_chase_p.is_empty() {
                    let mut next =
                        std::mem::replace(&mut to_chase_p, FxHashSet::default());
                    for cur in next.drain() {
                        to_update_p.insert_cow(cur);
                        let (_, referers) = index.by_pointee_p.remove(&existing);
                        if let Some(referers) = referers {
                            for p in &referers {
                                if !to_update_p.contains(p) {
                                    to_chase_p.insert(*p);
                                }
                            }
                        }
                    }
                }
                let mut to_update_m = set::SetM::new();
                for p in &to_update_p {
                    let (_, referers) = index.by_pointee_m.remove(&p);
                    if let Some(referers) = referers {
                        to_update_m = to_update_m.union(&referers);
                    }
                }
                // remove existing root to avoid double rehydration
                to_update_p.remove_cow(&existing);
                Some((to_update_p, to_update_m))
            }
        } else {
            None
        };
        let product = ProductRef::insert(
            Arc::make_mut(&mut self.product_by_name),
            Arc::make_mut(&mut self.product_by_id),
            inner,
            true,
        )?;
        {
            let index = Arc::make_mut(&mut self.index);
            product.iter_references(|r| {
                index.by_pointee_p.get_or_default_cow(r).insert_cow(product);
            });
        }
        if let Some((to_update_p, to_update_m)) = to_update {
            // update all referer products
            for p in &to_update_p {
                // scrub the index of p, will be added back when markets rehydrate
                Arc::make_mut(&mut self.index).remove_product(p);
                let to_rehydrate: api::symbology::Product = p.into();
                let rehydrated = self.hydrate_product_inner(to_rehydrate)?;
                let product = ProductRef::insert(
                    Arc::make_mut(&mut self.product_by_name),
                    Arc::make_mut(&mut self.product_by_id),
                    rehydrated,
                    true,
                )?;
                let index = Arc::make_mut(&mut self.index);
                product.iter_references(|r| {
                    index.by_pointee_p.get_or_default_cow(r).insert_cow(product);
                });
            }
            // for every referer market, remove the old market, which refers to the
            // old version of the product, and insert a newly hydrated one
            for old_market in &to_update_m {
                Arc::make_mut(&mut self.index).remove(old_market);
                let to_rehydrate: api::symbology::Market = (*old_market).into();
                let rehydrated = self.hydrate_market_inner(to_rehydrate)?;
                let market = MarketRef::insert(
                    Arc::make_mut(&mut self.market_by_name),
                    Arc::make_mut(&mut self.market_by_id),
                    rehydrated,
                    true,
                )?;
                Arc::make_mut(&mut self.index).insert(market);
            }
        }
        self.corrupted = false;
        self.num_products_added += 1;
        Ok(product)
    }

    /// Construct an sdk::ProductInner from its corresponding API type; hydrates pointers
    /// using the current state of the transaction.
    fn hydrate_product_inner(&self, p: api::symbology::Product) -> Result<ProductInner> {
        Ok(ProductInner {
            id: p.id,
            name: p.name,
            kind: self.hydrate_product_kind(p.kind)?,
        })
    }

    /// Construct an sdk::ProductKind from its corresponding API type; hydrates pointers
    /// using the current state of the transaction.
    fn hydrate_product_kind(
        &self,
        kind: api::symbology::ProductKind,
    ) -> Result<ProductKind> {
        use api::symbology::ProductKind as L;
        Ok(match kind {
            L::Coin { token_info: ti } => {
                let mut token_info = BTreeMap::new();
                for (k, v) in ti {
                    let k = self.find_venue_by_id(&k)?;
                    token_info.insert(k, v);
                }
                ProductKind::Coin { token_info }
            }
            L::Fiat => ProductKind::Fiat,
            L::Equity => ProductKind::Equity,
            L::Perpetual { underlying, multiplier, instrument_type } => {
                ProductKind::Perpetual {
                    underlying: match underlying {
                        None => None,
                        Some(underlying) => Some(self.find_product_by_id(&underlying)?),
                    },
                    multiplier,
                    instrument_type,
                }
            }
            L::Future { underlying, multiplier, expiration, instrument_type } => {
                ProductKind::Future {
                    underlying: match underlying {
                        None => None,
                        Some(underlying) => Some(self.find_product_by_id(&underlying)?),
                    },
                    multiplier,
                    expiration,
                    instrument_type,
                }
            }
            L::FutureSpread { same_side_leg, opp_side_leg } => {
                ProductKind::FutureSpread {
                    same_side_leg: match same_side_leg {
                        None => None,
                        Some(id) => Some(self.find_product_by_id(&id)?),
                    },
                    opp_side_leg: match opp_side_leg {
                        None => None,
                        Some(id) => Some(self.find_product_by_id(&id)?),
                    },
                }
            }
            L::Option { underlying, multiplier, expiration, instrument_type } => {
                ProductKind::Option {
                    underlying: match underlying {
                        None => None,
                        Some(underlying) => Some(self.find_product_by_id(&underlying)?),
                    },
                    multiplier,
                    expiration,
                    instrument_type,
                }
            }
            L::Commodity => ProductKind::Commodity,
            L::Index => ProductKind::Index,
            L::EventSeries { display_name } => ProductKind::EventSeries { display_name },
            L::Event { series, outcomes, mutually_exclusive, expiration } => {
                ProductKind::Event {
                    series: match series {
                        None => None,
                        Some(id) => Some(self.find_product_by_id(&id)?),
                    },
                    outcomes: outcomes
                        .iter()
                        .map(|p| {
                            let p = self.find_product_by_id(&p)?;
                            Ok(p)
                        })
                        .collect::<Result<_>>()?,
                    mutually_exclusive,
                    expiration,
                }
            }
            L::EventOutcome { display_order, contracts, display_name } => {
                ProductKind::EventOutcome {
                    display_order,
                    contracts: match contracts {
                        api::symbology::EventContracts::Single { yes, yes_alias } => {
                            EventContracts::Single {
                                yes: self.find_product_by_id(&yes)?,
                                yes_alias: yes_alias.clone(),
                            }
                        }
                        api::symbology::EventContracts::Dual {
                            yes,
                            yes_alias,
                            no,
                            no_alias,
                        } => EventContracts::Dual {
                            yes: self.find_product_by_id(&yes)?,
                            yes_alias: yes_alias.clone(),
                            no: self.find_product_by_id(&no)?,
                            no_alias: no_alias.clone(),
                        },
                    },
                    display_name,
                }
            }
            L::EventContract { expiration } => ProductKind::EventContract { expiration },
            L::Unknown => ProductKind::Unknown,
        })
    }

    pub fn remove_product(&mut self, product: &ProductId) -> Result<()> {
        let product = self.find_product_by_id(product)?;
        Ok(product.remove(
            Arc::make_mut(&mut self.product_by_name),
            Arc::make_mut(&mut self.product_by_id),
        ))
    }

    pub fn add_market(&mut self, market: api::symbology::Market) -> Result<MarketRef> {
        // manually construct the inner ref type, because we are inside a transaction
        // and the TryFrom impl might not know all the refs yet
        let inner = self.hydrate_market_inner(market)?;
        // atomic section--both operations must succeed for txn to be considered uncorrupted
        self.corrupted = true;
        let market = MarketRef::insert(
            Arc::make_mut(&mut self.market_by_name),
            Arc::make_mut(&mut self.market_by_id),
            inner,
            true,
        )?;
        Arc::make_mut(&mut self.index).insert(market.clone());
        self.corrupted = false;
        self.num_markets_added += 1;
        Ok(market)
    }

    /// Construct an sdk::MarketInner from its corresponding API type; hydrates pointers
    /// using the current state of the transaction.
    pub fn hydrate_market_inner(&self, m: api::symbology::Market) -> Result<MarketInner> {
        Ok(MarketInner {
            id: m.id,
            name: m.name,
            kind: self.hydrate_market_kind(m.kind)?,
            venue: self.find_venue_by_id(&m.venue)?,
            route: self.find_route_by_id(&m.route)?,
            exchange_symbol: m.exchange_symbol,
            extra_info: m.extra_info,
        })
    }

    /// Construct an sdk::MarketKind from its corresponding API type; hydrates pointers
    /// using the current state of the transaction.
    pub fn hydrate_market_kind(
        &self,
        mk: api::symbology::MarketKind,
    ) -> Result<MarketKind> {
        Ok(match mk {
            api::symbology::MarketKind::Exchange(x) => {
                MarketKind::Exchange(ExchangeMarketKind {
                    base: self
                        .get_product_by_id(&x.base)
                        .ok_or_else(|| anyhow!("no such base"))?
                        .clone(),
                    quote: self
                        .get_product_by_id(&x.quote)
                        .ok_or_else(|| anyhow!("no such quote"))?
                        .clone(),
                })
            }
            api::symbology::MarketKind::Pool(p) => {
                let mut pool = SmallVec::new();
                for p in &p.products {
                    let p = self
                        .get_product_by_id(&p)
                        .ok_or_else(|| anyhow!("no such product"))?;
                    pool.push(p.clone());
                }
                MarketKind::Pool(PoolMarketKind { products: pool })
            }
            api::symbology::MarketKind::Unknown => MarketKind::Unknown,
        })
    }

    pub fn remove_market(&mut self, market: &MarketId) -> Result<()> {
        let market = self.find_market_by_id(market)?;
        // atomic section--both operations must succeed for txn to be considered uncorrupted
        self.corrupted = true;
        Arc::make_mut(&mut self.index).remove(&market);
        market.remove(
            Arc::make_mut(&mut self.market_by_name),
            Arc::make_mut(&mut self.market_by_id),
        );
        self.corrupted = false;
        Ok(())
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
            SnapshotUnchanged(_md5) => {
                // CR alee: add an option to check md5 hash against current txn dump,
                // for correctness verification; honestly we probably need a better
                // way of keeping a hash of the current state of the symbology,
                // something that can be quickly update on each change; then this
                // check here can just be an Eq;
                //
                // see https://kerkour.com/fast-hashing-algorithms
                // cf collision-resistant not necessarily cryptographic
                Ok(())
            }
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

    fn dump_product(
        &self,
        pset: &mut FxHashSet<ProductRef>,
        updates: &mut Vec<SymbologyUpdateKind>,
        product: &ProductRef,
    ) {
        product.iter_references(|p| {
            if !pset.contains(&p) {
                self.dump_product(pset, updates, &p);
            }
        });
        if !pset.contains(product) {
            pset.insert(*product);
            updates.push(SymbologyUpdateKind::AddProduct(product.into()));
        }
    }

    /// dump the current symbology as of Txn to a series of symbology updates
    pub fn dump(&self) -> Pooled<Vec<SymbologyUpdateKind>> {
        pool!(pool_pset, FxHashSet<ProductRef>, 2, 1_000_000);
        pool!(pool_update, Vec<SymbologyUpdateKind>, 2, 1_000_000);
        let mut pset = pool_pset().take();
        let mut updates = pool_update().take();
        for (_, venue) in &*self.venue_by_id {
            updates.push(SymbologyUpdateKind::AddVenue((**venue).clone()));
        }
        for (_, route) in &*self.route_by_id {
            updates.push(SymbologyUpdateKind::AddRoute((**route).clone()));
        }
        for (_, product) in &*self.product_by_id {
            self.dump_product(&mut pset, &mut updates, &product);
        }
        for (_, market) in &*self.market_by_id {
            updates.push(SymbologyUpdateKind::AddMarket((*market).into()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::symbology::*;
    use api::symbology::{market::TestMarketInfo, MarketInfo};

    /// It's hard to anticipate the behavior of Arc::make_mut under
    /// different scenarios, better to test them explicitly.
    #[test]
    fn test_concurrent_access() -> Result<()> {
        let tmi = TestMarketInfo {
            tick_size: Default::default(),
            step_size: Default::default(),
            is_delisted: false,
        };
        let mut txn = Txn::begin();
        let direct = txn.add_route(RouteRef::new("DIRECT")?)?;
        let test = txn.add_venue(VenueRef::new("TEST")?)?;
        let usd = txn.add_product(ProductRef::new("USD", ProductKind::Fiat)?)?;
        let zar = txn.add_product(ProductRef::new("ZAR", ProductKind::Fiat)?)?;
        txn.add_market(MarketRef::exchange(
            zar,
            usd,
            test,
            direct,
            "USDZAR",
            MarketInfo::Test(tmi.clone()),
        )?)?;
        txn.commit()?;
        // this symbol should now exist
        let _exists = MarketIndex::current()
            .find_exactly_one_by_exchange_symbol(test, direct, "USDZAR")?;
        // hold the Arc<MarketIndex> over the next txn update--what happens?
        let index = MarketIndex::current();
        let mut txn = Txn::begin();
        let chf = txn.add_product(ProductRef::new("CHF", ProductKind::Fiat)?)?;
        txn.add_market(MarketRef::exchange(
            chf,
            usd,
            test,
            direct,
            "USDCHF",
            MarketInfo::Test(tmi.clone()),
        )?)?;
        txn.commit()?;
        let _not_exists =
            index.find_exactly_one_by_exchange_symbol(test, direct, "USDCHF");
        assert!(matches!(_not_exists, Err(_)));
        let updated_index = MarketIndex::current();
        let _exists =
            updated_index.find_exactly_one_by_exchange_symbol(test, direct, "USDCHF")?;
        Ok(())
    }

    /// This should be prevented by other means, but in the worst case we should
    /// ensure that we don't spinlock if there is a circular reference
    #[test]
    fn test_circular_reference() -> Result<()> {
        let mut txn = Txn::begin();
        let p1 = txn.add_product(ProductRef::new("P1", ProductKind::Fiat)?)?;
        let p2 = txn.add_product(ProductRef::new(
            "P2",
            ProductKind::Future {
                underlying: Some(p1),
                multiplier: None,
                expiration: None,
                instrument_type: None,
            },
        )?)?;
        let _p1 = txn.add_product(ProductRef::new(
            "P1",
            ProductKind::Future {
                underlying: Some(p2),
                multiplier: None,
                expiration: None,
                instrument_type: None,
            },
        )?)?;
        txn.commit().unwrap();
        Ok(())
    }
}
