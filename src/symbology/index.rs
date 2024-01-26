use super::{
    allocator::AllocatorSnapshot, market::MarketInner, static_ref::StaticRef, Market,
    MarketKind, Product, ProductKind, Route, Venue,
};
use anyhow::{bail, Result};
use api::{
    symbology::{
        market::NormalizedMarketInfo,
        query::{DateQ, Query},
        RouteId, VenueId,
    },
    Str,
};
use chrono::{prelude::*, Duration};
use immutable_chunkmap::{map::MapM as Map, set};

pub type Set<T> = set::Set<T, 16>;

/// This a queryable index of all markets
#[derive(Debug, Clone)]
pub struct MarketIndex {
    all: Set<Market>,
    by_base: Map<Product, Set<Market>>,
    by_base_kind: Map<Str, Set<Market>>,
    by_pool_has: Map<Product, Set<Market>>,
    by_quote: Map<Product, Set<Market>>,
    by_venue: Map<Venue, Set<Market>>,
    by_route: Map<Route, Set<Market>>,
    by_exchange_symbol: Map<Str, Set<Market>>,
    by_underlying: Map<Product, Set<Market>>,
    by_expiration: Map<DateTime<Utc>, Set<Market>>,
    snap: Option<AllocatorSnapshot<MarketInner>>,
}

impl FromIterator<Market> for MarketIndex {
    fn from_iter<T: IntoIterator<Item = Market>>(iter: T) -> Self {
        let mut t = Self::new();
        for p in iter {
            t.insert(p)
        }
        t
    }
}

impl MarketIndex {
    /// create a new empty market index
    pub fn new() -> Self {
        Self {
            all: Set::new(),
            by_base: Map::default(),
            by_base_kind: Map::default(),
            by_quote: Map::default(),
            by_pool_has: Map::default(),
            by_venue: Map::default(),
            by_route: Map::default(),
            by_exchange_symbol: Map::default(),
            by_underlying: Map::default(),
            by_expiration: Map::default(),
            snap: None,
        }
    }

    /// insert a market into the index
    pub fn insert(&mut self, i: Market) {
        fn insert<K: Ord + Clone + Copy + 'static>(
            m: &mut Map<K, Set<Market>>,
            k: K,
            i: Market,
        ) {
            let mut set = m.remove_cow(&k).unwrap_or(Set::new());
            set.insert_cow(i);
            m.insert_cow(k, set);
        }
        self.all.insert_cow(i);
        insert(&mut self.by_venue, i.venue, i);
        insert(&mut self.by_route, i.route, i);
        insert(&mut self.by_exchange_symbol, i.exchange_symbol, i);
        match &i.kind {
            MarketKind::Exchange(x) => {
                insert(&mut self.by_base, x.base, i);
                insert(&mut self.by_quote, x.quote, i);
                insert(
                    &mut self.by_base_kind,
                    Str::try_from(x.base.kind.name()).unwrap(),
                    i,
                );
                match x.base.kind {
                    ProductKind::Coin { .. }
                    | ProductKind::Fiat
                    | ProductKind::Equity
                    | ProductKind::Perpetual => (),
                    ProductKind::Future { underlying, multiplier: _, expiration }
                    | ProductKind::Option { underlying, multiplier: _, expiration } => {
                        if let Some(underlying) = underlying {
                            insert(&mut self.by_underlying, underlying, i);
                        }
                        if let Some(expiration) = expiration {
                            insert(&mut self.by_expiration, expiration, i);
                        }
                    }
                    ProductKind::FutureSpread { same_side_leg, opp_side_leg } => {
                        if let Some(leg) = same_side_leg {
                            insert(&mut self.by_underlying, leg, i);
                        }
                        if let Some(leg) = opp_side_leg {
                            insert(&mut self.by_underlying, leg, i);
                        }
                    }
                    ProductKind::Index
                    | ProductKind::Commodity
                    | ProductKind::Unknown => (),
                }
            }
            MarketKind::Pool(p) => {
                for pr in &p.products {
                    insert(&mut self.by_pool_has, *pr, i);
                }
            }
            MarketKind::Unknown => (),
        }
    }

    /// remove a tradable product from the index
    pub fn remove(&mut self, i: Market) {
        fn remove<K: Ord + Clone + Copy + 'static>(
            m: &mut Map<K, Set<Market>>,
            k: K,
            i: Market,
        ) {
            if let Some(mut set) = m.remove_cow(&k) {
                set.remove_cow(&i);
                if set.len() > 0 {
                    m.insert_cow(k, set);
                }
            }
        }
        self.all.remove_cow(&i);
        remove(&mut self.by_venue, i.venue, i);
        remove(&mut self.by_route, i.route, i);
        remove(&mut self.by_exchange_symbol, i.exchange_symbol, i);
        match &i.kind {
            MarketKind::Exchange(x) => {
                remove(&mut self.by_base, x.base, i);
                remove(&mut self.by_quote, x.quote, i);
                remove(
                    &mut self.by_base_kind,
                    Str::try_from(x.base.kind.name()).unwrap(),
                    i,
                );
                match x.base.kind {
                    ProductKind::Coin { .. }
                    | ProductKind::Fiat
                    | ProductKind::Equity
                    | ProductKind::Perpetual => (),
                    ProductKind::Future { underlying, multiplier: _, expiration }
                    | ProductKind::Option { underlying, multiplier: _, expiration } => {
                        if let Some(underlying) = underlying {
                            remove(&mut self.by_underlying, underlying, i);
                        }
                        if let Some(expiration) = expiration {
                            remove(&mut self.by_expiration, expiration, i);
                        }
                    }
                    ProductKind::FutureSpread { same_side_leg, opp_side_leg } => {
                        if let Some(leg) = same_side_leg {
                            remove(&mut self.by_underlying, leg, i);
                        }
                        if let Some(leg) = opp_side_leg {
                            remove(&mut self.by_underlying, leg, i);
                        }
                    }
                    ProductKind::Index
                    | ProductKind::Commodity
                    | ProductKind::Unknown => (),
                }
            }
            MarketKind::Pool(p) => {
                for pr in &p.products {
                    remove(&mut self.by_pool_has, *pr, i);
                }
            }
            MarketKind::Unknown => (),
        }
    }

    fn query_(&self, q: &Query) -> Set<Market> {
        fn start_of_day(dt: DateTime<Utc>) -> DateTime<Utc> {
            let date = dt.naive_utc().date();
            let ndt = NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
            Utc.from_utc_datetime(&ndt)
        }
        fn end_of_day(dt: DateTime<Utc>) -> DateTime<Utc> {
            let date = dt.naive_utc().date();
            let ndt = NaiveDateTime::new(
                date,
                NaiveTime::from_hms_milli_opt(23, 59, 59, 999).unwrap(),
            );
            Utc.from_utc_datetime(&ndt)
        }
        match q {
            Query::And(terms) => terms
                .iter()
                .fold(self.all.clone(), |acc, term| acc.intersect(&self.query_(term))),
            Query::Or(terms) => {
                terms.iter().fold(Set::new(), |acc, term| acc.union(&self.query_(term)))
            }
            Query::Not(term) => self.all.diff(&self.query_(term)),
            Query::All => self.all.clone(),
            Query::Base(s) => Product::get_by_name_or_id(s).map_or_else(Set::new, |p| {
                self.by_base.get(&p).cloned().unwrap_or_else(Set::new)
            }),
            Query::BaseKind(s) => {
                self.by_base_kind.get(s).cloned().unwrap_or_else(Set::new)
            }
            Query::Quote(s) => Product::get_by_name_or_id(s).map_or_else(Set::new, |p| {
                self.by_quote.get(&p).cloned().unwrap_or_else(Set::new)
            }),
            Query::Pool(s) => Product::get_by_name_or_id(s).map_or_else(Set::new, |p| {
                self.by_pool_has.get(&p).cloned().unwrap_or_else(Set::new)
            }),
            Query::Route(s) => Route::get_by_name_or_id(s).map_or_else(Set::new, |r| {
                self.by_route.get(&r).cloned().unwrap_or_else(Set::new)
            }),
            Query::Venue(s) => Venue::get_by_name_or_id(s).map_or_else(Set::new, |v| {
                self.by_venue.get(&v).cloned().unwrap_or_else(Set::new)
            }),
            Query::ExchangeSymbol(s) => {
                self.by_exchange_symbol.get(s).cloned().unwrap_or_else(Set::new)
            }
            Query::Underlying(s) => Product::get_by_name_or_id(s)
                .map_or_else(Set::new, |p| {
                    self.by_underlying.get(&p).cloned().unwrap_or_else(Set::new)
                }),
            Query::Expired => {
                let now = Utc::now();
                let res: Set<Market> = self
                    .all
                    .into_iter()
                    .filter(|t| {
                        if let Some(exp) = match t.kind {
                            MarketKind::Exchange(ref x) => match x.base.kind {
                                ProductKind::Future { expiration, .. }
                                | ProductKind::Option { expiration, .. } => {
                                    Some(expiration)
                                }
                                _ => None,
                            },
                            _ => None,
                        } {
                            match exp {
                                None => return false,
                                Some(exp) => {
                                    if exp < now - Duration::hours(8) {
                                        return false;
                                    }
                                }
                            }
                        }
                        !t.extra_info.is_delisted()
                    })
                    .map(|t| *t)
                    .collect();
                res
            }
            Query::Expiration(dq) => match dq {
                DateQ::On(dt) => {
                    let date = dt.naive_utc().date();
                    self.by_expiration
                        .range(start_of_day(*dt)..)
                        .take_while(|(k, _)| k.naive_utc().date() == date)
                        .fold(Set::new(), |acc, (_, s)| acc.union(s))
                }
                DateQ::OnOrAfter(dt) => self
                    .by_expiration
                    .range(start_of_day(*dt)..)
                    .fold(Set::new(), |acc, (_, s)| acc.union(s)),
                DateQ::OnOrBefore(dt) => self
                    .by_expiration
                    .range(..=end_of_day(*dt))
                    .fold(Set::new(), |acc, (_, s)| acc.union(s)),
                DateQ::Between(st, en) => self
                    .by_expiration
                    .range(start_of_day(*st)..=end_of_day(*en))
                    .fold(Set::new(), |acc, (_, s)| acc.union(s)),
            },
        }
    }

    /// index all the markets that were added to the symbology since the index
    /// was last updated. If none were added this is very quick. If the index
    /// has never been updated this will index all tradable products
    pub fn update(&mut self) {
        let snap = Market::allocator_snapshot(self.snap, |p| self.insert(p));
        self.snap = Some(snap);
    }

    /// Query the index, returning the set of all tradable products
    /// that match the query.
    pub fn query(&self, q: &Query) -> Set<Market> {
        self.query_(q)
    }

    /// This is the same as calling update followed by query
    pub fn update_and_query(&mut self, q: &Query) -> Set<Market> {
        self.update();
        self.query(q)
    }

    /// Return all markets in the index
    pub fn all(&self) -> Set<Market> {
        self.all.clone()
    }

    pub fn find_exactly_one_by_exchange_symbol(
        &self,
        venue: VenueId,
        route: RouteId,
        exchange_symbol: Str,
    ) -> Result<Market> {
        let res = self
            .by_exchange_symbol
            .get(&exchange_symbol)
            .cloned()
            .unwrap_or_else(Set::new);
        let mut iter =
            res.into_iter().filter(|m| m.venue.id == venue && m.route.id == route);
        let first = iter.next();
        if first.is_none() {
            bail!(
                "no market with exchange symbol {} for venue {} and route {}",
                exchange_symbol.as_str(),
                venue,
                route
            )
        } else if iter.next().is_some() {
            bail!(
                "more than one market with exchange symbol {} for venue {} and route {}",
                exchange_symbol.as_str(),
                venue,
                route
            )
        } else {
            Ok(*first.unwrap())
        }
    }

    pub fn find_exactly_one_by_base_and_quote(
        &self,
        venue: VenueId,
        route: RouteId,
        base: Product,
        quote: Product,
    ) -> Result<Market> {
        let res = self.by_base.get(&base).cloned().unwrap_or_else(Set::new);
        let mut iter = res.into_iter().filter(|m| {
            m.venue.id == venue
                && m.route.id == route
                && match &m.kind {
                    MarketKind::Exchange(e) => e.quote.id == quote.id,
                    _ => false,
                }
        });
        let first = iter.next();
        if first.is_none() {
            bail!(
                "no market with base {} and quote {} for venue {} and route {}",
                base,
                quote,
                venue,
                route
            )
        } else if iter.next().is_some() {
            bail!(
                "more than one market with base {} and quote {} for venue {} and route {}",
                base,
                quote,
                venue,
                route
            )
        } else {
            Ok(*first.unwrap())
        }
    }
}
