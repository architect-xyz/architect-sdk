use super::{
    static_ref::StaticRef, MarketKind, MarketRef, ProductKind, ProductRef, RouteRef,
    VenueRef,
};
use anyhow::{bail, Result};
use api::{
    symbology::{
        query::{DateQ, Query},
        Symbolic,
    },
    Str,
};
use chrono::prelude::*;
use immutable_chunkmap::{map::MapM as Map, set};
use std::sync::Arc;

pub type Set<T> = set::Set<T, 16>;

/// This a queryable index of all markets
#[derive(Debug, Clone)]
pub struct MarketIndex {
    all: Set<MarketRef>,
    // track which products/markets reference which products,
    // so we can update products/markets when products update
    pub(super) by_pointee_p: Map<ProductRef, set::SetM<ProductRef>>,
    pub(super) by_pointee_m: Map<ProductRef, set::SetM<MarketRef>>,
    by_base: Map<ProductRef, Set<MarketRef>>,
    by_base_kind: Map<Str, Set<MarketRef>>,
    by_pool_has: Map<ProductRef, Set<MarketRef>>,
    by_quote: Map<ProductRef, Set<MarketRef>>,
    by_venue: Map<VenueRef, Set<MarketRef>>,
    by_route: Map<RouteRef, Set<MarketRef>>,
    by_exchange_symbol: Map<Str, Set<MarketRef>>,
    by_underlying: Map<ProductRef, Set<MarketRef>>,
    by_expiration: Map<DateTime<Utc>, Set<MarketRef>>,
}

impl FromIterator<MarketRef> for MarketIndex {
    fn from_iter<T: IntoIterator<Item = MarketRef>>(iter: T) -> Self {
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
            by_pointee_p: Map::default(),
            by_pointee_m: Map::default(),
            by_base: Map::default(),
            by_base_kind: Map::default(),
            by_quote: Map::default(),
            by_pool_has: Map::default(),
            by_venue: Map::default(),
            by_route: Map::default(),
            by_exchange_symbol: Map::default(),
            by_underlying: Map::default(),
            by_expiration: Map::default(),
        }
    }

    /// easy access to global market index
    pub fn current() -> arc_swap::Guard<Arc<MarketIndex>> {
        super::GLOBAL_INDEX.load()
    }

    /// insert a market into the index
    pub fn insert(&mut self, i: MarketRef) {
        fn insert<K: Ord + Clone + Copy + 'static>(
            m: &mut Map<K, Set<MarketRef>>,
            k: K,
            i: MarketRef,
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
                    ProductKind::Perpetual {
                        underlying,
                        multiplier: _,
                        instrument_type: _,
                    } => {
                        if let Some(underlying) = underlying {
                            insert(&mut self.by_underlying, underlying, i);
                        }
                    }
                    ProductKind::Future {
                        underlying,
                        multiplier: _,
                        expiration,
                        instrument_type: _,
                    }
                    | ProductKind::Option {
                        underlying,
                        multiplier: _,
                        expiration,
                        instrument_type: _,
                    } => {
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
                    ProductKind::Coin { .. }
                    | ProductKind::Fiat
                    | ProductKind::Equity
                    | ProductKind::Index
                    | ProductKind::Commodity
                    | ProductKind::EventSeries
                    | ProductKind::Event { .. }
                    | ProductKind::EventOutcome { .. }
                    | ProductKind::EventContract { .. }
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
        // insert references
        i.iter_references(|r| {
            self.by_pointee_m.get_or_default_cow(r).insert_cow(i);
        });
    }

    /// remove a market from the index
    pub fn remove(&mut self, i: &MarketRef) {
        fn remove<K: Ord + Clone + Copy + 'static>(
            m: &mut Map<K, Set<MarketRef>>,
            k: K,
            i: &MarketRef,
        ) {
            if let Some(mut set) = m.remove_cow(&k) {
                set.remove_cow(i);
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
                    ProductKind::Perpetual {
                        underlying,
                        multiplier: _,
                        instrument_type: _,
                    } => {
                        if let Some(underlying) = underlying {
                            remove(&mut self.by_underlying, underlying, i);
                        }
                    }
                    ProductKind::Future {
                        underlying,
                        multiplier: _,
                        expiration,
                        instrument_type: _,
                    }
                    | ProductKind::Option {
                        underlying,
                        multiplier: _,
                        expiration,
                        instrument_type: _,
                    } => {
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
                    ProductKind::Coin { .. }
                    | ProductKind::Fiat
                    | ProductKind::Equity
                    | ProductKind::Index
                    | ProductKind::Commodity
                    | ProductKind::EventSeries
                    | ProductKind::Event { .. }
                    | ProductKind::EventOutcome { .. }
                    | ProductKind::EventContract { .. }
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
        // remove references
        i.iter_references(|r| {
            if let Some(m) = self.by_pointee_m.get_mut_cow(&r) {
                m.remove_cow(i);
            }
        });
    }

    pub(super) fn remove_product(&mut self, p: &ProductRef) {
        self.by_base.remove_cow(&p);
        self.by_pool_has.remove_cow(&p);
        self.by_quote.remove_cow(&p);
        self.by_underlying.remove_cow(&p);
    }

    fn query_(&self, q: &Query) -> Set<MarketRef> {
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
            Query::Regex(s) => {
                // CR alee: consider making this function fallible
                let re = regex::Regex::new(s.as_str()).unwrap();
                self.all
                    .into_iter()
                    .filter(|t| re.is_match(t.name().as_str()))
                    .map(|t| *t)
                    .collect()
            }
            Query::Base(s) => ProductRef::get_by_name_or_id(s)
                .map_or_else(Set::new, |p| {
                    self.by_base.get(&p).cloned().unwrap_or_else(Set::new)
                }),
            Query::BaseKind(s) => {
                self.by_base_kind.get(s).cloned().unwrap_or_else(Set::new)
            }
            Query::Quote(s) => ProductRef::get_by_name_or_id(s)
                .map_or_else(Set::new, |p| {
                    self.by_quote.get(&p).cloned().unwrap_or_else(Set::new)
                }),
            Query::Pool(s) => ProductRef::get_by_name_or_id(s)
                .map_or_else(Set::new, |p| {
                    self.by_pool_has.get(&p).cloned().unwrap_or_else(Set::new)
                }),
            Query::Route(s) => RouteRef::get_by_name_or_id(s)
                .map_or_else(Set::new, |r| {
                    self.by_route.get(&r).cloned().unwrap_or_else(Set::new)
                }),
            Query::Venue(s) => VenueRef::get_by_name_or_id(s)
                .map_or_else(Set::new, |v| {
                    self.by_venue.get(&v).cloned().unwrap_or_else(Set::new)
                }),
            Query::ExchangeSymbol(s) => {
                self.by_exchange_symbol.get(s).cloned().unwrap_or_else(Set::new)
            }
            Query::Underlying(s) => ProductRef::get_by_name_or_id(s)
                .map_or_else(Set::new, |p| {
                    self.by_underlying.get(&p).cloned().unwrap_or_else(Set::new)
                }),
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

    /// Query the index, returning the set of all tradable products
    /// that match the query.
    pub fn query(&self, q: &Query) -> Set<MarketRef> {
        self.query_(q)
    }

    /// Return all markets in the index
    pub fn all(&self) -> Set<MarketRef> {
        self.all.clone()
    }

    pub fn find_exactly_one_by_exchange_symbol<S: AsRef<str> + Ord>(
        &self,
        venue: VenueRef,
        route: RouteRef,
        exchange_symbol: S,
    ) -> Result<MarketRef> {
        let res = self
            .by_exchange_symbol
            .get(exchange_symbol.as_ref())
            .cloned()
            .unwrap_or_else(Set::new);
        let mut iter = res.into_iter().filter(|m| m.venue == venue && m.route == route);
        let first = iter.next();
        if first.is_none() {
            bail!(
                "no market with exchange symbol {} for venue {} and route {}",
                exchange_symbol.as_ref(),
                venue,
                route
            )
        } else if iter.next().is_some() {
            bail!(
                "more than one market with exchange symbol {} for venue {} and route {}",
                exchange_symbol.as_ref(),
                venue,
                route
            )
        } else {
            Ok(*first.unwrap())
        }
    }

    pub fn find_exactly_one_by_base_and_quote(
        &self,
        venue: VenueRef,
        route: RouteRef,
        base: ProductRef,
        quote: ProductRef,
    ) -> Result<MarketRef> {
        let res = self.by_base.get(&base).cloned().unwrap_or_else(Set::new);
        let mut iter = res.into_iter().filter(|m| {
            m.venue == venue
                && m.route == route
                && match &m.kind {
                    MarketKind::Exchange(e) => e.quote == quote,
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
