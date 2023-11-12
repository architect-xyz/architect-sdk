use crate::symbology::Market;
use api::{
    pool,
    qf::{Snapshot, Update, Updates},
    Dir, DirPair,
};
use chrono::prelude::*;
use fxhash::{FxHashMap, FxHashSet};
use itertools::Itertools;
use netidx_core::pool::Pooled;
use rust_decimal::Decimal;
use std::{
    collections::{btree_map::Iter, BTreeMap},
    iter::Rev,
    ops::{Deref, DerefMut},
};

#[derive(Clone, Debug)]
pub struct ConsolidatedLevel {
    pub total: Decimal,
    pub sizes: FxHashMap<Market, Decimal>,
}

impl ConsolidatedLevel {
    pub fn new() -> Self {
        Self { total: Decimal::ZERO, sizes: FxHashMap::default() }
    }
}

pub enum ConsolidatedLevelIterator<'a> {
    Forward(Iter<'a, Decimal, ConsolidatedLevel>),
    Reverse(Rev<Iter<'a, Decimal, ConsolidatedLevel>>),
}

impl<'a> Iterator for ConsolidatedLevelIterator<'a> {
    type Item = (&'a Decimal, &'a ConsolidatedLevel);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ConsolidatedLevelIterator::Forward(i) => i.next(),
            ConsolidatedLevelIterator::Reverse(i) => i.next(),
        }
    }
}

#[derive(Debug)]
pub struct ConsolidatedLevelBook {
    pub book: DirPair<BTreeMap<Decimal, ConsolidatedLevel>>,
    pub timestamp: DateTime<Utc>,
}

impl Deref for ConsolidatedLevelBook {
    type Target = DirPair<BTreeMap<Decimal, ConsolidatedLevel>>;

    fn deref(&self) -> &Self::Target {
        &self.book
    }
}

impl DerefMut for ConsolidatedLevelBook {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.book
    }
}

impl Default for ConsolidatedLevelBook {
    fn default() -> Self {
        ConsolidatedLevelBook {
            book: DirPair::default(),
            timestamp: DateTime::<Utc>::default(),
        }
    }
}

impl ConsolidatedLevelBook {
    /// clear the order book
    pub fn clear(&mut self) {
        self.buy.clear();
        self.sell.clear();
    }

    fn clear_one_from_dir(&mut self, market: Market, dir: Dir) {
        let mut levels_to_remove: Vec<Decimal> = Vec::new();
        let side = self.get_mut(dir);
        side.iter_mut().for_each(|(price, level)| match level.sizes.remove(&market) {
            None => (),
            Some(size) => {
                level.total -= size;
                if level.total == Decimal::ZERO {
                    levels_to_remove.push(*price);
                }
            }
        });
        levels_to_remove.iter().for_each(|price| {
            side.remove(price);
        });
    }

    pub fn clear_one(&mut self, market: Market) {
        self.clear_one_from_dir(market, Dir::Buy);
        self.clear_one_from_dir(market, Dir::Sell);
    }

    pub fn upsert(&mut self, market: Market, dir: Dir, price: Decimal, size: Decimal) {
        let side = self.get_mut(dir);
        let level = side.entry(price).or_insert_with(|| ConsolidatedLevel::new());
        let existing_size = level.sizes.insert(market, size).unwrap_or(Decimal::ZERO);
        level.total += size - existing_size;
    }

    pub fn remove(&mut self, market: Market, dir: Dir, price: Decimal) {
        let side = self.get_mut(dir);
        let level = side.entry(price).or_insert_with(|| ConsolidatedLevel::new());
        let existing_size = level.sizes.remove(&market).unwrap_or(Decimal::ZERO);
        level.total -= existing_size;
        if level.total == Decimal::ZERO {
            side.remove(&price);
        }
    }

    pub(super) fn update_from_snapshot(
        &mut self,
        market: Market,
        mut snapshot: Snapshot,
    ) {
        self.clear_one(market);
        for (price, size) in snapshot.book.buy.drain(..) {
            self.upsert(market, Dir::Buy, price, size);
        }
        for (price, size) in snapshot.book.sell.drain(..) {
            self.upsert(market, Dir::Sell, price, size);
        }
    }

    pub(super) fn update(&mut self, market: Market, mut updates: Updates) {
        for up in updates.book.buy.drain(..) {
            match up {
                Update::Change { price, size } => {
                    self.upsert(market, Dir::Buy, price, size);
                }
                Update::Remove { price } => {
                    self.remove(market, Dir::Buy, price);
                }
            }
        }
        for up in updates.book.sell.drain(..) {
            match up {
                Update::Change { price, size } => {
                    self.upsert(market, Dir::Sell, price, size);
                }
                Update::Remove { price } => {
                    self.remove(market, Dir::Sell, price);
                }
            }
        }
    }

    /// return the best price and quantity given a direction
    pub fn best(&self, dir: Dir) -> Option<(Decimal, &ConsolidatedLevel)> {
        match dir {
            Dir::Buy => match self.buy.iter().next_back() {
                None => return None,
                Some((price, level)) => return Some((*price, level)),
            },
            Dir::Sell => match self.sell.iter().next() {
                None => return None,
                Some((price, level)) => return Some((*price, level)),
            },
        }
    }

    /// return an iterator traversing the levels in best price order
    /// for a given direction
    pub fn iter_levels(&self, dir: Dir) -> ConsolidatedLevelIterator {
        match dir {
            Dir::Buy => ConsolidatedLevelIterator::Reverse(self.buy.iter().rev()),
            Dir::Sell => ConsolidatedLevelIterator::Forward(self.sell.iter()),
        }
    }

    /// Condense the order book grouping prices by `precision` and
    /// summing the size of condensed levels. Output a maximum of
    /// `num_levels` condensed levels from the top of the book
    pub fn condense(
        &self,
        num_levels: usize,
        precision: Decimal,
    ) -> DirPair<Pooled<Vec<CondensedLevel>>> {
        pool!(pool_levels, Vec<CondensedLevel>, 1000, 100);
        let mut dst = DirPair { buy: pool_levels().take(), sell: pool_levels().take() };
        condense_from_levels(
            num_levels,
            &mut dst,
            self.buy.iter().rev(),
            precision,
            Dir::Buy,
        );
        condense_from_levels(
            num_levels,
            &mut dst,
            self.sell.iter(),
            precision,
            Dir::Sell,
        );
        dst
    }
}

pub struct CondensedLevel {
    /// price of this level grouped by precision
    pub price: Decimal,
    /// size at this level
    pub size: Decimal,
    /// total size available at this level and all the levels above
    pub total: Decimal,
    /// tradable products in this level
    pub markets: Pooled<Vec<Market>>,
}

fn condense_from_levels<'a>(
    num_levels: usize,
    dst: &mut DirPair<Pooled<Vec<CondensedLevel>>>,
    levels: impl Iterator<Item = (&'a Decimal, &'a ConsolidatedLevel)>,
    precision: Decimal,
    dir: Dir,
) {
    pool!(pool_markets, Vec<Market>, 1000, 100);
    let mut total = Decimal::ZERO;
    let group = |price: Decimal| {
        let n = price / precision;
        let n = if dir == Dir::Buy { n.floor() } else { n.ceil() };
        n * precision
    };
    let dst = dst.get_mut(dir);
    dst.clear();
    dst.extend(
        levels
            .map(|(price, level)| (group(*price), level.clone()))
            .group_by(|(p, _)| *p)
            .into_iter()
            .take(num_levels)
            .map(|(price, levels)| {
                let mut size = Decimal::ZERO;
                let mut markets_set: FxHashSet<Market> = FxHashSet::default();
                levels.for_each(|(_, level)| {
                    size += level.total;
                    markets_set.extend(level.sizes.keys());
                });
                let mut markets = pool_markets().take();
                markets.extend(markets_set.iter());
                total += size;
                CondensedLevel { price, size, total, markets }
            }),
    );
}
