/// Order book representation
use api::{
    pool,
    qf::{Snapshot, Update, Updates},
    Dir, DirPair,
};
use chrono::prelude::*;
use itertools::Itertools;
use netidx::pool::Pooled;
use rust_decimal::Decimal;
use std::{
    collections::{btree_map::Iter, BTreeMap},
    iter::Rev,
    ops::{Deref, DerefMut},
};

pub trait LevelLike {
    fn price(&self) -> Decimal;
    fn size(&self) -> Decimal;
    fn total(&self) -> Option<Decimal>;
}

impl<'a> LevelLike for (&Decimal, &Decimal) {
    fn price(&self) -> Decimal {
        *self.0
    }

    fn size(&self) -> Decimal {
        *self.1
    }

    fn total(&self) -> Option<Decimal> {
        None
    }
}

impl LevelLike for (Decimal, Decimal) {
    fn price(&self) -> Decimal {
        self.0
    }

    fn size(&self) -> Decimal {
        self.1
    }

    fn total(&self) -> Option<Decimal> {
        None
    }
}

impl LevelLike for &(Decimal, Decimal) {
    fn price(&self) -> Decimal {
        self.0
    }

    fn size(&self) -> Decimal {
        self.1
    }

    fn total(&self) -> Option<Decimal> {
        None
    }
}

pub enum LevelIterator<'a> {
    Forward(Iter<'a, Decimal, Decimal>),
    Reverse(Rev<Iter<'a, Decimal, Decimal>>),
}

impl<'a> Iterator for LevelIterator<'a> {
    type Item = (&'a Decimal, &'a Decimal);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            LevelIterator::Forward(i) => i.next(),
            LevelIterator::Reverse(i) => i.next(),
        }
    }
}

/// An order book
#[derive(Debug)]
pub struct LevelBook {
    pub book: DirPair<BTreeMap<Decimal, Decimal>>,
    pub timestamp: DateTime<Utc>,
}

impl Deref for LevelBook {
    type Target = DirPair<BTreeMap<Decimal, Decimal>>;

    fn deref(&self) -> &Self::Target {
        &self.book
    }
}

impl DerefMut for LevelBook {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.book
    }
}

impl Default for LevelBook {
    fn default() -> Self {
        LevelBook { book: DirPair::default(), timestamp: DateTime::<Utc>::default() }
    }
}

impl LevelBook {
    /// clear the order book
    pub fn clear(&mut self) {
        self.buy.clear();
        self.sell.clear();
    }

    pub(super) fn update_from_snapshot(&mut self, mut snapshot: Snapshot) {
        self.buy.clear();
        self.sell.clear();
        for (price, size) in snapshot.book.buy.drain(..) {
            self.buy.insert(price, size);
        }
        for (price, size) in snapshot.book.sell.drain(..) {
            self.sell.insert(price, size);
        }
        self.timestamp = snapshot.timestamp;
    }

    pub(super) fn update(&mut self, mut updates: Updates) {
        for up in updates.book.buy.drain(..) {
            match up {
                Update::Change { price, size } => {
                    self.buy.insert(price, size);
                }
                Update::Remove { price } => {
                    self.buy.remove(&price);
                }
            }
        }
        for up in updates.book.sell.drain(..) {
            match up {
                Update::Change { price, size } => {
                    self.sell.insert(price, size);
                }
                Update::Remove { price } => {
                    self.sell.remove(&price);
                }
            }
        }
        self.timestamp = updates.timestamp;
    }

    /// return the best price and quantity given a direction
    pub fn best(&self, dir: Dir) -> Option<(Decimal, Decimal)> {
        match dir {
            Dir::Buy => match self.buy.iter().next_back() {
                None => return None,
                Some((price, size)) => return Some((*price, *size)),
            },
            Dir::Sell => match self.sell.iter().next() {
                None => return None,
                Some((price, size)) => return Some((*price, *size)),
            },
        }
    }

    /// return an iterator traversing the levels in best price order
    /// for a given direction
    pub fn iter_levels(&self, dir: Dir) -> LevelIterator {
        match dir {
            Dir::Buy => LevelIterator::Reverse(self.buy.iter().rev()),
            Dir::Sell => LevelIterator::Forward(self.sell.iter()),
        }
    }

    /// return true if the book is crossed, false otherwise. Depending
    /// on the exchange a crossed book may mean different things. On
    /// many (most?) exchanges a crossed book would only occurr if
    /// there were an error of some kind.
    pub fn is_crossed(&self) -> bool {
        self.book
            .buy
            .last_key_value()
            .and_then(|(e0, _)| self.book.sell.first_key_value().map(|(e1, _)| (e0, e1)))
            .map(|(e0, e1)| e0 >= e1)
            .unwrap_or(false)
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

#[derive(Debug)]
pub struct CondensedLevel {
    /// price of this level grouped by precision
    pub price: Decimal,
    /// size at this level
    pub size: Decimal,
    /// total size available at this level and all the levels above
    pub total: Decimal,
}

impl LevelLike for CondensedLevel {
    fn price(&self) -> Decimal {
        self.price
    }

    fn size(&self) -> Decimal {
        self.size
    }

    fn total(&self) -> Option<Decimal> {
        Some(self.total)
    }
}

impl<'a> LevelLike for &'a CondensedLevel {
    fn price(&self) -> Decimal {
        self.price
    }

    fn size(&self) -> Decimal {
        self.size
    }

    fn total(&self) -> Option<Decimal> {
        Some(self.total)
    }
}

fn condense_from_levels<'a>(
    num_levels: usize,
    dst: &mut DirPair<Pooled<Vec<CondensedLevel>>>,
    levels: impl Iterator<Item = (&'a Decimal, &'a Decimal)>,
    precision: Decimal,
    dir: Dir,
) {
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
            .map(|(price, size)| (group(*price), *size))
            .group_by(|(p, _)| *p)
            .into_iter()
            .take(num_levels)
            .map(|(price, sizes)| {
                let size = sizes.map(|(_, s)| s).sum();
                total += size;
                CondensedLevel { price, size, total }
            }),
    );
}
