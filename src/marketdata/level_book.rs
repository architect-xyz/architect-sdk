use anyhow::{anyhow, Result};
use architect_api::{
    pool,
    utils::{pool::Pooled, sequence::SequenceIdAndNumber},
    Dir, DirPair,
};
use chrono::prelude::*;
use derive_more::{Deref, DerefMut};
use itertools::Itertools;
use rust_decimal::Decimal;
use std::{
    collections::{btree_map::Iter, BTreeMap},
    iter::Rev,
};

// CR alee: probably want to rethink where to put these
pub trait LevelLike {
    fn price(&self) -> Decimal;
    fn size(&self) -> Decimal;
    fn total(&self) -> Option<Decimal>;
}

impl LevelLike for (&Decimal, &Decimal) {
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
#[derive(Debug, Default, Deref, DerefMut, Clone)]
pub struct LevelBook {
    #[deref]
    #[deref_mut]
    pub book: DirPair<BTreeMap<Decimal, Decimal>>,
    pub timestamp: DateTime<Utc>,
}

impl LevelBook {
    /// clear the order book
    pub fn clear(&mut self) {
        self.buy.clear();
        self.sell.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.buy.is_empty() && self.sell.is_empty()
    }

    /// return the best price and quantity given a direction
    pub fn best(&self, dir: Dir) -> Option<(Decimal, Decimal)> {
        match dir {
            Dir::Buy => self.buy.iter().next_back().map(|(price, size)| (*price, *size)),
            Dir::Sell => self.sell.iter().next().map(|(price, size)| (*price, *size)),
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

    pub fn to_l2_book_snapshot(
        &self,
        sequence: SequenceIdAndNumber,
    ) -> architect_api::marketdata::L2BookSnapshot {
        let bids = self.iter_levels(Dir::Buy).map(|(p, q)| (*p, *q)).collect::<Vec<_>>();
        let asks = self.iter_levels(Dir::Sell).map(|(p, q)| (*p, *q)).collect::<Vec<_>>();
        architect_api::marketdata::L2BookSnapshot {
            timestamp: self.timestamp.timestamp(),
            timestamp_ns: self.timestamp.timestamp_subsec_nanos(),
            sequence,
            bids,
            asks,
        }
    }

    pub fn of_l2_book_snapshot(
        snapshot: architect_api::marketdata::L2BookSnapshot,
    ) -> Result<Self> {
        let timestamp =
            snapshot.timestamp().ok_or(anyhow!("BUG: Snapshot timestamp is invalid"))?;
        Ok(Self {
            book: DirPair {
                buy: snapshot.bids.into_iter().collect(),
                sell: snapshot.asks.into_iter().collect(),
            },
            timestamp,
        })
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

impl LevelLike for &CondensedLevel {
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
            .chunk_by(|(p, _)| *p)
            .into_iter()
            .take(num_levels)
            .map(|(price, sizes)| {
                let size = sizes.map(|(_, s)| s).sum();
                total += size;
                CondensedLevel { price, size, total }
            }),
    );
}
