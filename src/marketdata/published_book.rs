// CR alee: this might be deprecated/unused

use super::book_client::level_book::{LevelBook, LevelLike};
use crate::symbology::Market;
use anyhow::Result;
use api::{
    marketdata::NetidxFeedPaths, symbology::market::NormalizedMarketInfo, Dir, DirPair,
};
use fxhash::FxHashMap;
use netidx::{
    path::Path,
    publisher::{PublishFlags, Publisher, UpdateBatch, Val, Value},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

trait PBook {
    fn update_level(
        &mut self,
        batch: &mut UpdateBatch,
        level: u32,
        price: Decimal,
        size: Decimal,
        total: Decimal,
    ) -> Result<()>;
    fn clear_used(&mut self);
    fn mark_used(&mut self, lvl: &Decimal);
    fn remove_unused_levels(&mut self);
    fn finalize_unused_levels(&mut self, batch: &mut UpdateBatch);
}

struct PublishedBook {
    prices: Vec<Val>,
    sizes: Vec<Val>,
    totals: Vec<Val>,
    publisher: Publisher,
    base: Path,
    alias_base: Path,
    dir: Dir,
}

impl PBook for PublishedBook {
    fn update_level(
        &mut self,
        batch: &mut UpdateBatch,
        level: u32,
        price: Decimal,
        size: Decimal,
        total: Decimal,
    ) -> Result<()> {
        let level = level as usize;
        self.prices[level].update_changed(batch, price);
        self.sizes[level].update_changed(batch, size);
        self.totals[level].update_changed(batch, total);
        Ok(())
    }

    fn clear_used(&mut self) {}

    fn mark_used(&mut self, _lvl: &Decimal) {}

    fn remove_unused_levels(&mut self) {}

    fn finalize_unused_levels(&mut self, _batch: &mut UpdateBatch) {}
}

impl PublishedBook {
    fn publish_level(&mut self) -> Result<()> {
        let base = &self.base;
        let alias_base = &self.alias_base;
        let publisher = &self.publisher;
        let publish = |path: Path| -> Result<Val> {
            let flags = PublishFlags::FORCE_LOCAL | PublishFlags::USE_EXISTING;
            let val =
                publisher.publish_with_flags(flags, base.append(&path), Value::Null)?;
            publisher.alias_with_flags(val.id(), flags, alias_base.append(&path))?;
            Ok(val)
        };
        let id = self.prices.len();
        let prefix = self.dir.to_str_lowercase();
        self.prices.push(publish(format!("{}/{}/price", prefix, id).into())?);
        self.sizes.push(publish(format!("{}/{}/size", prefix, id).into())?);
        self.totals.push(publish(format!("{}/{}/total", prefix, id).into())?);
        Ok(())
    }

    fn new(
        publisher: &Publisher,
        base: &Path,
        market: &Market,
        dir: Dir,
        num_levels: usize,
    ) -> Result<Self> {
        let alias_base = market.path_by_name(&base);
        let base = market.path_by_id(&base);
        let mut t = Self {
            publisher: publisher.clone(),
            base,
            alias_base,
            dir,
            prices: vec![],
            sizes: vec![],
            totals: vec![],
        };
        for _ in 0..num_levels {
            t.publish_level()?
        }
        Ok(t)
    }
}

struct BookEntry {
    price: Val,
    size: Val,
    total: Val,
    level: Val,
    used: bool,
}

struct PublishedDeltaBook {
    base: Path,
    alias_base: Path,
    prefix: String,
    publisher: Publisher,
    level_id: usize,
    by_price: FxHashMap<Decimal, BookEntry>,
    unused: Vec<BookEntry>,
    unused_prices: Vec<Decimal>,
}

impl PBook for PublishedDeltaBook {
    fn clear_used(&mut self) {
        for ent in self.by_price.values_mut() {
            ent.used = false;
        }
    }

    fn mark_used(&mut self, lvl: &Decimal) {
        if let Some(ent) = self.by_price.get_mut(lvl) {
            ent.used = true;
        }
    }

    fn update_level(
        &mut self,
        batch: &mut UpdateBatch,
        level: u32,
        price: Decimal,
        size: Decimal,
        total: Decimal,
    ) -> Result<()> {
        let unused = &mut self.unused;
        let ent = self.by_price.entry(price).or_insert_with(|| unused.pop().unwrap());
        ent.used = true;
        ent.level.update_changed(batch, Value::V32(level));
        ent.price.update_changed(batch, price);
        ent.size.update_changed(batch, size);
        ent.total.update_changed(batch, total);
        Ok(())
    }

    fn remove_unused_levels(&mut self) {
        for (price, ent) in &self.by_price {
            if !ent.used {
                self.unused_prices.push(*price)
            }
        }
        for price in self.unused_prices.drain(..) {
            if let Some(ent) = self.by_price.remove(&price) {
                self.unused.push(ent)
            }
        }
    }

    fn finalize_unused_levels(&mut self, batch: &mut UpdateBatch) {
        for ent in self.unused.iter_mut() {
            ent.level.update_changed(batch, Value::V32(999));
        }
    }
}

fn to_base26(i: usize) -> String {
    static ALPHABET: [char; 26] = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
        'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];
    let mut res = String::new();
    if i < 26 {
        res.push(ALPHABET[i])
    } else {
        res.push(ALPHABET[i / 26]);
        res.push(ALPHABET[i % 26]);
    }
    res
}

impl PublishedDeltaBook {
    fn new(
        publisher: &Publisher,
        base: &Path,
        market: &Market,
        prefix: &str,
        dir: Dir,
        num_levels: usize,
    ) -> Result<Self> {
        let alias_base = market.path_by_name(&base);
        let base = market.path_by_id(&base);
        let dir = dir.to_str_lowercase();
        let prefix = format!("{}{}", prefix, dir);
        let mut t = Self {
            publisher: publisher.clone(),
            base,
            alias_base,
            prefix,
            level_id: 0,
            unused: vec![],
            by_price: HashMap::default(),
            unused_prices: vec![],
        };
        for _ in 0..num_levels {
            let lvl = t.publish_level()?;
            t.unused.push(lvl);
        }
        Ok(t)
    }

    fn publish_level(&mut self) -> Result<BookEntry> {
        let base = &self.base;
        let alias_base = &self.alias_base;
        let publisher = &self.publisher;
        let publish = |path: Path| -> Result<Val> {
            let flags = PublishFlags::FORCE_LOCAL | PublishFlags::USE_EXISTING;
            let val =
                publisher.publish_with_flags(flags, base.append(&path), Value::Null)?;
            publisher.alias_with_flags(val.id(), flags, alias_base.append(&path))?;
            Ok(val)
        };
        let id = to_base26(self.level_id);
        self.level_id += 1;
        let prefix = &self.prefix;
        Ok(BookEntry {
            price: publish(format!("{}/{}/price", prefix, id).into())?,
            size: publish(format!("{}/{}/size", prefix, id).into())?,
            total: publish(format!("{}/{}/total", prefix, id).into())?,
            level: publish(format!("{}/{}/level", prefix, id).into())?,
            used: false,
        })
    }
}

// CR estokes: on demand publishing
pub struct BookVals {
    full: DirPair<PublishedBook>,
    grouped: FxHashMap<u32, (Decimal, DirPair<PublishedDeltaBook>)>,
}

impl BookVals {
    pub fn new(publisher: &Publisher, base: &Path, market: &Market) -> Result<BookVals> {
        let full = DirPair {
            buy: PublishedBook::new(publisher, base, market, Dir::Buy, 5)?,
            sell: PublishedBook::new(publisher, base, market, Dir::Sell, 5)?,
        };
        let tick_size = market.extra_info.tick_size();
        let grouped = [1, 10, 100, 1000, 5000, 10000]
            .into_iter()
            .map(|width_in_ticks| {
                let prefix = format!("book/{}/", width_in_ticks);
                let buy = PublishedDeltaBook::new(
                    publisher,
                    base,
                    market,
                    &prefix,
                    Dir::Buy,
                    50,
                )?;
                let sell = PublishedDeltaBook::new(
                    publisher,
                    base,
                    market,
                    &prefix,
                    Dir::Sell,
                    50,
                )?;
                let precision = if width_in_ticks > 1 {
                    Decimal::from(width_in_ticks) * tick_size
                } else {
                    dec!(1) * tick_size
                };
                Ok((width_in_ticks, (precision, DirPair { buy, sell })))
            })
            .collect::<Result<_>>()?;
        Ok(BookVals { full, grouped })
    }

    fn update_raw<'a, L, B, I0, I1>(
        num_levels: u32,
        published: &mut DirPair<B>,
        batch: &mut UpdateBatch,
        bids0: I0,
        bids1: I0,
        asks0: I1,
        asks1: I1,
    ) -> Result<()>
    where
        L: LevelLike + 'a,
        I0: Iterator<Item = L>,
        I1: Iterator<Item = L>,
        B: PBook + 'static,
    {
        {
            let mut i = 0;
            let mut total = Decimal::ZERO;
            let bid = published.get_mut(Dir::Buy);
            bid.clear_used();
            for l in bids0 {
                bid.mark_used(&l.price());
            }
            bid.remove_unused_levels();
            for l in bids1 {
                let total = match l.total() {
                    None => {
                        total += l.size();
                        total
                    }
                    Some(t) => t,
                };
                bid.update_level(batch, i, l.price(), l.size(), total)?;
                i += 1;
                if i >= num_levels {
                    break;
                }
            }
            bid.finalize_unused_levels(batch);
        }
        {
            let mut i = 0;
            let mut total = Decimal::ZERO;
            let ask = published.get_mut(Dir::Sell);
            ask.clear_used();
            for l in asks0 {
                ask.mark_used(&l.price());
            }
            ask.remove_unused_levels();
            for l in asks1 {
                let total = match l.total() {
                    None => {
                        total += l.size();
                        total
                    }
                    Some(t) => t,
                };
                ask.update_level(batch, i, l.price(), l.size(), total)?;
                i += 1;
                if i >= num_levels {
                    break;
                }
            }
            ask.finalize_unused_levels(batch);
        }
        Ok(())
    }

    pub fn update(&mut self, batch: &mut UpdateBatch, book: &LevelBook) -> Result<()> {
        Self::update_raw(
            5,
            &mut self.full,
            batch,
            book.get(Dir::Buy).iter().rev(),
            book.get(Dir::Buy).iter().rev(),
            book.get(Dir::Sell).iter(),
            book.get(Dir::Sell).iter(),
        )
    }

    pub fn update_on_interval(
        &mut self,
        batch: &mut UpdateBatch,
        book: &LevelBook,
    ) -> Result<()> {
        for (precision, published) in self.grouped.values_mut() {
            let condensed = book.condense(50, *precision);
            Self::update_raw(
                50,
                published,
                batch,
                condensed.get(Dir::Buy).iter(),
                condensed.get(Dir::Buy).iter(),
                condensed.get(Dir::Sell).iter(),
                condensed.get(Dir::Sell).iter(),
            )?
        }
        Ok(())
    }
}
