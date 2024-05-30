//! Subscribe to book data

use crate::symbology::Market;
use crate::synced::Synced;
use anyhow::{anyhow, bail, Result};
use api::marketdata::{MessageHeader, NetidxFeedPaths, Snapshot, Updates};
use consolidated_level_book::ConsolidatedLevelBook;
use futures::channel::mpsc;
use fxhash::FxHashMap;
use log::trace;
use netidx::{
    pack::Pack,
    path::Path,
    pool::Pooled,
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags, Value},
};
use std::ops::Deref;
use tokio::sync::watch;

pub mod consolidated_level_book;
pub mod level_book;
pub use level_book::*;

/// A subscription to book data
pub struct BookClient {
    book: LevelBook,
    market: Market,
    subscription: Dval,
    synced: u64,
    tx_updates: watch::Sender<u64>,
}

impl Deref for BookClient {
    type Target = LevelBook;

    fn deref(&self) -> &Self::Target {
        &self.book
    }
}

impl BookClient {
    /// Subscribe to book data for the specified market at the given
    /// base path. You must receive the output of the specified up
    /// channel and call `process_event` for each event received with
    /// an id that matches the id of this subscription.
    pub fn new(
        subscriber: &Subscriber,
        path: &Path,
        _display: bool,
        market: Market,
        up: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
    ) -> Self {
        // let base =
        //     common.paths.qf_rt(Some(Cpty { venue: Some(tp.venue), route: tp.route }));
        let already = subscriber.is_subscribed_or_pending(&path);
        // let subscription = if display {
        //     common
        //         .display_subscriber()
        //         .subscribe_updates(path, [(UpdatesFlags::empty(), up)])
        // } else {
        let subscription =
            subscriber.subscribe_updates(path.clone(), [(UpdatesFlags::empty(), up)]);
        // };
        if already || subscription.strong_count() > 1 {
            // if we are already subscribed then we need to ask for a
            // snapshot manually
            subscription.write(Value::Null);
        }
        let synced = 0;
        let (tx_updates, _) = watch::channel(synced);
        Self { book: LevelBook::default(), market, subscription, synced, tx_updates }
    }

    /// Return the id of this subscription
    pub fn id(&self) -> SubId {
        self.subscription.id()
    }

    pub fn book(&self) -> &LevelBook {
        &self.book
    }

    pub fn market(&self) -> &Market {
        &self.market
    }

    pub fn synced(&self) -> bool {
        self.synced != 0
    }

    pub fn subscribe_updates(&self) -> Synced<u64> {
        Synced(self.tx_updates.subscribe())
    }

    /// Process the specified book event, updating the book with its contents.
    pub fn process_event(&mut self, ev: Event) -> Result<()> {
        match ev {
            Event::Update(Value::Bytes(mut buf)) => {
                let typ: MessageHeader = Pack::decode(&mut buf)?;
                match typ {
                    MessageHeader::Updates => {
                        if self.synced > 0 {
                            let updates: Updates = Pack::decode(&mut buf)?;
                            trace!("book updates: {:?}", updates);
                            self.book.update(updates);
                            self.synced += 1;
                            self.tx_updates.send_replace(self.synced);
                        }
                    }
                    MessageHeader::Snapshot => {
                        let snap: Snapshot = Pack::decode(&mut buf)?;
                        trace!("book snap: {:?}", snap);
                        self.book.update_from_snapshot(snap);
                        self.synced = 1;
                        self.tx_updates.send_replace(self.synced);
                    }
                }
            }
            // this is the default value before the book subscribes on the qf side
            Event::Update(Value::Null) | Event::Unsubscribed => (),
            e => bail!("book protocol error, invalid event {:?}", e),
        }
        Ok(())
    }
}

/// Subscriptions to multiple books consolidated into one
pub struct ConsolidatedBookClient {
    consolidated_book: ConsolidatedLevelBook,
    books: FxHashMap<SubId, (Market, BookClient)>,
}

impl Deref for ConsolidatedBookClient {
    type Target = ConsolidatedLevelBook;

    fn deref(&self) -> &Self::Target {
        &self.consolidated_book
    }
}

impl ConsolidatedBookClient {
    /// Subscribe to book data for the specified tradable products. You
    /// must receive the output of the specified up channel and call
    /// `process_event` for each event received.
    pub fn new(
        subscriber: &Subscriber,
        base_path: &Path,
        display: bool,
        markets: Vec<Market>,
        up: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
    ) -> Self {
        let mut books: FxHashMap<SubId, (Market, BookClient)> = FxHashMap::default();
        markets.iter().for_each(|m| {
            let path = m.path_by_name(base_path).append("book");
            let client = BookClient::new(subscriber, &path, display, *m, up.clone());
            books.insert(client.id(), (*m, client));
        });
        Self { consolidated_book: ConsolidatedLevelBook::default(), books }
    }

    /// Process the specified book event, updating the indivudal book and
    /// consolidated book with it contents.
    pub fn process_event(&mut self, sub_id: SubId, ev: Event) -> Result<()> {
        let (tp, book_client) = self
            .books
            .get_mut(&sub_id)
            .ok_or_else(|| anyhow!("missing book for sub_id: {:?}", sub_id))?;
        book_client.process_event(ev.clone())?;
        match ev {
            Event::Update(Value::Bytes(mut buf)) => {
                let typ: MessageHeader = Pack::decode(&mut buf)?;
                match typ {
                    MessageHeader::Updates => {
                        if book_client.synced() {
                            let updates: Updates = Pack::decode(&mut buf)?;
                            self.consolidated_book.update(*tp, updates)
                        }
                    }
                    MessageHeader::Snapshot => {
                        let snap: Snapshot = Pack::decode(&mut buf)?;
                        self.consolidated_book.update_from_snapshot(*tp, snap)
                    }
                }
            }
            // this is the default value before the book subscribes on the qf side
            Event::Update(Value::Null) | Event::Unsubscribed => (),
            e => bail!("book protocol error, invalid event {:?}", e),
        }
        Ok(())
    }
}
