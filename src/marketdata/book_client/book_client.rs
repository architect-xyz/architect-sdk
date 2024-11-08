use super::level_book::LevelBook;
use crate::{symbology::MarketRef, synced::Synced};
use anyhow::{bail, Context, Result};
use api::marketdata::{MessageHeader, Snapshot, Updates};
use futures::channel::mpsc;
use log::trace;
use netidx::{
    pack::Pack,
    path::Path,
    pool::Pooled,
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags, Value},
};
use std::ops::Deref;
use tokio::sync::watch;

/// A subscription to book data
pub struct BookClient {
    book: LevelBook,
    market: MarketRef,
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
        market: MarketRef,
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

    pub fn market(&self) -> &MarketRef {
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
                let typ: MessageHeader =
                    Pack::decode(&mut buf).with_context(|| "invalid message header")?;
                match typ {
                    MessageHeader::Updates => {
                        if self.synced > 0 {
                            let updates: Updates = Pack::decode(&mut buf)
                                .with_context(|| "invalid book update")?;
                            trace!("book updates: {:?}", updates);
                            self.book.update(updates);
                            self.synced += 1;
                            self.tx_updates.send_replace(self.synced);
                        }
                    }
                    MessageHeader::Snapshot => {
                        let snap: Snapshot = Pack::decode(&mut buf)
                            .with_context(|| "invalid book snapshot")?;
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
