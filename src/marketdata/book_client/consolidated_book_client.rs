use super::{book_client::BookClient, consolidated_level_book::ConsolidatedLevelBook};
use crate::symbology::MarketRef;
use anyhow::{anyhow, bail, Result};
use api::marketdata::{MessageHeader, NetidxFeedPaths, Snapshot, Updates};
use futures::channel::mpsc;
use fxhash::FxHashMap;
use netidx::{
    pack::Pack,
    path::Path,
    pool::Pooled,
    subscriber::{Event, SubId, Subscriber, Value},
};
use std::ops::Deref;

/// Subscriptions to multiple books consolidated into one
pub struct ConsolidatedBookClient {
    consolidated_book: ConsolidatedLevelBook,
    books: FxHashMap<SubId, (MarketRef, BookClient)>,
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
        markets: Vec<MarketRef>,
        up: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
    ) -> Self {
        let mut books: FxHashMap<SubId, (MarketRef, BookClient)> = FxHashMap::default();
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
