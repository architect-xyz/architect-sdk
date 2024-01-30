//! Marketdata manager.  An interface for pooling subscriptions to marketdata throughout
//! multiple threads in a single process.  For subscribing the many different markets, and
//! where market subscriptions are expected to change over time, this ought to provide an
//! easier, more efficient interface than trying to manually juggle a bunch of
//! `BookClient`s.

use super::book_client::{BookClient, Synced};
use crate::{symbology::Market, Common};
use futures::channel::mpsc;
use futures_util::StreamExt;
use fxhash::FxHashMap;
use log::{error, warn};
use netidx::{
    pool::Pooled,
    subscriber::{Event, SubId},
};
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    sync::Mutex,
    task::{self, JoinHandle},
};

pub struct ManagedMarketdata {
    book_handles: Arc<Mutex<BookHandles>>,
    common: Common,
    _subscription_driver: Option<JoinHandle<()>>,
    subscription_tx: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
}

pub struct BookHandles {
    by_market: FxHashMap<Market, Weak<Mutex<BookClient>>>,
    by_sub_id: FxHashMap<SubId, Weak<Mutex<BookClient>>>,
}

impl ManagedMarketdata {
    pub fn start(common: Common, runtime: Option<&tokio::runtime::Handle>) -> Self {
        let book_handles = Arc::new(Mutex::new(BookHandles {
            by_market: FxHashMap::default(),
            by_sub_id: FxHashMap::default(),
        }));
        let (tx, mut rx) = mpsc::channel::<Pooled<Vec<(SubId, Event)>>>(10000);
        let handle = {
            let book_handles = book_handles.clone();
            let f = async move {
                'outer: while let Some(mut batch) = rx.next().await {
                    let mut book_handles = book_handles.lock().await;
                    for (id, event) in batch.drain(..) {
                        if let Some(book) =
                            book_handles.by_sub_id.get_mut(&id).and_then(|w| w.upgrade())
                        {
                            if let Err(e) = book.lock().await.process_event(event) {
                                error!("error processing book event: {}", e);
                                break 'outer;
                            }
                        }
                    }
                }
                warn!("subscription driver terminated");
            };
            match runtime {
                Some(rt) => rt.spawn(f),
                None => task::spawn(f),
            }
        };
        Self {
            book_handles,
            common,
            _subscription_driver: Some(handle),
            subscription_tx: tx,
        }
    }

    pub fn dummy(common: Common) -> Self {
        let (tx, _rx) = mpsc::channel::<Pooled<Vec<(SubId, Event)>>>(1);
        Self {
            book_handles: Arc::new(Mutex::new(BookHandles {
                by_market: FxHashMap::default(),
                by_sub_id: FxHashMap::default(),
            })),
            common,
            _subscription_driver: None,
            subscription_tx: tx,
        }
    }

    pub async fn subscribe(&self, market: Market) -> (Arc<Mutex<BookClient>>, Synced) {
        let mut book_handles = self.book_handles.lock().await;
        if let Some(existing) =
            book_handles.by_market.get(&market).and_then(|w| w.upgrade())
        {
            let synced = existing.lock().await.subscribe_synced();
            return (existing, synced);
        }
        let book_path = self.common.paths.marketdata_rt_book(market);
        let book_client = BookClient::new(
            &self.common.subscriber,
            &book_path,
            false,
            market,
            self.subscription_tx.clone(),
        );
        let sub_id = book_client.id();
        let synced = book_client.subscribe_synced();
        let book_client = Arc::new(Mutex::new(book_client));
        book_handles.by_market.insert(market, Arc::downgrade(&book_client));
        book_handles.by_sub_id.insert(sub_id, Arc::downgrade(&book_client));
        (book_client, synced)
    }

    /// Keep a book client alive for some time instead of dropping immediately
    pub fn retain(book_client: Arc<Mutex<BookClient>>, duration: Duration) {
        task::spawn(async move {
            let _retained = book_client;
            tokio::time::sleep(duration).await;
        });
    }
}
