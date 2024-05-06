//! Marketdata manager.  An interface for pooling subscriptions to marketdata throughout
//! multiple threads in a single process.  For subscribing the many different markets, and
//! where market subscriptions are expected to change over time, this ought to provide an
//! easier, more efficient interface than trying to manually juggle a bunch of
//! `BookClient`s.

use super::book_client::BookClient;
use crate::{
    marketdata::utils::Synced,
    symbology::{Cpty, Market, MarketKind},
    Common,
};
use anyhow::{bail, Result};
use api::marketdata::{RfqRequest, RfqResponse};
use futures::channel::mpsc;
use futures_util::StreamExt;
use fxhash::FxHashMap;
use log::{error, warn};
use netidx::{
    pool::Pooled,
    subscriber::{Dval, Event, SubId, UpdatesFlags, Value},
};
use netidx_protocols::{call_rpc, rpc::client::Proc};
use rust_decimal::Decimal;
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    sync::{watch, Mutex},
    task::{self, JoinHandle},
};

pub struct ManagedMarketdata {
    book_handles: Arc<Mutex<BookHandles>>,
    rfq_handles: Arc<Mutex<RfqHandles>>,
    dval_handles: Arc<Mutex<DvalHandles>>,
    common: Common,
    _subscription_driver: Option<JoinHandle<()>>,
    subscription_tx: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
}

// CR alee: periodically garbage collect weaks that have been dropped
pub struct BookHandles {
    by_market: FxHashMap<Market, Weak<Mutex<BookClient>>>,
    by_sub_id: FxHashMap<SubId, Weak<Mutex<BookClient>>>,
}

pub struct RfqHandles {
    by_rfq: FxHashMap<(Cpty, RfqRequest), Weak<Mutex<RfqResponseHandle>>>,
    by_sub_id: FxHashMap<SubId, Weak<Mutex<RfqResponseHandle>>>,
}

pub struct DvalHandles {
    by_market_and_path_leaf: FxHashMap<(Market, String), Weak<Mutex<DvalHandle>>>,
    by_sub_id: FxHashMap<SubId, Weak<Mutex<DvalHandle>>>,
}

pub struct RfqResponseHandle {
    sub: Option<Dval>,
    synced: u64,
    tx_updates: watch::Sender<u64>,
    pub last_rfq_response: Option<RfqResponse>,
}

impl RfqResponseHandle {
    pub fn subscribe_updates(&self) -> Synced {
        Synced(self.tx_updates.subscribe())
    }
}

pub struct DvalHandle {
    sub: Option<Dval>,
    synced: u64,
    tx_updates: watch::Sender<u64>,
    pub last_value: Option<Value>,
}

impl DvalHandle {
    pub fn subscribe_updates(&self) -> Synced {
        Synced(self.tx_updates.subscribe())
    }
}

impl ManagedMarketdata {
    pub fn start(common: Common, runtime: Option<&tokio::runtime::Handle>) -> Self {
        let book_handles = Arc::new(Mutex::new(BookHandles {
            by_market: FxHashMap::default(),
            by_sub_id: FxHashMap::default(),
        }));
        let rfq_handles = Arc::new(Mutex::new(RfqHandles {
            by_rfq: FxHashMap::default(),
            by_sub_id: FxHashMap::default(),
        }));
        let dval_handles = Arc::new(Mutex::new(DvalHandles {
            by_market_and_path_leaf: FxHashMap::default(),
            by_sub_id: FxHashMap::default(),
        }));
        let (tx, mut rx) = mpsc::channel::<Pooled<Vec<(SubId, Event)>>>(10000);
        let handle = {
            let book_handles = book_handles.clone();
            let rfq_handles = rfq_handles.clone();
            let dval_handles = dval_handles.clone();
            let f = async move {
                'outer: while let Some(mut batch) = rx.next().await {
                    let mut book_handles = book_handles.lock().await;
                    let mut rfq_handles = rfq_handles.lock().await;
                    let mut dval_handles = dval_handles.lock().await;
                    for (id, event) in batch.drain(..) {
                        if let Some(book) =
                            book_handles.by_sub_id.get_mut(&id).and_then(|w| w.upgrade())
                        {
                            if let Err(e) = book.lock().await.process_event(event) {
                                error!("error processing book event: {}", e);
                                break 'outer;
                            }
                        } else if let Some(handle) =
                            dval_handles.by_sub_id.get_mut(&id).and_then(|w| w.upgrade())
                        {
                            match event {
                                Event::Unsubscribed => {}
                                Event::Update(v) => {
                                    let mut handle = handle.lock().await;
                                    handle.last_value = Some(v);
                                    handle.synced += 1;
                                    handle.tx_updates.send_replace(handle.synced);
                                }
                            }
                        } else if let Some(rfq) =
                            rfq_handles.by_sub_id.get_mut(&id).and_then(|w| w.upgrade())
                        {
                            match event {
                                // CR alee: should we do something here?
                                Event::Unsubscribed => {}
                                Event::Update(Value::Null) => {}
                                Event::Update(v) => {
                                    match serde_json::from_str::<RfqResponse>(
                                        v.to_string_naked().as_str(),
                                    ) {
                                        Ok(r) => {
                                            let mut rfq = rfq.lock().await;
                                            rfq.last_rfq_response = Some(r);
                                            rfq.synced += 1;
                                            rfq.tx_updates.send_replace(rfq.synced);
                                        }
                                        Err(e) => {
                                            error!("failed to parse RFQ response: {e}",)
                                        }
                                    }
                                }
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
            rfq_handles,
            dval_handles,
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
            rfq_handles: Arc::new(Mutex::new(RfqHandles {
                by_rfq: FxHashMap::default(),
                by_sub_id: FxHashMap::default(),
            })),
            dval_handles: Arc::new(Mutex::new(DvalHandles {
                by_market_and_path_leaf: FxHashMap::default(),
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
            let synced = existing.lock().await.subscribe_updates();
            return (existing, synced);
        }
        let book_path = self.common.paths.marketdata_rt_by_name(market).append("book");
        let book_client = BookClient::new(
            &self.common.subscriber,
            &book_path,
            false,
            market,
            self.subscription_tx.clone(),
        );
        let sub_id = book_client.id();
        let synced = book_client.subscribe_updates();
        let book_client = Arc::new(Mutex::new(book_client));
        book_handles.by_market.insert(market, Arc::downgrade(&book_client));
        book_handles.by_sub_id.insert(sub_id, Arc::downgrade(&book_client));
        (book_client, synced)
    }

    pub async fn subscribe_path(
        &self,
        market: Market,
        path_leaf: String,
    ) -> Result<(Arc<Mutex<DvalHandle>>, Synced)> {
        let path =
            self.common.paths.marketdata_rt_by_name(market).append(path_leaf.as_str());
        let (handle, synced) = {
            let mut dval_handles = self.dval_handles.lock().await;
            if let Some(existing) = dval_handles
                .by_market_and_path_leaf
                .get(&(market, path_leaf.clone()))
                .and_then(|w| w.upgrade())
            {
                let synced = existing.lock().await.subscribe_updates();
                return Ok((existing, synced));
            }
            let (tx_updates, rx_updates) = watch::channel(0);
            let handle = Arc::new(Mutex::new(DvalHandle {
                sub: None,
                synced: 0,
                tx_updates,
                last_value: None,
            }));
            dval_handles
                .by_market_and_path_leaf
                .insert((market, path_leaf), Arc::downgrade(&handle));
            (handle, Synced(rx_updates))
        };
        let dval = self.common.subscriber.subscribe(path);
        let sub_id = dval.id();
        dval.updates(UpdatesFlags::BEGIN_WITH_LAST, self.subscription_tx.clone());
        {
            let mut handle = handle.lock().await;
            handle.sub = Some(dval);
        }
        let mut dval_handles = self.dval_handles.lock().await;
        dval_handles.by_sub_id.insert(sub_id, Arc::downgrade(&handle));
        Ok((handle, synced))
    }

    pub async fn subscribe_rfq(
        &self,
        market: Market,
        qty: Decimal,
    ) -> Result<(Arc<Mutex<RfqResponseHandle>>, Synced)> {
        let cpty = Cpty { venue: market.venue, route: market.route };
        let (base, quote) = match market.kind {
            MarketKind::Exchange(k) => (k.base, k.quote),
            _ => bail!("unsupported market kind"),
        };
        let rfq = RfqRequest { base: base.id, quote: quote.id, quantity: qty };
        let (handle, synced) = {
            let mut rfq_handles = self.rfq_handles.lock().await;
            if let Some(existing) =
                rfq_handles.by_rfq.get(&(cpty, rfq)).and_then(|w| w.upgrade())
            {
                let synced = existing.lock().await.subscribe_updates();
                return Ok((existing, synced));
            }
            // reserve the insert while we wait for the proc
            let (tx_updates, rx_updates) = watch::channel(0);
            let handle = Arc::new(Mutex::new(RfqResponseHandle {
                sub: None,
                synced: 0,
                tx_updates,
                last_rfq_response: None,
            }));
            rfq_handles.by_rfq.insert((cpty, rfq), Arc::downgrade(&handle));
            (handle, Synced(rx_updates))
        };
        let path = self.common.paths.marketdata_api(cpty).append("subscribe-rfq");
        let proc = Proc::new_with_timeout(
            &self.common.subscriber,
            path,
            Duration::from_secs(2),
        )?;
        let res = call_rpc!(
            proc,
            base: format!("\"{}\"", rfq.base),
            quote: format!("\"{}\"", rfq.quote),
            quantity: rfq.quantity
        )
        .await?;
        let uuid = res.to_string_naked();
        let rfq_path = self.common.paths.marketdata_rfq(cpty).append(uuid.as_str());
        let dval = self.common.subscriber.subscribe(rfq_path);
        let sub_id = dval.id();
        dval.updates(UpdatesFlags::BEGIN_WITH_LAST, self.subscription_tx.clone());
        {
            let mut handle = handle.lock().await;
            handle.sub = Some(dval);
        }
        let mut rfq_handles = self.rfq_handles.lock().await;
        rfq_handles.by_sub_id.insert(sub_id, Arc::downgrade(&handle));
        Ok((handle, synced))
    }

    /// Keep a book client alive for some time instead of dropping immediately
    pub fn retain(book_client: Arc<Mutex<BookClient>>, duration: Duration) {
        task::spawn(async move {
            let _retained = book_client;
            tokio::time::sleep(duration).await;
        });
    }
}
