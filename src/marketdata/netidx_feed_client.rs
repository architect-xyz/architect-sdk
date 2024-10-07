//! Subscribe to all netidx feed data

use super::book_client::BookClient;
use crate::symbology::MarketRef;
use anyhow::Result;
use api::marketdata::TradeV1;
use futures::channel::mpsc;
use fxhash::{FxHashMap, FxHashSet};
use log::{debug, error};
use netidx::{
    path::Path,
    pool::{Poolable, Pooled},
    subscriber::{Dval, Event, FromValue, SubId, Subscriber, UpdatesFlags, Value},
};
use rust_decimal::Decimal;

pub struct Client<T: FromValue> {
    pub store: Option<T>,
    pub subscription: Dval,
    pub sub_id: SubId,
    pub path: Path,
}

impl<T: FromValue> Client<T> {
    pub fn new(
        subscriber: &Subscriber,
        up: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
        path: Path,
    ) -> Self {
        let subscription =
            subscriber.subscribe_updates(path.clone(), [(UpdatesFlags::empty(), up)]);
        let sub_id = subscription.id();
        Self { store: None, subscription, sub_id, path }
    }

    pub fn process_event(&mut self, ev: Event) -> Result<()> {
        match ev {
            Event::Update(val) => match Option::from_value(val) {
                Ok(val) => {
                    self.store = val;
                }
                Err(e) => {
                    debug!("Could not cast value ffrom path {}: {}", self.path, e);
                }
            },
            Event::Unsubscribed => (),
        }
        Ok(())
    }
}

// Copied from netidx feed, maybe move to API instead
pub struct TickerClient {
    pub open_24h: Client<Decimal>,
    pub volume_24h: Client<Decimal>,
    pub low_24h: Client<Decimal>,
    pub high_24h: Client<Decimal>,
    pub volume_30d: Client<Decimal>,
    sub_ids: FxHashSet<SubId>,
}

impl TickerClient {
    pub fn new(
        subscriber: &Subscriber,
        up: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
        base_path: Path,
    ) -> Self {
        let open_24h = Client::new(subscriber, up.clone(), base_path.append("open_24h"));
        let volume_24h =
            Client::new(subscriber, up.clone(), base_path.append("volume_24h"));
        let low_24h = Client::new(subscriber, up.clone(), base_path.append("low_24h"));
        let high_24h = Client::new(subscriber, up.clone(), base_path.append("high_24h"));
        let volume_30d =
            Client::new(subscriber, up.clone(), base_path.append("volume_30d"));
        let sub_ids: FxHashSet<SubId> = [
            open_24h.sub_id,
            volume_24h.sub_id,
            low_24h.sub_id,
            high_24h.sub_id,
            volume_30d.sub_id,
        ]
        .iter()
        .cloned()
        .collect();
        Self { open_24h, volume_24h, low_24h, high_24h, volume_30d, sub_ids }
    }

    pub fn process_event(&mut self, sub_id: SubId, ev: Event) -> Result<()> {
        if sub_id == self.open_24h.sub_id {
            self.open_24h.process_event(ev)?;
        } else if sub_id == self.volume_24h.sub_id {
            self.volume_24h.process_event(ev)?;
        } else if sub_id == self.low_24h.sub_id {
            self.low_24h.process_event(ev)?;
        } else if sub_id == self.high_24h.sub_id {
            self.high_24h.process_event(ev)?;
        } else if sub_id == self.volume_30d.sub_id {
            self.volume_30d.process_event(ev)?;
        }
        Ok(())
    }

    pub fn is_subscribed(&self, sub_id: &SubId) -> bool {
        self.sub_ids.contains(sub_id)
    }
}

pub struct NetidxFeedClient {
    pub book_client: BookClient,
    pub last_trade_client: Client<TradeV1>,
    pub ticker_client: TickerClient,
    pub extra_vals: FxHashMap<SubId, Client<Value>>,
    pub extra_vals_sub_ids: FxHashMap<String, SubId>,
    pub sub_ids: FxHashSet<SubId>,
}

impl NetidxFeedClient {
    pub fn new(
        subscriber: &Subscriber,
        up: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
        base_path: &Path,
        extra_vals_keys: &Vec<String>,
        market: MarketRef,
    ) -> Self {
        let book_path = base_path.append("book");
        debug!("subscribing to book at {}", book_path);
        let book_client =
            BookClient::new(subscriber, &book_path, false, market, up.clone());
        let last_trade_client =
            Client::new(subscriber, up.clone(), base_path.append("last_trade_v1"));
        let ticker_client = TickerClient::new(subscriber, up.clone(), base_path.clone());
        let mut extra_vals_sub_ids = FxHashMap::empty();
        let mut extra_vals = FxHashMap::empty();
        for key in extra_vals_keys.into_iter() {
            let path = base_path.append(key);
            let client = Client::new(subscriber, up.clone(), path);
            let sub_id = client.sub_id;
            extra_vals.insert(sub_id, client);
            extra_vals_sub_ids.insert(key.clone(), sub_id);
        }
        let sub_ids: FxHashSet<SubId> = extra_vals
            .keys()
            .copied()
            .chain(ticker_client.sub_ids.iter().copied())
            .chain(vec![book_client.id(), last_trade_client.sub_id].iter().copied())
            .collect();
        Self {
            book_client,
            last_trade_client,
            ticker_client,
            extra_vals,
            extra_vals_sub_ids,
            sub_ids,
        }
    }

    pub fn process_event(&mut self, sub_id: SubId, ev: Event) -> Result<()> {
        if sub_id == self.book_client.id() {
            self.book_client.process_event(ev)?;
        } else if sub_id == self.last_trade_client.sub_id {
            self.last_trade_client.process_event(ev)?;
        } else if self.ticker_client.is_subscribed(&sub_id) {
            self.ticker_client.process_event(sub_id, ev)?;
        } else if let Some(client) = self.extra_vals.get_mut(&sub_id) {
            client.process_event(ev)?;
        } else {
            error!("unhandled event for sub_id: {:?}", sub_id);
        }
        Ok(())
    }

    pub fn market(&mut self) -> &MarketRef {
        self.book_client.market()
    }

    pub fn get_extra_val(&self, key: &str) -> Option<&Client<Value>> {
        match self.extra_vals_sub_ids.get(key) {
            Some(sub_id) => self.extra_vals.get(sub_id),
            None => None,
        }
    }
}
