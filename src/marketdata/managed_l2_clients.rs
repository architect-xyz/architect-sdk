use super::l2_client::{L2ClientHandle, L2ClientState};
use crate::{symbology::MarketRef, synced::SyncHandle};
use anyhow::{anyhow, bail, Result};
use api::{
    external::marketdata::*, grpc::json_service::marketdata_client::MarketdataClient,
    symbology::MarketId,
};
use futures::{select_biased, stream::FuturesUnordered, FutureExt, StreamExt};
use fxhash::FxHashMap;
use log::{error, warn};
use parking_lot::Mutex;
use std::{
    borrow::Borrow,
    future::Future,
    hash::{Hash, Hasher},
    ops::Deref,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::StreamMap;
use tonic::{transport::Channel, Status, Streaming};

pub struct ManagedL2Clients {
    handles: Arc<Mutex<FxHashMap<MarketId, L2ClientHandle>>>,
    updates: StreamMap<StreamKey, Streaming<L2BookUpdate>>,
    subscribing: FuturesUnordered<Pin<Box<dyn Future<Output = SubscribeResult> + Send>>>,
    tx_subs: mpsc::Sender<SubscribeOrUnsubscribe<MarketId>>,
    rx_subs: mpsc::Receiver<SubscribeOrUnsubscribe<MarketId>>,
    cooldown_before_unsubscribe: Option<Duration>,
    cooldown_max_tasks: usize,
    cooler: FuturesUnordered<Pin<Box<dyn Future<Output = L2ClientHandle> + Send>>>,
}

type SubscribeResult = Result<(StreamKey, Streaming<L2BookUpdate>), MarketId>;

enum SubscribeOrUnsubscribe<T> {
    Subscribe(T, Option<oneshot::Sender<Result<L2ClientHandle>>>),
    Unsubscribe(T),
}

impl ManagedL2Clients {
    pub fn new() -> Self {
        let (tx_subs, rx_subs) = mpsc::channel(1000);
        let subscribing: FuturesUnordered<
            Pin<Box<dyn Future<Output = SubscribeResult> + Send>>,
        > = FuturesUnordered::new();
        let cooler: FuturesUnordered<
            Pin<Box<dyn Future<Output = L2ClientHandle> + Send>>,
        > = FuturesUnordered::new();
        // don't stop the music
        subscribing.push(Box::pin(async move { std::future::pending().await }));
        cooler.push(Box::pin(async move { std::future::pending().await }));
        Self {
            handles: Arc::new(Mutex::new(FxHashMap::default())),
            updates: StreamMap::new(),
            subscribing,
            tx_subs,
            rx_subs,
            cooldown_before_unsubscribe: None,
            cooldown_max_tasks: 1024,
            cooler,
        }
    }

    /// If set, the manager will retain an unused marketdata subscription
    /// for at least `duration` before actually unsubscribing.  This might
    /// be useful to debounce rapid subscription and unsubscription.
    ///
    /// Requires a specific async runtime to be enabled.
    // #[cfg(feature = "tokio")]
    pub fn set_cooldown_before_unsubscribe(&mut self, duration: Duration) {
        self.cooldown_before_unsubscribe = Some(duration);
    }

    /// Set the maximum number of subscriptions to cooldown.  The next
    /// subscription after the max will drop immediately without respecting
    /// the cooldown period.
    pub fn set_cooldown_max_tasks(&mut self, max_tasks: usize) {
        self.cooldown_max_tasks = max_tasks;
    }

    pub fn handle(&self) -> ManagedL2ClientsHandle {
        ManagedL2ClientsHandle {
            handles: self.handles.clone(),
            tx_subs: self.tx_subs.clone(),
        }
    }

    // #[cfg(feature = "tokio")]
    pub fn run_in_background(
        mut self,
    ) -> (ManagedL2ClientsHandle, JoinHandle<Result<()>>) {
        let handle = self.handle();
        let task = tokio::spawn(async move {
            loop {
                self.next().await?;
            }
        });
        (handle, task)
    }

    pub async fn next(&mut self) -> Result<()> {
        select_biased! {
            r = self.cooler.next().fuse() => {
                let handle = r.ok_or_else(|| anyhow!("cooler empty"))?;
                if let Some(alive) = handle.alive.upgrade() {
                    // CR alee: add tests or have a less brittle way to
                    // know what the "right" ref count should be here.
                    if Arc::weak_count(&alive) <= 3 {
                        // nobody using this handle other than ourselves
                        // (1) in self.handles,
                        // (2) in self.updates,
                        // (3) in the handle here
                        self.unsubscribe(handle.market_id);
                    }
                }
            }
            r = self.rx_subs.recv().fuse() => {
                let action = r.ok_or_else(|| anyhow!("rx_subs dropped"))?;
                self.apply_subscribe_or_unsubscribe(action).await;
            }
            // next_update awaits forever if updates is empty, but...
            //
            // we are waiting for a first subscription, which has
            // to happen as a result of the subscribing queue, so
            // there's no livelock here.
            r = Self::next_update(&mut self.updates).fuse() => {
                if let Some((key, item)) = r {
                    self.apply_keyed_update(key, item);
                }
            }
            r = self.subscribing.next().fuse() => {
                let res = r.ok_or_else(|| anyhow!("subscribing rx dropped"))?;
                match res {
                    Ok((key, updates)) => {
                        self.updates.insert(key, updates);
                    }
                    Err(market_id) => {
                        self.handles.lock().remove(&market_id);
                    }
                }
            }
        }
        Ok(())
    }

    // updates would hotloop with Ready(None) if empty
    async fn next_update(
        updates: &mut StreamMap<StreamKey, Streaming<L2BookUpdate>>,
    ) -> Option<(StreamKey, Result<L2BookUpdate, Status>)> {
        if updates.is_empty() {
            std::future::pending().await
        } else {
            updates.next().await
        }
    }

    async fn apply_subscribe_or_unsubscribe(
        &mut self,
        action: SubscribeOrUnsubscribe<MarketId>,
    ) {
        match action {
            SubscribeOrUnsubscribe::Subscribe(market_id, tx) => {
                let res = self.subscribe(market_id).await;
                if let Err(e) = &res {
                    error!("error subscribing to {}: {:?}", market_id, e);
                }
                if let Some(tx) = tx {
                    // don't care if nobody is listening
                    let _ = tx.send(res);
                }
            }
            SubscribeOrUnsubscribe::Unsubscribe(market_id) => self.unsubscribe(market_id),
        }
    }

    fn apply_keyed_update(&mut self, key: StreamKey, up: Result<L2BookUpdate, Status>) {
        if Arc::weak_count(&key.alive) <= 3 {
            // nobody using this handle other than ourselves
            // (1) in self.handles,
            // (2) in self.updates
            // (3) in the cloned StreamKey here
            if let Some(cooldown_period) = self.cooldown_before_unsubscribe {
                if self.cooler.len() < self.cooldown_max_tasks {
                    // to ensure we only cooler a subscription once,
                    // clone the handle to bump its weak count, holding
                    // it inside a timer task.  the driver will end up
                    // either retaining it, or unsubscribing it.
                    // #[cfg(feature = "tokio")]
                    let handle = key.handle.clone();
                    self.cooler.push(Box::pin(async move {
                        tokio::time::sleep(cooldown_period).await;
                        handle
                    }));
                    return;
                }
            }
            self.unsubscribe(key.market_id);
            return;
        }
        let up = match up {
            Ok(up) => up,
            Err(e) => {
                error!("error on L2 book update stream for {}: {:?}", key.market_id, e);
                self.unsubscribe(key.market_id);
                return;
            }
        };
        let mut do_unsubscribe = false;
        if let Err(e) = { key.state.lock().apply_update(up) } {
            error!("error applying L2 book update to {}: {:?}", key.market_id, e);
            do_unsubscribe = true;
        }
        if do_unsubscribe {
            self.unsubscribe(key.market_id);
        }
    }

    pub async fn subscribe(&mut self, market_id: MarketId) -> Result<L2ClientHandle> {
        let (ready, alive, handle) = {
            let mut handles = self.handles.lock();
            if let Some(handle) = handles.get(&market_id) {
                return Ok(handle.clone());
            }
            let ready = SyncHandle::new(false);
            let alive = Arc::new(());
            let handle = L2ClientHandle {
                market_id,
                state: Arc::new(Mutex::new(L2ClientState::default())),
                ready: ready.synced(),
                alive: Arc::downgrade(&alive),
            };
            handles.insert(market_id, handle.clone());
            (ready, alive, handle)
        };
        // TODO: endpoint resolution
        let client = MarketdataClient::connect("http://127.0.0.1:9000").await?;
        let handle_for_return = handle.clone();
        self.subscribing.push(Box::pin(async move {
            match Self::subscribe_inner(client, ready, alive, handle).await {
                Ok(key_and_updates) => Ok(key_and_updates),
                Err(e) => {
                    error!("error subscribing to {}: {:?}", market_id, e);
                    Err(market_id)
                }
            }
        }));
        Ok(handle_for_return)
    }

    async fn subscribe_inner(
        mut client: MarketdataClient<Channel>,
        ready: SyncHandle<bool>,
        alive: Arc<()>,
        handle: L2ClientHandle,
    ) -> Result<(StreamKey, Streaming<L2BookUpdate>)> {
        let market_id = handle.market_id;
        let mut updates = client
            .subscribe_l2_book_updates(SubscribeL2BookUpdatesRequest {
                market_id: Some(market_id),
                symbol: None,
            })
            .await?
            .into_inner();
        let first_update = updates.next().await.ok_or(anyhow!("no first update"))??;
        if !first_update.is_snapshot() {
            // speculative loading failed, driver will remove the handle
            bail!("received diff before snapshot on L2 book update stream");
        } else {
            handle.state.lock().apply_update(first_update)?;
            ready.set(true);
            let key = StreamKey { handle, alive };
            // driver will insert the updates into the stream map
            Ok((key, updates))
        }
    }

    pub fn unsubscribe(&mut self, market_id: MarketId) {
        if self.updates.remove(&market_id).is_none() {
            warn!(
                "unsubscribing from market {}, but it wasn't subscribed to begin with",
                market_id
            );
        }
        self.handles.lock().remove(&market_id);
    }

    pub fn get(&self, market_id: MarketId) -> Option<L2ClientHandle> {
        let handles = self.handles.lock();
        handles.get(&market_id).cloned()
    }
}

#[derive(Clone)]
pub struct ManagedL2ClientsHandle {
    handles: Arc<Mutex<FxHashMap<MarketId, L2ClientHandle>>>,
    tx_subs: mpsc::Sender<SubscribeOrUnsubscribe<MarketId>>,
}

impl ManagedL2ClientsHandle {
    pub async fn subscribe(&mut self, market_id: MarketId) -> Result<L2ClientHandle> {
        let (tx, rx) = oneshot::channel();
        self.tx_subs.send(SubscribeOrUnsubscribe::Subscribe(market_id, Some(tx))).await?;
        rx.await?
    }

    pub fn subscribe_without_waiting(&mut self, market: MarketRef) -> Result<()> {
        self.tx_subs.try_send(SubscribeOrUnsubscribe::Subscribe(market.id, None))?;
        Ok(())
    }

    pub fn unsubscribe(&self, market_id: MarketId) -> Result<()> {
        self.tx_subs.try_send(SubscribeOrUnsubscribe::Unsubscribe(market_id))?;
        Ok(())
    }

    pub fn get(&self, market_id: MarketId) -> Option<L2ClientHandle> {
        let handles = self.handles.lock();
        handles.get(&market_id).cloned()
    }
}

#[derive(Clone)]
struct StreamKey {
    handle: L2ClientHandle,
    alive: Arc<()>,
}

impl Borrow<MarketId> for StreamKey {
    fn borrow(&self) -> &MarketId {
        &self.handle.market_id
    }
}

impl Deref for StreamKey {
    type Target = L2ClientHandle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl PartialEq for StreamKey {
    fn eq(&self, other: &Self) -> bool {
        self.handle.market_id == other.handle.market_id
    }
}

impl Eq for StreamKey {}

impl PartialOrd for StreamKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.handle.market_id.partial_cmp(&other.handle.market_id)
    }
}

impl Ord for StreamKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.handle.market_id.cmp(&other.handle.market_id)
    }
}

impl Hash for StreamKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handle.market_id.hash(state);
    }
}
