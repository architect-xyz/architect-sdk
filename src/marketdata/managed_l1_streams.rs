use crate::MarketdataSource;
use anyhow::{anyhow, Result};
use api::{
    grpc::json_service::marketdata_client::MarketdataClient, marketdata::*, symbology::*,
};
use futures::{select_biased, FutureExt, StreamExt};
use log::error;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamMap;
use tonic::{transport::Channel, Status, Streaming};

pub struct ManagedL1Streams {
    marketdata: Arc<MarketdataSource<Channel>>,
    updates: StreamMap<SubscriptionKey, Streaming<L1BookSnapshot>>,
    tx_subs: mpsc::Sender<SubscribeOrUnsubscribe<SubscriptionKey>>,
    rx_subs: mpsc::Receiver<SubscribeOrUnsubscribe<SubscriptionKey>>,
}

type SubscriptionKey = (MarketdataVenue, String);

enum SubscribeOrUnsubscribe<T> {
    Subscribe(T, Option<oneshot::Sender<Result<()>>>),
    Unsubscribe(T),
}

impl ManagedL1Streams {
    pub fn new(marketdata: Arc<MarketdataSource<Channel>>) -> Self {
        let (tx_subs, rx_subs) = mpsc::channel(1000);
        Self { marketdata, updates: StreamMap::new(), tx_subs, rx_subs }
    }

    pub fn handle(&self) -> ManagedL1StreamsHandle {
        ManagedL1StreamsHandle { tx_subs: self.tx_subs.clone() }
    }

    pub async fn next(
        &mut self,
    ) -> Result<Option<((MarketdataVenue, String), L1BookSnapshot)>> {
        select_biased! {
            r = self.rx_subs.recv().fuse() => {
                let action = r.ok_or_else(|| anyhow!("rx_subs dropped"))?;
                self.apply_subscribe_or_unsubscribe(action).await;
            }
            r = Self::next_update(&mut self.updates).fuse() => {
                if let Some((key, res)) = r {
                    match res {
                        Ok(item) => return Ok(Some((key, item))),
                        Err(e) => {
                            error!("error in l1 stream for {:?}: {e:?}", key);
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    // updates would hotloop with Ready(None) if empty
    async fn next_update(
        updates: &mut StreamMap<SubscriptionKey, Streaming<L1BookSnapshot>>,
    ) -> Option<(SubscriptionKey, Result<L1BookSnapshot, Status>)> {
        if updates.is_empty() {
            std::future::pending().await
        } else {
            updates.next().await
        }
    }

    async fn apply_subscribe_or_unsubscribe(
        &mut self,
        action: SubscribeOrUnsubscribe<SubscriptionKey>,
    ) {
        match action {
            SubscribeOrUnsubscribe::Subscribe((venue, symbol), tx) => {
                if let Some(channel) = self.marketdata.get(&venue) {
                    let mut client = MarketdataClient::new(channel.clone());
                    // CR alee: would be slightly more efficient if we could
                    // modify the snapshot requests on the fly?
                    match client
                        .subscribe_l1_book_snapshots(SubscribeL1BookSnapshotsRequest {
                            symbols: Some(vec![symbol.clone()]),
                        })
                        .await
                    {
                        Ok(res) => {
                            let stream = res.into_inner();
                            self.updates.insert((venue, symbol), stream);
                            if let Some(tx) = tx {
                                let _ = tx.send(Ok(()));
                            }
                        }
                        Err(e) => {
                            if let Some(tx) = tx {
                                let _ = tx.send(Err(e.into()));
                            }
                        }
                    }
                } else {
                    if let Some(tx) = tx {
                        let _ = tx.send(Err(anyhow!("no marketdata source for venue")));
                    }
                }
            }
            SubscribeOrUnsubscribe::Unsubscribe(key) => {
                self.updates.remove(&key);
            }
        }
    }
}

#[derive(Clone)]
pub struct ManagedL1StreamsHandle {
    tx_subs: mpsc::Sender<SubscribeOrUnsubscribe<SubscriptionKey>>,
}

impl ManagedL1StreamsHandle {
    pub async fn subscribe(
        &self,
        venue: &MarketdataVenue,
        symbol: impl AsRef<str>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx_subs
            .send(SubscribeOrUnsubscribe::Subscribe(
                (venue.clone(), symbol.as_ref().to_string()),
                Some(tx),
            ))
            .await?;
        rx.await??;
        Ok(())
    }

    pub fn subscribe_without_waiting(
        &self,
        venue: &MarketdataVenue,
        symbol: impl AsRef<str>,
    ) -> Result<()> {
        self.tx_subs.try_send(SubscribeOrUnsubscribe::Subscribe(
            (venue.clone(), symbol.as_ref().to_string()),
            None,
        ))?;
        Ok(())
    }

    pub fn unsubscribe(
        &self,
        venue: &MarketdataVenue,
        symbol: impl AsRef<str>,
    ) -> Result<()> {
        self.tx_subs.try_send(SubscribeOrUnsubscribe::Unsubscribe((
            venue.clone(),
            symbol.as_ref().to_string(),
        )))?;
        Ok(())
    }
}
