use super::book_client::LevelBook;
use crate::synced::{SyncHandle, Synced};
use anyhow::{anyhow, bail, Result};
use api::{
    external::marketdata::*, grpc::json_service::marketdata_client::MarketdataClient,
    symbology::MarketId, utils::sequence::SequenceIdAndNumber,
};
use futures::StreamExt;
use log::{debug, error};
use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use tonic::Streaming;

/// L2 book client for a single symbol.  No retries/reconnection logic; it just
/// subscribes to and maintains the book.
///
/// To use this, call `L2Client::connect` to construct a client, drive its
/// `next` method to apply updates, and use the accessors on `L2Client` to access
/// the current state of the book.
///
/// If you need multiple readers/consumers of this client, use `L2Client::handle()`
/// to create a cheaply cloneable handle to pass around.
pub struct L2Client {
    market_id: MarketId,
    updates: Streaming<L2BookUpdate>,
    state: Arc<Mutex<L2ClientState>>,
    ready: SyncHandle<bool>,
    alive: Arc<()>,
}

impl L2Client {
    pub async fn connect<D>(endpoint: D, market_id: MarketId) -> Result<Self>
    where
        D: TryInto<tonic::transport::Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let mut client = MarketdataClient::connect(endpoint).await?;
        let mut updates = client
            .subscribe_l2_book_updates(SubscribeL2BookUpdatesRequest { market_id })
            .await?
            .into_inner();
        // Simple, non-buffering version of the client; we trust the server to send
        // us the snapshot first, and then the diffs in sequence order.  If that's
        // not the case, bail.
        let first_update = updates.next().await.ok_or(anyhow!("no first update"))??;
        let state = match first_update {
            L2BookUpdate::Snapshot(snap) => {
                debug!("subscribed to stream, first update: {:?}", snap);
                L2ClientState {
                    sequence: snap.sequence,
                    book: LevelBook::of_l2_book_snapshot(snap)?,
                }
            }
            L2BookUpdate::Diff(..) => {
                bail!("received diff before snapshot on L2 book update stream");
            }
        };
        Ok(Self {
            market_id,
            updates,
            state: Arc::new(Mutex::new(state)),
            ready: SyncHandle::new(true), // already got snapshot
            alive: Arc::new(()),
        })
    }

    pub async fn next(&mut self) -> Option<SequenceIdAndNumber> {
        let update = match self.updates.next().await? {
            Ok(update) => update,
            Err(e) => {
                error!("error on L2 book update stream for {}: {:?}", self.market_id, e);
                return None;
            }
        };
        match self.apply_update(update) {
            Ok(sin) => Some(sin),
            Err(e) => {
                error!("error applying L2 book update for {}: {:?}", self.market_id, e);
                return None;
            }
        }
    }

    pub fn handle(&self) -> L2ClientHandle {
        L2ClientHandle {
            market_id: self.market_id,
            state: self.state.clone(),
            ready: self.ready.synced(),
            alive: Arc::downgrade(&self.alive),
        }
    }

    pub fn sequence(&self) -> SequenceIdAndNumber {
        self.state.lock().sequence
    }

    pub fn book(&self) -> MappedMutexGuard<LevelBook> {
        let guard = self.state.lock();
        MutexGuard::map(guard, |state| &mut state.book)
    }

    pub(super) fn apply_update(
        &mut self,
        update: L2BookUpdate,
    ) -> Result<SequenceIdAndNumber> {
        let mut state = self.state.lock();
        state.apply_update(update)
    }
}

/// Handle to an `L2Client` that can be cheaply cloned.  
///
/// If still waiting on the first snapshot, `is_ready` will return false.
/// If the driving `L2Client` is dropped, `is_alive` will return false.
///
/// In either case, accessor functions will return `None`.
#[derive(Clone)]
pub struct L2ClientHandle {
    pub(super) market_id: MarketId,
    pub(super) state: Arc<Mutex<L2ClientState>>,
    pub(super) ready: Synced<bool>,
    pub(super) alive: Weak<()>,
}

impl L2ClientHandle {
    pub fn is_ready(&self) -> bool {
        self.ready.is_synced()
    }

    pub async fn wait_ready(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.ready.wait_synced(timeout).await
    }

    pub fn is_alive(&self) -> bool {
        self.alive.upgrade().is_some()
    }

    pub fn sequence(&self) -> Option<SequenceIdAndNumber> {
        if self.is_ready() && self.is_alive() {
            Some(self.state.lock().sequence)
        } else {
            None
        }
    }

    pub fn book(&self) -> Option<MappedMutexGuard<LevelBook>> {
        if self.is_ready() && self.is_alive() {
            let guard = self.state.lock();
            Some(MutexGuard::map(guard, |state| &mut state.book))
        } else {
            None
        }
    }
}

#[derive(Default)]
pub(super) struct L2ClientState {
    pub sequence: SequenceIdAndNumber,
    pub book: LevelBook,
}

impl L2ClientState {
    pub(super) fn apply_update(
        &mut self,
        update: L2BookUpdate,
    ) -> Result<SequenceIdAndNumber> {
        match update {
            L2BookUpdate::Snapshot(snap) => {
                debug!("processing new snapshot: {:?}", snap);
                self.sequence = snap.sequence;
                self.book = LevelBook::of_l2_book_snapshot(snap)?;
                Ok(self.sequence)
            }
            L2BookUpdate::Diff(diff) => {
                let L2BookDiff { sequence, ref bids, ref asks, .. } = diff;
                if !sequence.is_next_in_sequence(&self.sequence) {
                    bail!(
                        "feed sequence numbers out of sync: expected {}, got {}",
                        self.sequence,
                        sequence
                    );
                }
                self.sequence = sequence;
                self.book.timestamp = diff
                    .timestamp()
                    .ok_or_else(|| anyhow!("invalid timestamp on book update"))?;
                for (price, size) in bids {
                    if size.is_zero() {
                        let _ = self.book.buy.remove(price);
                    } else {
                        let _ = self.book.buy.insert(*price, *size);
                    }
                }
                for (price, size) in asks {
                    if size.is_zero() {
                        let _ = self.book.sell.remove(price);
                    } else {
                        let _ = self.book.sell.insert(*price, *size);
                    }
                }
                Ok(self.sequence)
            }
        }
    }
}
