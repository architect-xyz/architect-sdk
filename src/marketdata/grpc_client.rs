use super::book_client::LevelBook;
use anyhow::{anyhow, bail, Context, Result};
use api::{
    external::marketdata::*, grpc::json_service::marketdata_client::MarketdataClient,
    symbology::MarketId,
};
use futures::StreamExt;
use log::debug;
use std::sync::Arc;
use tokio::sync::Mutex;

// TODO: rename?
pub struct L2Client {
    pub sequence: SequenceIdAndNumber,
    pub book: LevelBook,
}

// TODO: this has some weird semantics
impl L2Client {
    pub async fn subscribe(
        grpc_endpoint: String,
        market_id: MarketId,
    ) -> Result<Arc<Mutex<Self>>> {
        let mut grpc_client = MarketdataClient::connect(grpc_endpoint).await?;
        let mut updates = grpc_client
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
                Arc::new(Mutex::new(Self {
                    sequence: snap.sequence,
                    book: LevelBook::of_l2_book_snapshot(snap)?,
                }))
            }
            L2BookUpdate::Diff(..) => {
                bail!("received diff before snapshot on L2 book update stream");
            }
        };
        {
            let state = state.clone();
            tokio::task::spawn(async move {
                while let Some(Ok(update)) = updates.next().await {
                    let mut state = state.lock().await;
                    state.apply_update(update).context("applying book diff")?;
                }
                Ok::<_, anyhow::Error>(())
            });
        }
        Ok(state)
    }

    fn apply_update(&mut self, update: L2BookUpdate) -> Result<()> {
        match update {
            L2BookUpdate::Snapshot(snap) => {
                debug!("processing new snapshot: {:?}", snap);
                self.sequence = snap.sequence;
                self.book = LevelBook::of_l2_book_snapshot(snap)?;
            }
            L2BookUpdate::Diff(diff) => {
                let L2BookDiff { sequence, ref bids, ref asks, .. } = diff;
                if !sequence.is_next_in_sequence(&self.sequence) {
                    bail!(
                        "feed sequence numbers out of sync: expected {:?}, got {:?}",
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
            }
        }
        Ok(())
    }
}
