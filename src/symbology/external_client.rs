//! External/plugin symbology client--slightly different wire protocol
//! from the normal symbology client.

use super::Txn;
use crate::{
    external_driver::ExternalDriver,
    synced::{SyncHandle, Synced},
};
use anyhow::Result;
use api::external::symbology::*;
use chrono::{DateTime, Utc};
use log::error;
use std::time::Duration;
use tokio::task;
use url::Url;

#[derive(Debug)]
pub struct ExternalClient {
    task: task::JoinHandle<()>,
    sync_handle: SyncHandle<Option<DateTime<Utc>>>,
}

impl Drop for ExternalClient {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ExternalClient {
    pub fn start(url: Url) -> Self {
        let sync_handle = SyncHandle::new(None);
        let task = {
            let sync_handle = sync_handle.clone();
            task::spawn(async move {
                loop {
                    if let Err(e) = Self::run(url.clone(), sync_handle.clone()).await {
                        error!(
                            "external symbology client error, restarting in 5s: {e:?}"
                        );
                        tokio::time::sleep(Duration::from_secs(5)).await
                    }
                }
            })
        };
        Self { task, sync_handle }
    }

    pub fn synced(&self) -> Synced<Option<DateTime<Utc>>> {
        self.sync_handle.synced()
    }

    async fn run(url: Url, sync_handle: SyncHandle<Option<DateTime<Utc>>>) -> Result<()> {
        let driver = ExternalDriver::connect(url).await?;
        let snap: SymbologySnapshot =
            driver.query("symbology/snapshot", None::<()>).await?;
        let mut txn = Txn::begin();
        for route in snap.routes {
            txn.add_route(route)?;
        }
        for venue in snap.venues {
            txn.add_venue(venue)?;
        }
        // CR alee: respect product dependencies
        for product in snap.products {
            txn.add_product(product)?;
        }
        for market in snap.markets {
            txn.add_market(market)?;
        }
        txn.commit()?;
        sync_handle.set(Some(snap.epoch));
        std::future::pending::<()>().await;
        Ok(())
    }
}
