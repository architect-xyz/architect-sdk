//! A device for knowing if a subscription is synced.

use anyhow::{bail, Result};
use std::time::Duration;
use tokio::sync::watch;

// CR alee: switch the names of the Handle/non-Handle structs, and maybe
// reconsider the names of the methods and such.  This is really a thin
// layer over watch.
#[derive(Debug, Clone)]
pub struct SyncHandle<T>(watch::Sender<T>);

impl<T> SyncHandle<T> {
    pub fn new(value: T) -> Self {
        let (tx, _) = watch::channel(value);
        Self(tx)
    }

    pub fn set(&self, value: T) {
        self.0.send_replace(value);
    }

    pub fn synced(&self) -> Synced<T> {
        Synced(self.0.subscribe())
    }
}

#[derive(Clone)]
pub struct Synced<T>(pub watch::Receiver<T>);

impl<T> Synced<T> {
    pub async fn wait_synced_with(
        &mut self,
        timeout: Option<Duration>,
        f: impl FnMut(&T) -> bool,
    ) -> Result<()> {
        if let Some(timeout) = timeout {
            if let Err(_) = tokio::time::timeout(timeout, self.0.wait_for(f)).await {
                bail!("timed out waiting for book to sync");
            }
        } else {
            self.0.wait_for(f).await?;
        }
        Ok(())
    }

    pub async fn changed(&mut self) -> Result<()> {
        Ok(self.0.changed().await?)
    }
}

impl Synced<bool> {
    pub fn is_synced(&self) -> bool {
        *self.0.borrow()
    }

    pub async fn wait_synced(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.wait_synced_with(timeout, |v| *v).await
    }
}

impl Synced<u64> {
    pub async fn wait_synced(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.wait_synced_with(timeout, |v| *v > 0).await
    }
}

impl<T> Synced<Option<T>> {
    pub async fn wait_synced(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.wait_synced_with(timeout, |v| v.is_some()).await
    }
}
