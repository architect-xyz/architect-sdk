//! A device for knowing if a subscription is synced.

use anyhow::{bail, Result};
use std::time::Duration;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct Synced<T>(watch::Sender<T>);

impl<T> Synced<T> {
    pub fn new(value: T) -> Self {
        let (tx, _) = watch::channel(value);
        Self(tx)
    }

    pub fn set(&self, value: T) {
        self.0.send_replace(value);
    }

    pub fn handle(&self) -> SyncedHandle<T> {
        SyncedHandle(self.0.subscribe())
    }
}

#[derive(Clone)]
pub struct SyncedHandle<T>(pub watch::Receiver<T>);

impl<T> SyncedHandle<T> {
    pub async fn wait_synced_with(
        &mut self,
        timeout: Option<Duration>,
        f: impl FnMut(&T) -> bool,
    ) -> Result<()> {
        if let Some(timeout) = timeout {
            if tokio::time::timeout(timeout, self.0.wait_for(f)).await.is_err() {
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

impl SyncedHandle<bool> {
    pub fn is_synced(&self) -> bool {
        *self.0.borrow()
    }

    pub async fn wait_synced(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.wait_synced_with(timeout, |v| *v).await
    }
}

impl SyncedHandle<u64> {
    pub async fn wait_synced(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.wait_synced_with(timeout, |v| *v > 0).await
    }
}

impl<T> SyncedHandle<Option<T>> {
    pub async fn wait_synced(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.wait_synced_with(timeout, |v| v.is_some()).await
    }
}
