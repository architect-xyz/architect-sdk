/* Copyright 2023 Architect Financial Technologies LLC. This is free
 * software released under the GNU Affero Public License version 3. */
//! symbology client

use super::Txn;
use anyhow::{bail, Result};
use api::symbology::SymbologyUpdate;
use bytes::Buf;
use futures::{channel::mpsc, prelude::*};
use log::{debug, error, warn};
use netidx::{
    pack::Pack,
    path::Path,
    subscriber::{Event, Subscriber, UpdatesFlags, Value},
};
use netidx_protocols::{call_rpc, rpc::client::Proc};
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast, task, time};

/// Apply the given symbology update [u] and optionally push symbology updates
/// to [f].  Returns [false] if a seqno skip was detected.
fn push_update(
    txn: &mut Option<Txn>,
    f: &Option<mpsc::UnboundedSender<SymbologyUpdate>>,
    seq: &mut u64,
    u: SymbologyUpdate,
) -> bool {
    if *seq > 0 && u.sequence_number > *seq + 1 {
        warn!("seqno skip detected, {} -> {}", *seq, u.sequence_number);
        // seqno skip
        return false;
    }
    if *seq < u.sequence_number {
        *seq = u.sequence_number;
        if let Some(txn) = txn {
            if let Err(e) = txn.apply(&u.kind) {
                warn!("could not process symbology update {:?}, {}", u, e);
            }
        }
        if let Some(f) = f {
            let _ = f.unbounded_send(u);
        }
    }
    true
}

async fn load_history(
    subscriber: &Subscriber,
    base: &Path,
    f: &Option<mpsc::UnboundedSender<SymbologyUpdate>>,
) -> Result<u64> {
    let mut seq = 0u64;
    debug!("loading history");
    let query_updates = Proc::new(&subscriber, base.append("query-updates"))?;
    debug!("calling query updates with {}", seq);
    match call_rpc!(query_updates, end: Value::Null).await? {
        Value::Bytes(mut history) => {
            debug!("received {} bytes of history", history.len());
            let mut txn = if f.is_none() { Some(Txn::begin()) } else { None };
            while history.has_remaining() {
                let u: SymbologyUpdate = Pack::decode(&mut history)?;
                push_update(&mut txn, f, &mut seq, u);
            }
            if let Some(txn) = txn {
                txn.commit();
            }
            debug!("history load finished with seqno = {seq}");
        }
        _ => bail!("protocol error"),
    }
    Ok(seq)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    /// we have the latest symbology, and we are connected
    CaughtUp,
    /// we are connected, but we are catching up to the latest symbology
    CatchingUp,
    /// The connection has failed, and we are trying to reestabilish it
    Restarting,
    /// The client is starting up, no symbology has been loaded yet
    StartingUp,
}

#[derive(Clone)]
struct StatusCtx {
    updates: broadcast::Sender<Status>,
    current: Arc<Mutex<Status>>,
}

impl StatusCtx {
    fn new() -> Self {
        let (updates, _) = broadcast::channel(100);
        Self { updates, current: Arc::new(Mutex::new(Status::StartingUp)) }
    }

    fn change(&self, new_status: Status) {
        let mut current = self.current.lock();
        *current = new_status;
        let _ = self.updates.send(new_status);
    }

    fn subscribe(&self) -> (Status, broadcast::Receiver<Status>) {
        let current = self.current.lock();
        (*current, self.updates.subscribe())
    }
}

async fn run(
    status: StatusCtx,
    subscriber: Subscriber,
    base: Path,
    f: Option<mpsc::UnboundedSender<SymbologyUpdate>>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel(3);
    let updates_sub = subscriber.subscribe(base.append("updates"));
    updates_sub.updates(UpdatesFlags::STOP_COLLECTING_LAST, tx.clone());
    let mut seqno: u64;
    loop {
        status.change(Status::CatchingUp);
        seqno = load_history(&subscriber, &base, &f).await?;
        status.change(Status::CaughtUp);
        'batch: while let Some(mut batch) = rx.next().await {
            let mut txn = if f.is_none() { Some(Txn::begin()) } else { None };
            for (_, e) in batch.drain(..) {
                match e {
                    Event::Unsubscribed => {
                        while let Ok(_) = rx.try_next() {}
                        status.change(Status::Restarting);
                        break 'batch;
                    }
                    Event::Update(v) => match v {
                        Value::Null => (),
                        Value::Bytes(_) => {
                            let up = v.cast_to::<SymbologyUpdate>()?;
                            if !push_update(&mut txn, &f, &mut seqno, up) {
                                warn!("seqno skip detected, restarting");
                                break 'batch;
                            }
                        }
                        _ => bail!("unexpected update type {}", v),
                    },
                }
            }
        }
    }
}

pub struct Client {
    task: task::JoinHandle<()>,
    status: StatusCtx,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.task.abort()
    }
}

impl Client {
    /// Connect to the symbology server and do one of two things,
    ///
    /// - if updates is Some, then simply give the feed of updates
    ///   from the symbology server to the updates channel.
    ///
    /// - if updates is None, then update the in process symbology
    /// database in the background.
    pub fn start(
        subscriber: Subscriber,
        base_path: Path,
        updates: Option<mpsc::UnboundedSender<SymbologyUpdate>>,
    ) -> Self {
        let status = StatusCtx::new();
        let task = {
            let status = status.clone();
            task::spawn(async move {
                loop {
                    if let Err(e) = run(
                        status.clone(),
                        subscriber.clone(),
                        base_path.clone(),
                        updates.clone(),
                    )
                    .await
                    {
                        error!("symbology client task died with {}", e);
                    }
                    status.change(Status::Restarting);
                    time::sleep(Duration::from_secs(5)).await
                }
            })
        };
        Client { task, status }
    }

    /// Return the current status of the symbology and a channel of
    /// status changes
    pub fn status(&self) -> (Status, broadcast::Receiver<Status>) {
        self.status.subscribe()
    }

    /// Wait for the symbology to download
    pub async fn wait_caught_up(&self) {
        let (mut st, mut rx) = self.status();
        loop {
            match st {
                Status::CaughtUp => break,
                _ => match rx.recv().await {
                    Ok(status) => st = status,
                    Err(_) => {
                        let (status, receiver) = self.status();
                        st = status;
                        rx = receiver;
                    }
                },
            }
        }
    }
}
