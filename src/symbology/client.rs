/* Copyright 2023 Architect Financial Technologies LLC. This is free
 * software released under the GNU Affero Public License version 3. */
//! symbology client

use super::Txn;
use anyhow::{anyhow, bail, Result};
use api::symbology::{SymbologyUpdate, SymbologyUpdateKind};
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
    write_rpcs: Option<WriteRpcs>,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.task.abort()
    }
}

fn checked_result(v: Value) -> Result<()> {
    match v {
        Value::Ok => Ok(()),
        Value::Error(e) => Err(anyhow!(e)),
        _ => bail!("unexpected response"),
    }
}

macro_rules! checked_call {
    ($wr:expr, $proc:ident, $($name:ident: $arg:expr),*) => {
        if let Some(wr) = $wr {
            checked_result(wr.$proc.call([$((stringify!($name), $arg.try_into()?)),*]).await?)
        } else {
            Err(anyhow!("not in write mode"))
        }
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
    pub fn new(
        subscriber: Subscriber,
        base_path: Path,
        updates: Option<mpsc::UnboundedSender<SymbologyUpdate>>,
        write: bool,
    ) -> Self {
        let status = StatusCtx::new();
        let task = {
            let status = status.clone();
            let subscriber = subscriber.clone();
            let base_path = base_path.clone();
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
        let write_rpcs = if write {
            Some(WriteRpcs::new(&subscriber, base_path).unwrap())
        } else {
            None
        };
        Client { task, status, write_rpcs }
    }

    /// Return the current status of the symbology and a channel of status changes
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

    pub async fn squash(&self) -> Result<()> {
        if let Some(wr) = &self.write_rpcs {
            checked_result(wr.squash.call::<_, &str>([]).await?)
        } else {
            Err(anyhow!("not in write mode"))
        }
    }

    pub async fn apply_updates(&self, up: Vec<SymbologyUpdateKind>) -> Result<()> {
        checked_call!(&self.write_rpcs, apply_updates, updates: up)
    }

    pub async fn add_venue(&self, venue: api::symbology::Venue) -> Result<()> {
        checked_call!(&self.write_rpcs, add_venue, venue: venue)
    }

    pub async fn add_route(&self, route: api::symbology::Route) -> Result<()> {
        checked_call!(&self.write_rpcs, add_route, route: route)
    }

    pub async fn add_product(&self, product: api::symbology::Product) -> Result<()> {
        checked_call!(&self.write_rpcs, add_product, product: product)
    }

    pub async fn add_market(&self, market: api::symbology::Market) -> Result<()> {
        checked_call!(&self.write_rpcs, add_market, market: market)
    }

    pub async fn remove_venue(&self, id: api::symbology::VenueId) -> Result<()> {
        checked_call!(&self.write_rpcs, remove_venue, id: id)
    }

    pub async fn remove_route(&self, id: api::symbology::RouteId) -> Result<()> {
        checked_call!(&self.write_rpcs, remove_route, id: id)
    }

    pub async fn remove_product(&self, id: api::symbology::ProductId) -> Result<()> {
        checked_call!(&self.write_rpcs, remove_product, id: id)
    }

    pub async fn remove_market(&self, id: api::symbology::MarketId) -> Result<()> {
        checked_call!(&self.write_rpcs, remove_market, id: id)
    }
}

struct WriteRpcs {
    squash: Proc,
    apply_updates: Proc,
    add_venue: Proc,
    add_route: Proc,
    add_product: Proc,
    add_market: Proc,
    remove_venue: Proc,
    remove_route: Proc,
    remove_product: Proc,
    remove_market: Proc,
}

impl WriteRpcs {
    pub fn new(subscriber: &Subscriber, base: Path) -> Result<Self> {
        let squash = Proc::new(subscriber, base.append("squash"))?;
        let apply_updates = Proc::new(subscriber, base.append("apply-updates"))?;
        let add_venue = Proc::new(subscriber, base.append("add-venue"))?;
        let add_route = Proc::new(subscriber, base.append("add-route"))?;
        let add_product = Proc::new(subscriber, base.append("add-product"))?;
        let add_market = Proc::new(subscriber, base.append("add-market"))?;
        let remove_venue = Proc::new(subscriber, base.append("remove-venue"))?;
        let remove_route = Proc::new(subscriber, base.append("remove-route"))?;
        let remove_product = Proc::new(subscriber, base.append("remove-product"))?;
        let remove_market = Proc::new(subscriber, base.append("remove-market"))?;
        Ok(WriteRpcs {
            squash,
            apply_updates,
            add_venue,
            add_route,
            add_product,
            add_market,
            remove_venue,
            remove_route,
            remove_product,
            remove_market,
        })
    }
}
