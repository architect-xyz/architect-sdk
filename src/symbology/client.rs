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
                txn.commit()?;
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
    pub async fn start(
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
        let t = Client { task, status, write_rpcs };
        t.wait_caught_up().await;
        t
    }

    pub fn write_only(subscriber: &Subscriber, base_path: Path) -> Self {
        let task = tokio::task::spawn(async move { future::pending().await });
        let status = StatusCtx::new();
        let write_rpcs = WriteRpcs::new(subscriber, base_path).unwrap();
        Self { task, status, write_rpcs: Some(write_rpcs) }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::symbology::*;
    use api::{
        symbology::{
            market::TestMarketInfo,
            query::{DateQ, Query},
            Symbolic, SymbologyUpdateKind,
        },
        Str,
    };
    use chrono::Utc;
    use rust_decimal_macros::dec;

    /// Demonstrates a symbology bug that could happen.
    ///
    /// 1. Load FOO Product with multiplier 5 and FOO/USD Market whose base is FOO
    /// 2. Call Market::get("FOO/USD Market"), check that the base multiplier is 5
    /// 3. Update FOO Product with multiplier 10
    /// 4. Call Market::get("FOO/USD Market"), check that the base multiplier is 10
    ///
    /// Step 4 will fail.  It's intuitive and expected that if you call Market::get(..)
    /// that its return value won't mutate underneath you.  But calling Market::get(..)
    /// again _after_ an update should certainly reflect the new multiplier.
    #[test]
    fn test_update_related_product() -> Result<()> {
        let mut seq = 0u64;
        let mut sseq = 0u64;
        macro_rules! push {
            ($txn:expr, $u:expr) => {
                sseq += 1;
                if !push_update(
                    &mut $txn,
                    &None,
                    &mut seq,
                    SymbologyUpdate { sequence_number: sseq, kind: $u },
                ) {
                    bail!("seqno skip detected")
                }
            };
        }
        let mut txn = Some(Txn::begin());
        let foo_index = ProductRef::new("FOO Index", ProductKind::Index)?;
        push!(txn, SymbologyUpdateKind::AddProduct(foo_index));
        txn.unwrap().commit().unwrap();
        let route = RouteRef::new("DIRECT")?;
        let venue = VenueRef::new("TEST")?;
        let usd = ProductRef::new("USD", ProductKind::Fiat)?;
        let foo_index = ProductRef::get("FOO Index").unwrap();
        let foo = ProductRef::new(
            "FOO Future",
            ProductKind::Future {
                underlying: Some(foo_index),
                multiplier: Some(dec!(5)),
                expiration: None,
                instrument_type: None,
            },
        )?;
        let foo_usd = api::symbology::Market::exchange(
            &foo,
            &usd,
            &venue,
            &route,
            "foo_usd",
            api::symbology::MarketInfo::Test(TestMarketInfo {
                tick_size: dec!(1),
                step_size: dec!(1),
                is_delisted: false,
            }),
        )?;
        let mut txn = Some(Txn::begin());
        push!(txn, SymbologyUpdateKind::AddRoute(route));
        push!(txn, SymbologyUpdateKind::AddVenue(venue));
        push!(txn, SymbologyUpdateKind::AddProduct(usd.clone()));
        push!(txn, SymbologyUpdateKind::AddProduct(foo));
        push!(txn, SymbologyUpdateKind::AddMarket(foo_usd));
        txn.unwrap().commit().unwrap();
        let foo = ProductRef::get("FOO Future").unwrap();
        assert_eq!(foo.kind.multiplier(), dec!(5));
        let foo_usd = MarketRef::get("FOO Future/USD*TEST/DIRECT").unwrap();
        assert_eq!(foo_usd.base().unwrap().kind.multiplier(), dec!(5));
        let foo_index = ProductRef::get("FOO Index").unwrap();
        let foo2 = ProductRef::new(
            "FOO Future",
            ProductKind::Future {
                underlying: Some(foo_index),
                multiplier: Some(dec!(10)),
                expiration: None,
                instrument_type: None,
            },
        )?;
        let mut txn = Some(Txn::begin());
        push!(txn, SymbologyUpdateKind::AddProduct(foo2));
        txn.unwrap().commit().unwrap();
        let foo2 = ProductRef::get("FOO Future").unwrap();
        assert_eq!(foo2.kind.multiplier(), dec!(10));
        let foo_usd = MarketRef::get("FOO Future/USD*TEST/DIRECT").unwrap();
        assert_eq!(foo_usd.base().unwrap().kind.multiplier(), dec!(10));
        // also test product->product reference updates
        let updated_foo_index = ProductRef::new(
            "FOO Index",
            ProductKind::Equity, // nonsense change but just to test
        )?;
        let mut txn = Some(Txn::begin());
        push!(txn, SymbologyUpdateKind::AddProduct(updated_foo_index));
        txn.unwrap().commit().unwrap();
        let foo_index = ProductRef::get("FOO Index").unwrap();
        assert!(matches!(foo_index.kind, ProductKind::Equity));
        let foo_future = ProductRef::get("FOO Future").unwrap();
        assert!(matches!(
            foo_future.kind.underlying().unwrap().kind,
            ProductKind::Equity
        ));
        let foo_usd = MarketRef::get("FOO Future/USD*TEST/DIRECT").unwrap();
        assert!(matches!(
            foo_usd.base().unwrap().kind.underlying().unwrap().kind,
            ProductKind::Equity
        ));
        // test that MarketIndex has picked up the base kind change
        let res = MarketIndex::current().query(&Query::Base(foo_future.name()));
        assert_eq!(res.len(), 1);
        let res: Vec<_> = res.into_iter().collect();
        assert_eq!(
            res[0].base().unwrap().kind.underlying().unwrap().kind,
            ProductKind::Equity
        );
        Ok(())
    }

    /// Another symbology bug that could happen.
    ///
    /// 1. Load FOO Product with expiration now
    /// 2. Call MarketIndex::query(Query::Expiration(DateQ::On(now))), expect 1 result
    /// 3. Update FOO Product with expiration now + 1 day
    /// 4. Call MarketIndex::query(Query::Expiration(DateQ::On(now))), expect 0 results
    ///
    /// Step 4 will return the wrong thing.
    ///
    /// This one is easier to fix in theory, just be more careful about updating the index.
    #[test]
    fn test_update_market_index() -> Result<()> {
        let mut seq = 0u64;
        let mut sseq = 0u64;
        macro_rules! push {
            ($txn:expr, $u:expr) => {
                sseq += 1;
                if !push_update(
                    &mut $txn,
                    &None,
                    &mut seq,
                    SymbologyUpdate { sequence_number: sseq, kind: $u },
                ) {
                    bail!("seqno skip detected")
                }
            };
        }
        let route = RouteRef::new("DIRECT")?;
        let venue = VenueRef::new("TEST")?;
        let usd = ProductRef::new("USD", ProductKind::Fiat)?;
        let now = Utc::now();
        let foo = ProductRef::new(
            "FOO Future",
            ProductKind::Future {
                underlying: None,
                multiplier: None,
                expiration: Some(now),
                instrument_type: None,
            },
        )?;
        let foo_usd = api::symbology::Market::exchange(
            &foo,
            &usd,
            &venue,
            &route,
            "foo_usd",
            api::symbology::MarketInfo::Test(TestMarketInfo {
                tick_size: dec!(1),
                step_size: dec!(1),
                is_delisted: false,
            }),
        )?;
        let mut txn = Some(Txn::begin());
        push!(txn, SymbologyUpdateKind::AddRoute(route));
        push!(txn, SymbologyUpdateKind::AddVenue(venue));
        push!(txn, SymbologyUpdateKind::AddProduct(usd.clone()));
        push!(txn, SymbologyUpdateKind::AddProduct(foo));
        push!(txn, SymbologyUpdateKind::AddMarket(foo_usd));
        txn.unwrap().commit().unwrap();
        let res = MarketIndex::current().query(&Query::Expiration(DateQ::On(now)));
        // expecting exactly one result, "FOO Future/USD"
        assert_eq!(res.len(), 1);
        // update the expiration of FOO Future to some other time
        let foo2 = ProductRef::new(
            "FOO Future",
            ProductKind::Future {
                underlying: None,
                multiplier: None,
                expiration: Some(now + chrono::Duration::days(1)),
                instrument_type: None,
            },
        )?;
        let mut txn = Some(Txn::begin());
        push!(txn, SymbologyUpdateKind::AddProduct(foo2));
        txn.unwrap().commit().unwrap();
        // intuitively we expect this query to now have zero results
        let res = MarketIndex::current().query(&Query::Expiration(DateQ::On(now)));
        assert_eq!(res.len(), 0);
        // update the kind of foo2 to something else
        let foo3 = ProductRef::new("FOO Future", ProductKind::Equity)?;
        let mut txn = Some(Txn::begin());
        push!(txn, SymbologyUpdateKind::AddProduct(foo3));
        txn.unwrap().commit().unwrap();
        // check the kind of FOO Future which is now Equity was indexed
        let res = MarketIndex::current()
            .query(&Query::BaseKind(Str::try_from("Equity").unwrap()));
        assert_eq!(res.len(), 1);
        let res: Vec<_> = res.into_iter().collect();
        assert_eq!(res[0].base().unwrap().kind, ProductKind::Equity);
        Ok(())
    }
}
