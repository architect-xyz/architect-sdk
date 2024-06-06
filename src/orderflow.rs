//! Simple orderflow client suitable for connecting to an Oms or directly
//! to a Cpty.  It handles connecting to an OrderflowAuthority, requesting
//! an order id range, and passing orderflow messages.

use crate::{
    AtomicOrderIdAllocator, ChannelDriver, Common, OrderIdAllocatorRequestBuilder,
};
use anyhow::{anyhow, bail, Result};
use api::{orderflow::*, ComponentId, TypedMessage};
use log::{info, warn};
use std::sync::Arc;
use tokio::sync::watch;

pub struct OrderflowClient {
    common: Common,
    driver: Arc<ChannelDriver>,
    order_ids_tx: Arc<watch::Sender<Option<AtomicOrderIdAllocator>>>,
    order_ids_rx: watch::Receiver<Option<AtomicOrderIdAllocator>>,
    order_authority: Option<ComponentId>,
    default_order_id_allocation: u64,
    target: ComponentId,
}

impl OrderflowClient {
    /// Connect to a component that implements an orderflow interface.  If no target is specified,
    /// search for an "Oms" component in the config.  If no order authority is specified, search
    /// for a "OrderAuthority" component in the config.
    ///
    /// If no order id range is specified, defaults to 100.
    pub fn new(
        common: &Common,
        driver: Arc<ChannelDriver>,
        order_authority: Option<ComponentId>,
        default_order_id_allocation: u64,
        target: Option<ComponentId>,
    ) -> Result<Self> {
        let (order_ids_tx, order_ids_rx) = watch::channel(None);
        let order_ids_tx = Arc::new(order_ids_tx);
        let target = target
            .or_else(|| {
                info!("no target specified; searching for an Oms in config...");
                common.get_component_of_kind("Oms")
            })
            .ok_or_else(|| anyhow!("no target found"))?;
        Ok(Self {
            common: common.clone(),
            driver,
            order_ids_tx,
            order_ids_rx,
            order_authority,
            default_order_id_allocation,
            target,
        })
    }

    pub async fn wait_allocated(&mut self) -> Result<()> {
        let _ = self.order_ids_rx.wait_for(|a| a.is_some()).await?;
        Ok(())
    }

    /// Get the next order id.  If exhausted, allocate on demand.
    ///
    /// Allocating new order ids incurs some delay--to get an immediate order id,
    /// pre-allocate a block of ids and use `next_allocated_order_id`.
    pub async fn next_order_id(&self) -> Result<OrderId> {
        let mut attempts = 0;
        loop {
            {
                let order_ids = self.order_ids_rx.borrow();
                if let Some(oids) = order_ids.as_ref() {
                    if let Ok(order_id) = oids.next() {
                        return Ok(order_id);
                    }
                }
            }
            if attempts > 0 {
                break;
            }
            warn!("order ids exhausted; allocating more...");
            self.allocate_order_ids(self.default_order_id_allocation).await?;
            attempts += 1;
        }
        bail!("unable to allocate order ids")
    }

    pub async fn allocate_order_ids(&self, range: u64) -> Result<()> {
        Self::do_allocate_order_ids(
            self.common.clone(),
            &self.driver,
            self.order_authority,
            range,
            self.order_ids_tx.clone(),
        )
        .await
    }

    async fn do_allocate_order_ids(
        common: Common,
        driver: &ChannelDriver,
        order_authority: Option<ComponentId>,
        range: u64,
        order_ids_tx: Arc<watch::Sender<Option<AtomicOrderIdAllocator>>>,
    ) -> Result<()> {
        driver.wait_connected().await?;
        let order_ids = OrderIdAllocatorRequestBuilder::new(&common)
            .driver(Some(&driver))
            .order_authority(order_authority)
            .order_id_range(range)
            .build()?
            .get_allocation()
            .await?;
        info!("order id range allocated: {:?}", order_ids);
        order_ids_tx.send(Some(order_ids.into()))?;
        Ok(())
    }

    /// Get the next allocated order id.  If exhausted, fail.
    pub fn next_allocated_order_id(&self) -> Result<OrderId> {
        let order_ids = self.order_ids_rx.borrow();
        match order_ids.as_ref() {
            Some(order_ids) => order_ids.next(),
            None => Err(anyhow!("order ids exhausted")),
        }
    }

    /// Send a message to the configured target.
    pub fn send<M>(&self, msg: M) -> Result<()>
    where
        M: Into<TypedMessage>,
    {
        self.driver.send_to(self.target, msg)
    }

    pub fn driver(&self) -> &ChannelDriver {
        &self.driver
    }
}
