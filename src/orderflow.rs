//! Simple orderflow client suitable for connecting to an Oms or directly
//! to a Cpty.  It handles connecting to an OrderflowAuthority, requesting
//! an order id range, and passing orderflow messages.

use crate::{AtomicOrderIdAllocator, ChannelDriver, Common, OrderIdAllocator};
use anyhow::{anyhow, Result};
use api::{orderflow::*, ComponentId, TypedMessage};
use log::info;
use std::sync::Arc;
use tokio::{sync::watch, task};

pub struct OrderflowClient {
    driver: Arc<ChannelDriver>,
    _order_ids_tx: Arc<watch::Sender<Option<AtomicOrderIdAllocator>>>,
    order_ids_rx: watch::Receiver<Option<AtomicOrderIdAllocator>>,
    target: ComponentId,
}

impl OrderflowClient {
    /// Connect to a component that implements an orderflow interface.  If no target is specified,
    /// search for an "Oms" component in the config.  If no order authority is specified, search
    /// for a "OrderAuthority" component in the config.
    ///
    /// If no order id range is specified, default to 2^20.
    pub fn new(
        common: &Common,
        driver: Arc<ChannelDriver>,
        order_authority: Option<ComponentId>,
        order_id_range: Option<u64>,
        target: Option<ComponentId>,
    ) -> Result<Self> {
        let (order_ids_tx, order_ids_rx) = watch::channel(None);
        let order_ids_tx = Arc::new(order_ids_tx);
        {
            let common = common.clone();
            let driver = driver.clone();
            let order_ids_tx = order_ids_tx.clone();
            task::spawn(async move {
                driver.wait_connected().await?;
                let order_ids = OrderIdAllocator::get_allocation(
                    &common,
                    Some(&driver),
                    order_authority,
                    order_id_range,
                )
                .await?;
                info!("order id range allocated: {:?}", order_ids);
                order_ids_tx.send(Some(order_ids.into()))?;
                Ok::<_, anyhow::Error>(())
            });
        }
        let target = target
            .or_else(|| {
                info!("no target specified; searching for an Oms in config...");
                common.get_component_of_kind("Oms")
            })
            .ok_or_else(|| anyhow!("no target found"))?;
        Ok(Self { driver, _order_ids_tx: order_ids_tx, order_ids_rx, target })
    }

    pub async fn wait_allocated(&mut self) -> Result<()> {
        let _ = self.order_ids_rx.wait_for(|a| a.is_some()).await?;
        Ok(())
    }

    pub fn next_order_id(&self) -> Result<OrderId> {
        let order_ids = self.order_ids_rx.borrow();
        match order_ids.as_ref() {
            Some(order_ids) => order_ids.next(),
            None => Err(anyhow!("no order ids")),
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