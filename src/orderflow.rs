//! Simple orderflow client suitable for connecting to an Oms or directly
//! to a Cpty.  It handles connecting to an OrderflowAuthority, requesting
//! an order id range, and passing orderflow messages.

use crate::{AtomicOrderIdAllocator, ChannelDriver, Common, OrderIdAllocator};
use anyhow::{anyhow, Result};
use api::{orderflow::*, ComponentId, TypedMessage};
use log::info;
use std::sync::Arc;

pub struct OrderflowClient {
    driver: Arc<ChannelDriver>,
    order_ids: AtomicOrderIdAllocator,
    target: ComponentId,
}

impl OrderflowClient {
    /// Connect to a component that implements an orderflow interface.  If no target is specified,
    /// search for an "Oms" component in the config.  If no order authority is specified, search
    /// for a "OrderAuthority" component in the config.
    ///
    /// If no order id range is specified, default to 2^20.
    pub async fn connect(
        common: &Common,
        driver: Arc<ChannelDriver>,
        order_authority: Option<ComponentId>,
        order_id_range: Option<u64>,
        target: Option<ComponentId>,
    ) -> Result<Self> {
        let order_ids: AtomicOrderIdAllocator =
            OrderIdAllocator::get_allocation_with_driver(
                &common,
                &driver,
                order_authority,
                order_id_range,
            )
            .await?
            .into();
        let target = target
            .or_else(|| {
                info!("no target specified; searching for an Oms in config...");
                common.get_component_of_kind("Oms")
            })
            .ok_or_else(|| anyhow!("no target found"))?;
        Ok(Self { driver, order_ids, target })
    }

    pub fn next_order_id(&self) -> Result<OrderId> {
        self.order_ids.next()
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
