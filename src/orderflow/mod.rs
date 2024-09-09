//! Simple orderflow client suitable for connecting to an Oms or directly
//! to a Cpty.  It handles tracking order ids and passing orderflow messages.

use crate::{AtomicOrderIdAllocator, ChannelDriver, Common};
use anyhow::{anyhow, Result};
use api::{orderflow::*, ComponentId, TypedMessage};
use log::info;
use std::sync::Arc;

pub mod oms;
pub mod order_id_allocator;

pub struct OrderflowClient {
    driver: Arc<ChannelDriver>,
    target: ComponentId,
    order_ids: Arc<AtomicOrderIdAllocator>,
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
        target: Option<ComponentId>,
        // if specified, resume order ids from the given seqid/seqno
        order_ids: Option<AtomicOrderIdAllocator>,
    ) -> Result<Self> {
        let target = target
            .or_else(|| {
                info!("no target specified; searching for an Oms in config...");
                common.get_component_of_kind("Oms")
            })
            .ok_or_else(|| anyhow!("no target found"))?;
        let order_ids = order_ids.unwrap_or_else(AtomicOrderIdAllocator::new);
        Ok(Self { driver, target, order_ids: Arc::new(order_ids) })
    }

    /// Get the next order id.
    pub fn next_order_id(&self) -> OrderId {
        self.order_ids.next_order_id()
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
