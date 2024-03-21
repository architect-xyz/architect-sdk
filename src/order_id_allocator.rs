//! One particular implementation of an order id allocator.

use crate::{ChannelDriver, Common};
use anyhow::{anyhow, bail, Result};
use api::{
    expect_response,
    orderflow::{OrderAuthorityMessage, OrderIdAllocation},
    ComponentId, OrderId,
};
use log::info;
use maybe_owned::MaybeOwned;
use netidx_derive::Pack;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

pub struct OrderIdAllocatorBuilder<'a> {
    common: &'a Common,
    driver: Option<&'a ChannelDriver>,
    order_authority: Option<ComponentId>,
    order_id_range: Option<u64>,
}

impl<'a> OrderIdAllocatorBuilder<'a> {
    pub fn new(common: &'a Common) -> Self {
        Self { common, driver: None, order_authority: None, order_id_range: None }
    }

    pub fn driver(mut self, driver: &'a ChannelDriver) -> Self {
        self.driver = Some(driver);
        self
    }

    pub fn order_authority(mut self, order_authority: ComponentId) -> Self {
        self.order_authority = Some(order_authority);
        self
    }

    pub fn order_id_range(mut self, order_id_range: u64) -> Self {
        self.order_id_range = Some(order_id_range);
        self
    }

    pub async fn get_allocation(self) -> Result<OrderIdAllocation> {
        OrderIdAllocator::get_allocation(
            self.common,
            self.driver,
            self.order_authority,
            self.order_id_range,
        )
        .await
    }
}

#[derive(Debug, Pack, Serialize, Deserialize)]
pub struct OrderIdAllocator {
    next_order_id: u64,
    allocation_max: u64,
}

impl From<OrderIdAllocation> for OrderIdAllocator {
    fn from(allocation: OrderIdAllocation) -> Self {
        Self {
            next_order_id: allocation.allocation_min,
            allocation_max: allocation.allocation_max,
        }
    }
}

impl OrderIdAllocator {
    pub async fn get_allocation(
        common: &Common,
        driver: Option<&ChannelDriver>,
        order_authority: Option<ComponentId>,
        order_id_range: Option<u64>,
    ) -> Result<OrderIdAllocation> {
        use OrderAuthorityMessage::*;
        let order_authority = order_authority
            .or_else(|| {
                info!("no order authority specified; searching for one in config...");
                common.get_local_component_of_kind("OrderAuthority")
            })
            .ok_or_else(|| anyhow!("no order authority found"))?;
        let order_authority_path = common.paths.component(order_authority)?;
        let order_id_range = order_id_range.unwrap_or(0x100000);
        let driver = match driver {
            Some(driver) => {
                if *driver.path() != order_authority_path {
                    bail!("channel driver not connected to order authority core");
                }
                MaybeOwned::Borrowed(driver)
            }
            None => {
                let mut driver = ChannelDriver::new(
                    &common.subscriber,
                    common.paths.component(order_authority).unwrap(),
                    None,
                );
                driver.wait_connected().await?;
                MaybeOwned::Owned(driver)
            }
        };
        let allocation: OrderIdAllocation = driver
            .request_and_wait_for(
                order_authority,
                RequestAllocation(Uuid::new_v4(), order_id_range),
                expect_response!(Allocation(_, Some(allocation)) => allocation),
            )
            .await?;
        Ok(allocation)
    }

    pub fn next(&mut self) -> Result<OrderId> {
        if self.next_order_id >= self.allocation_max {
            bail!("order id allocation exhausted")
        }
        let oid = OrderId::new_unchecked(self.next_order_id);
        self.next_order_id += 1;
        Ok(oid)
    }
}

pub mod atomic {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug)]
    pub struct AtomicOrderIdAllocator {
        next_order_id: AtomicU64,
        allocation_max: u64,
    }

    impl From<OrderIdAllocation> for AtomicOrderIdAllocator {
        fn from(allocation: OrderIdAllocation) -> Self {
            Self {
                next_order_id: AtomicU64::new(allocation.allocation_min),
                allocation_max: allocation.allocation_max,
            }
        }
    }

    impl AtomicOrderIdAllocator {
        pub fn next(&self) -> Result<OrderId> {
            let oid = self.next_order_id.fetch_add(1, Ordering::Relaxed);
            if oid > self.allocation_max {
                bail!("order id allocation exhausted")
            }
            Ok(OrderId::new_unchecked(oid))
        }
    }
}
