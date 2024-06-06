//! One particular implementation of an order id allocator.

use crate::{ChannelDriver, Common};
use anyhow::{anyhow, bail, Result};
use api::{
    expect_response,
    orderflow::{OrderAuthorityMessage, OrderIdAllocation},
    ComponentId, OrderId, UserId,
};
use derive_builder::Builder;
use log::info;
use maybe_owned::MaybeOwned;
use netidx_derive::Pack;
use serde_derive::{Deserialize, Serialize};
use uuid::Uuid;

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
    pub fn next(&mut self) -> Result<OrderId> {
        if self.next_order_id >= self.allocation_max {
            bail!("order id allocation exhausted")
        }
        let oid = OrderId::new_unchecked(self.next_order_id);
        self.next_order_id += 1;
        Ok(oid)
    }
}

#[derive(Builder)]
pub struct OrderIdAllocatorRequest<'a> {
    common: &'a Common,
    /// Channel driver--if not provided, a new one will be created.
    driver: Option<&'a ChannelDriver>,
    /// Order authority component--defaults to first one found in common config.
    order_authority: Option<ComponentId>,
    /// Range of order ids to allocate.
    order_id_range: u64,
    /// Defaults to the channel driver's user id.
    user_id: Option<UserId>,
}

impl<'a> OrderIdAllocatorRequestBuilder<'a> {
    pub fn new(common: &'a Common) -> Self {
        Self {
            common: Some(common),
            driver: Some(None),
            order_authority: Some(None),
            order_id_range: None,
            user_id: Some(None),
        }
    }
}

impl<'a> OrderIdAllocatorRequest<'a> {
    pub async fn get_allocation(self) -> Result<OrderIdAllocation> {
        use OrderAuthorityMessage::*;
        let order_authority = self
            .order_authority
            .or_else(|| {
                info!("no order authority specified; searching for one in config...");
                self.common.get_component_of_kind("OrderAuthority")
            })
            .ok_or_else(|| anyhow!("no order authority found"))?;
        let order_authority_path = self.common.paths.channel(Some(order_authority))?;
        let order_id_range = self.order_id_range;
        let driver = match self.driver {
            Some(driver) => {
                if *driver.path() != order_authority_path {
                    bail!("channel driver not connected to order authority core");
                }
                MaybeOwned::Borrowed(driver)
            }
            None => {
                let driver = self
                    .common
                    .channel_driver()
                    .with_path(order_authority_path)
                    // CR alee: dumb hack to avoid cross-talk from user drivers,
                    // which are assumed to have a channel_id >= 1
                    .on_channel(0)
                    .build();
                driver.wait_connected().await?;
                MaybeOwned::Owned(driver)
            }
        };
        let user_id = match self.user_id {
            Some(user_id) => user_id,
            None => driver.user_id()?,
        };
        let allocation: OrderIdAllocation = driver
            .request_and_wait_for(
                order_authority,
                RequestAllocation(Uuid::new_v4(), user_id, order_id_range),
                expect_response!(Allocation(_, Some(allocation)) => allocation),
            )
            .await?;
        Ok(allocation)
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
