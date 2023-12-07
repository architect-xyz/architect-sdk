use crate::{Common, ComponentDriver};
use anyhow::{anyhow, bail, Result};
use api::{orderflow::*, ComponentId, MaybeSplit, TypedMessage};
use chrono::{DateTime, Utc};
use fxhash::FxHashMap;
use log::{debug, info, warn};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use uuid::Uuid;

pub struct OrderDetails {
    pub state: OrderState,
    pub sent: DateTime<Utc>,
    pub filled_qty: Decimal,
    pub fills: Vec<Result<Fill, AberrantFill>>,
}

pub struct Client {
    driver: ComponentDriver,
    order_ids: OrderIdGenerator,
    orders: FxHashMap<OrderId, OrderDetails>,
}

impl Client {
    /// Connect to a component that implements an orderflow interface.  If no target is specified,
    /// search for an "Oms" component in the config.  If no channel authority is specified, search
    /// for a "ChannelAuthority" component in the config.
    pub async fn connect(
        common: &Common,
        channel_authority: Option<ComponentId>,
        target: Option<ComponentId>,
    ) -> Result<Self> {
        let channel_authority = channel_authority
            .or_else(|| {
                info!("no channel authority specified; searching for one in config...");
                common
                    .config
                    .find_local_component_of_kind("ChannelAuthority")
                    .map(|(id, _)| id)
            })
            .ok_or_else(|| anyhow!("no channel authority found"))?;
        info!("requesting channel id from channel authority...");
        let mut channel_authority =
            ComponentDriver::connect(common, channel_authority).await?;
        let channel_id = channel_authority
            .request_and_wait_for(
                ChannelAuthorityMessage::RequestChannelId(Uuid::new_v4()),
                |msg: ChannelAuthorityMessage| msg.channel_id(),
            )
            .await?;
        let order_ids = OrderIdGenerator::channel(channel_id)?;
        let target = target
            .or_else(|| {
                info!("no target specified; searching for an Oms in config...");
                common.config.find_local_component_of_kind("Oms").map(|(id, _)| id)
            })
            .ok_or_else(|| anyhow!("no target found"))?;
        info!("connecting to target {target}...");
        let driver = ComponentDriver::connect(common, target).await?;
        Ok(Self { driver, order_ids, orders: FxHashMap::default() })
    }

    fn update(&mut self, msg: OrderflowMessage) {
        // CR alee: this is just mirroring Oms state, factor the state logic out
        // of the Oms component and use it here too for perfect syncing
        match msg {
            OrderflowMessage::Order(o) => {
                self.orders.insert(
                    o.id,
                    OrderDetails {
                        state: OrderStateFlags::Open.into(),
                        sent: Utc::now(),
                        filled_qty: dec!(0),
                        fills: vec![],
                    },
                );
            }
            OrderflowMessage::Cancel(c) => {
                if let Some(o) = self.orders.get_mut(&c.order_id) {
                    o.state |= OrderStateFlags::Canceling;
                }
            }
            OrderflowMessage::Reject(r) => {
                if let Some(o) = self.orders.get_mut(&r.order_id) {
                    o.state &= !OrderStateFlags::Open;
                    o.state |= OrderStateFlags::Rejected | OrderStateFlags::Out;
                }
            }
            OrderflowMessage::Ack(a) => {
                if let Some(o) = self.orders.get_mut(&a.order_id) {
                    o.state |= OrderStateFlags::Acked;
                }
            }
            OrderflowMessage::Fill(Ok(f)) => {
                if let Some(o) = self.orders.get_mut(&f.order_id) {
                    o.state |= OrderStateFlags::Filled;
                    o.filled_qty += f.quantity;
                    o.fills.push(Ok(f));
                }
            }
            OrderflowMessage::Fill(Err(af)) => {
                if let Some(oid) = af.order_id {
                    if let Some(o) = self.orders.get_mut(&oid) {
                        o.state |= OrderStateFlags::Filled;
                        if let Some(qty) = af.quantity {
                            o.filled_qty += qty;
                        }
                        o.fills.push(Err(af))
                    }
                }
            }
            OrderflowMessage::Out(o) => {
                if let Some(o) = self.orders.get_mut(&o.order_id) {
                    o.state &= !OrderStateFlags::Open;
                    o.state |= OrderStateFlags::Out;
                }
            }
        }
    }

    pub fn send<M>(&mut self, msg: M) -> Result<()>
    where
        M: std::fmt::Debug + Into<TypedMessage>,
        for<'a> &'a M: TryInto<OrderflowMessage>,
    {
        if let Ok(omsg) = TryInto::<OrderflowMessage>::try_into(&msg) {
            self.update(omsg);
        } else {
            bail!("invalid orderflow message: {:?}", msg)
        }
        self.driver.send(Into::<TypedMessage>::into(msg));
        Ok(())
    }

    pub fn next_order_id(&self) -> OrderId {
        self.order_ids.next()
    }

    /// Drive this receiver in a loop to continuously update the state of orders
    pub async fn next(&mut self) -> Result<()> {
        let mut batch = self.driver.recv().await?;
        for env in batch.drain(..) {
            debug!("received message: {:?}", env.msg);
            if let Ok((_, msg)) =
                TryInto::<MaybeSplit<TypedMessage, OrderflowMessage>>::try_into(
                    env.msg.clone(),
                )
                .map(MaybeSplit::parts)
            {
                self.update(msg);
            } else {
                warn!("ignoring message: {:?}", env.msg);
            }
        }
        Ok(())
    }

    pub fn is_open(&self, oid: OrderId) -> bool {
        self.orders.get(&oid).map_or(false, |o| o.state.contains(OrderStateFlags::Open))
    }

    pub fn is_acked(&self, oid: OrderId) -> bool {
        self.orders.get(&oid).map_or(false, |o| o.state.contains(OrderStateFlags::Acked))
    }

    pub fn is_filled(&self, oid: OrderId) -> bool {
        self.orders.get(&oid).map_or(false, |o| o.state.contains(OrderStateFlags::Filled))
    }

    pub fn is_canceled(&self, oid: OrderId) -> bool {
        self.orders
            .get(&oid)
            .map_or(false, |o| o.state.contains(OrderStateFlags::Canceled))
    }

    pub fn is_out(&self, oid: OrderId) -> bool {
        self.orders.get(&oid).map_or(false, |o| o.state.contains(OrderStateFlags::Out))
    }

    pub fn get_order_state(&self, oid: OrderId) -> Option<OrderState> {
        self.orders.get(&oid).map(|o| o.state)
    }

    pub fn get_filled_qty(&self, oid: OrderId) -> Option<Decimal> {
        self.orders.get(&oid).map(|o| o.filled_qty)
    }

    pub fn get_fills(&self, oid: OrderId) -> Option<&Vec<Result<Fill, AberrantFill>>> {
        self.orders.get(&oid).map(|o| &o.fills)
    }
}
