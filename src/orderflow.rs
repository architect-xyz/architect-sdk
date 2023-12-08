//! Simple orderflow client suitable for connecting to an Oms
//! or directly to a Cpty.  It doesn't do much more than connect
//! to a channel authority and pass orderflow messages through.

use crate::{Common, ComponentDriver};
use anyhow::{anyhow, Result};
use api::{orderflow::*, ComponentId, Envelope, MaybeSplit, TypedMessage};
use log::{debug, info, warn};
use uuid::Uuid;

pub struct OrderflowClient {
    driver: ComponentDriver,
    order_ids: OrderIdGenerator,
}

impl OrderflowClient {
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
        Ok(Self { driver, order_ids })
    }

    pub fn next_order_id(&self) -> OrderId {
        self.order_ids.next()
    }

    pub fn send<M>(&mut self, msg: M) -> Result<()>
    where
        M: std::fmt::Debug + Into<TypedMessage>,
    {
        self.driver.send(Into::<TypedMessage>::into(msg));
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Vec<Envelope<TypedMessage>>> {
        self.driver.recv().await
    }

    /// Drive this receiver in a loop to continuously receive updates
    pub async fn next(&mut self) -> Result<Vec<OrderflowMessage>> {
        let mut updates = vec![];
        let mut batch = self.recv().await?;
        for env in batch.drain(..) {
            debug!("received message: {:?}", env.msg);
            if let Ok((_, msg)) =
                TryInto::<MaybeSplit<TypedMessage, OrderflowMessage>>::try_into(
                    env.msg.clone(),
                )
                .map(MaybeSplit::parts)
            {
                updates.push(msg);
            } else {
                warn!("ignoring message: {:?}", env.msg);
            }
        }
        Ok(updates)
    }
}
