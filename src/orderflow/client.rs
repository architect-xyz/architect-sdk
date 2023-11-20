use crate::{Common, ComponentDriver};
use anyhow::Result;
use api::{orderflow::*, ComponentId};
use log::info;
use uuid::Uuid;

pub struct Client {
    common: Common,
    channel_authority: ComponentDriver,
    channel_id: ChannelId,
    order_ids: OrderIdGenerator,
}

impl Client {
    pub async fn connect(common: Common, channel_authority: ComponentId) -> Result<Self> {
        info!("requesting channel id from channel authority...");
        let mut channel_authority =
            ComponentDriver::new(common.clone(), channel_authority);
        let channel_id = channel_authority
            .request_and_wait_for(
                ChannelAuthorityMessage::RequestChannelId(Uuid::new_v4()),
                |msg| msg.channel_id(),
            )
            .await?;
        let order_ids = OrderIdGenerator::channel(channel_id)?;
        Ok(Self { common, channel_authority, channel_id, order_ids })
    }

    // TODO: think harder about the intf of the orderflow client; supporting different topologies
    // and different message types...for now just using this for debug/test
    pub fn next_order_id(&self) -> OrderId {
        self.order_ids.next()
    }
}
