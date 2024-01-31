//! Upgrades the OrderflowClient with some Oms specific functionality.

use crate::{orderflow::OrderflowClient, ChannelDriver, Common};
use anyhow::Result;
use api::{oms::*, orderflow::*, ComponentId, MaybeSplit, TypedMessage};
use chrono::{DateTime, Utc};
use fxhash::FxHashMap;
use log::{debug, warn};
use std::sync::Arc;
use tokio::sync::oneshot;
use uuid::Uuid;

pub struct OmsClient {
    pub orderflow: OrderflowClient,
    last_order_update: FxHashMap<OrderId, (DateTime<Utc>, OmsOrderUpdate)>,
    get_fills_requests:
        FxHashMap<Uuid, oneshot::Sender<Result<GetFillsResponse, GetFillsError>>>,
}

impl OmsClient {
    /// Connect to a component that implements an orderflow interface.  If no target is specified,
    /// search for an "Oms" component in the config.  If no channel authority is specified, search
    /// for a "ChannelAuthority" component in the config.
    pub async fn connect(
        common: &Common,
        driver: Arc<ChannelDriver>,
        order_authority: Option<ComponentId>,
        order_id_range: Option<u64>,
        target: Option<ComponentId>,
    ) -> Result<Self> {
        let orderflow = OrderflowClient::connect(
            &common,
            driver,
            order_authority,
            order_id_range,
            target,
        )
        .await?;
        Ok(Self {
            orderflow,
            last_order_update: FxHashMap::default(),
            get_fills_requests: FxHashMap::default(),
        })
    }

    pub fn get_fills(
        &mut self,
        order_id: OrderId,
    ) -> Result<oneshot::Receiver<Result<GetFillsResponse, GetFillsError>>> {
        let (tx, rx) = oneshot::channel();
        let request_id = Uuid::new_v4();
        self.get_fills_requests.insert(request_id, tx);
        self.orderflow.send(OmsMessage::GetFills(request_id, order_id))?;
        Ok(rx)
    }

    /// Drive this receiver in a loop to continuously update the state of orders
    pub async fn next(&mut self) -> Result<Vec<OmsOrderUpdate>> {
        let mut updates = vec![];
        let batch = self.orderflow.driver().subscribe().recv().await?;
        let now = Utc::now();
        for env in batch.iter() {
            debug!("received message: {:?}", env.msg);
            if let Ok((_, msg)) =
                TryInto::<MaybeSplit<TypedMessage, OmsMessage>>::try_into(env.msg.clone())
                    .map(MaybeSplit::parts)
            {
                match msg {
                    OmsMessage::OrderUpdate(up) => {
                        if up.state.contains(OrderStateFlags::Out) {
                            self.last_order_update.remove(&up.order_id);
                        } else {
                            self.last_order_update.insert(up.order_id, (now, up));
                        }
                        updates.push(up);
                    }
                    OmsMessage::GetFillsResponse(request_id, res) => {
                        if let Some(waiter) = self.get_fills_requests.remove(&request_id)
                        {
                            let _ = waiter.send(res);
                        }
                    }
                    _ => (),
                }
            } else {
                warn!("ignoring message: {:?}", env.msg);
            }
        }
        Ok(updates)
    }
}
