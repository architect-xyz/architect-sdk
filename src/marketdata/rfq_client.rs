use crate::{
    symbology::{Cpty, Route, Venue},
    Common,
};
use anyhow::Result;
use api::marketdata::*;
use futures::channel::mpsc;
use futures_util::StreamExt;
use fxhash::FxHashMap;
use log::error;
use netidx::{
    pool::Pooled,
    subscriber::{Dval, Event, SubId, UpdatesFlags, Value},
};
use netidx_protocols::{call_rpc, rpc::client::Proc};

pub struct RfqClient {
    common: Common,
    subs: FxHashMap<SubId, Dval>,
    index: FxHashMap<(Venue, Route, RfqRequest), SubId>,
    tx: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
    rx: mpsc::Receiver<Pooled<Vec<(SubId, Event)>>>,
}

impl RfqClient {
    pub fn new(common: &Common) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        Self {
            common: common.clone(),
            subs: FxHashMap::default(),
            index: FxHashMap::default(),
            tx,
            rx,
        }
    }

    pub fn clear(&mut self) {
        self.subs.clear();
    }

    pub async fn next(&mut self) -> Option<Vec<RfqResponse>> {
        if let Some(mut events) = self.rx.next().await {
            let mut res = vec![];
            for (sub_id, event) in events.drain(..) {
                match event {
                    Event::Unsubscribed => {
                        self.subs.remove(&sub_id);
                    }
                    Event::Update(Value::Null) => {}
                    Event::Update(value) => {
                        match serde_json::from_str::<RfqResponse>(
                            value.to_string_naked().as_str(),
                        ) {
                            Ok(r) => res.push(r),
                            Err(e) => error!("failed to parse RFQ response: {}", e),
                        }
                    }
                }
            }
            if res.is_empty() {
                None
            } else {
                Some(res)
            }
        } else {
            None
        }
    }

    /// Subscribe to an RFQ stream.
    ///
    /// If `reuse_existing` is true, re-use the subscription with the same
    /// parameters if one exists.
    pub async fn subscribe_rfq(
        &mut self,
        venue: Venue,
        route: Route,
        rfq: RfqRequest,
        reuse_existing: bool,
    ) -> Result<()> {
        if !self.index.contains_key(&(venue, route, rfq)) || !reuse_existing {
            let cpty = Cpty { venue, route };
            let api_path = self.common.paths.marketdata_api(cpty).append("subscribe-rfq");
            let proc = Proc::new_with_timeout(
                &self.common.subscriber,
                api_path,
                std::time::Duration::from_secs(2),
            )?;
            let res = call_rpc!(
                proc,
                base: format!("\"{}\"", rfq.base),
                quote: format!("\"{}\"", rfq.quote),
                quantity: rfq.quantity
            )
            .await?;
            let uuid = res.to_string_naked();
            let rfq_path = self.common.paths.marketdata_rfq(cpty).append(uuid.as_str());
            let dval = self.common.subscriber.subscribe(rfq_path);
            dval.updates(UpdatesFlags::BEGIN_WITH_LAST, self.tx.clone());
            self.index.insert((venue, route, rfq), dval.id());
            self.subs.insert(dval.id(), dval);
        }
        Ok(())
    }

    // CR alee: add a one-shot RFQ request method
}
