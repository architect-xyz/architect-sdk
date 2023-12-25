//! No-frills direct core driver.

use crate::Common;
use anyhow::{anyhow, Result};
use api::{Envelope, TypedMessage};
use futures::channel::mpsc as fmpsc;
use futures_util::StreamExt;
use log::warn;
use netidx::{
    pool::Pooled,
    subscriber::{Dval, Event, FromValue, SubId, UpdatesFlags, Value},
};

pub struct Driver {
    channel: Dval,
    rx: fmpsc::Receiver<Pooled<Vec<(SubId, Event)>>>,
}

impl Driver {
    pub async fn connect(common: &Common) -> Result<Self> {
        let (tx, rx) = fmpsc::channel(1000);
        let channel = common.subscriber.subscribe(common.paths.core().append("channel"));
        channel.updates(UpdatesFlags::empty(), tx);
        channel.wait_subscribed().await?;
        Ok(Self { channel, rx })
    }

    pub fn send(&self, msg: Envelope<TypedMessage>) {
        let _ = self.channel.write(msg.into());
    }

    // CR alee: is this cancel safe?
    pub async fn recv(&mut self) -> Result<Vec<Envelope<TypedMessage>>> {
        let mut messages = vec![];
        let mut batch = self
            .rx
            .next()
            .await
            .ok_or_else(|| anyhow!("lost connection to component channel"))?;
        for (_, msg) in batch.drain(..) {
            match msg {
                Event::Unsubscribed => {
                    return Err(anyhow!("lost connection to component channel"))
                }
                Event::Update(Value::Null) => (),
                Event::Update(v) => match Envelope::<TypedMessage>::from_value(v) {
                    Ok(m) => messages.push(m),
                    Err(e) => {
                        warn!("ignoring undecipherable message: {e}");
                    }
                },
            }
        }
        Ok(messages)
    }
}
