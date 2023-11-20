use crate::Common;
use anyhow::{anyhow, Result};
use api::{utils::messaging::MaybeRequest, ComponentId, Envelope};
use futures::channel::mpsc as fmpsc;
use futures_util::StreamExt;
use log::warn;
use netidx::{
    pack::Pack,
    pool::Pooled,
    subscriber::{Dval, Event, FromValue, SubId, UpdatesFlags, Value},
};

pub struct ComponentDriver {
    id: ComponentId,
    channel: Dval,
    rx: fmpsc::Receiver<Pooled<Vec<(SubId, Event)>>>,
}

impl ComponentDriver {
    pub fn new(common: Common, id: ComponentId) -> Self {
        let (tx, rx) = fmpsc::channel(1000);
        let channel =
            common.subscriber.subscribe(common.paths.component(id).append("channel"));
        channel.updates(UpdatesFlags::empty(), tx);
        Self { id, channel, rx }
    }

    pub fn send<M>(&mut self, msg: M)
    where
        M: Pack + 'static,
    {
        self.channel.write(Envelope::to(self.id, msg).into());
    }

    // TODO: is this cancel safe?
    pub async fn recv<M>(&mut self) -> Result<Vec<M>>
    where
        M: FromValue + 'static,
    {
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
                Event::Update(v) => match M::from_value(v) {
                    Ok(m) => messages.push(m),
                    Err(e) => {
                        warn!("ignoring message: {e}");
                    }
                },
            }
        }
        Ok(messages)
    }

    /// Send message to a component and wait for a response that satisfies `f`.
    /// Ignores and discards any intervening messages.
    pub async fn send_and_wait_for<M, T>(
        &mut self,
        msg: M,
        mut f: impl FnMut(M) -> Option<T>,
    ) -> Result<T>
    where
        M: Pack + FromValue + 'static,
    {
        self.channel.write(Envelope::to(self.id, msg).into());
        'outer: while let Some(mut batch) = self.rx.next().await {
            for (_, msg) in batch.drain(..) {
                match msg {
                    Event::Unsubscribed => break 'outer,
                    Event::Update(Value::Null) => (),
                    Event::Update(v) => match M::from_value(v) {
                        Ok(m) => {
                            if let Some(t) = f(m) {
                                return Ok(t);
                            }
                        }
                        Err(e) => {
                            warn!("ignoring message: {e}");
                        }
                    },
                }
            }
        }
        Err(anyhow!("lost connection to component channel"))
    }

    /// Send a request to a component and wait for the corresponding response.  Calls the
    /// provided `unwrap` function on the response and returns the result.
    ///
    /// Ignores and discards any intervening messages.
    pub async fn request_and_wait_for<M, T>(
        &mut self,
        msg: M,
        unwrap: impl Fn(M) -> Result<T>,
    ) -> Result<T>
    where
        M: MaybeRequest + Pack + FromValue + 'static,
    {
        let req_id = msg.request_id();
        self.send_and_wait_for(msg, |msg| {
            if msg.response_id() == req_id {
                Some(unwrap(msg))
            } else {
                None
            }
        })
        .await?
    }
}
