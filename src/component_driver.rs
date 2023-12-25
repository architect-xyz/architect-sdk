use crate::Common;
use anyhow::{anyhow, Result};
use api::{
    utils::messaging::MaybeRequest, ComponentId, Envelope, MaybeSplit, TypedMessage,
};
use futures::channel::mpsc as fmpsc;
use futures_util::StreamExt;
use log::warn;
use netidx::{
    pool::Pooled,
    subscriber::{Dval, Event, FromValue, SubId, UpdatesFlags, Value},
};

pub struct ComponentDriver {
    id: ComponentId,
    channel: Dval,
    rx: fmpsc::Receiver<Pooled<Vec<(SubId, Event)>>>,
}

impl ComponentDriver {
    pub async fn connect(common: &Common, id: ComponentId) -> Result<Self> {
        let (tx, rx) = fmpsc::channel(1000);
        let channel = common.subscriber.subscribe(common.paths.core().append("channel"));
        channel.updates(UpdatesFlags::empty(), tx);
        channel.wait_subscribed().await?;
        Ok(Self { id, channel, rx })
    }

    // CR alee: probably want to give these type signatures some more thought;
    // one disadvantage to using Into<TypedMessage> as a bound is how to support
    // custom builds without having to make a new api/sdk;
    //
    // another observation: this isn't symmetric with receive...we're sending
    // TypedMessage's but getting Envelope<TypedMessage>'s
    pub fn send<M>(&self, msg: M)
    where
        M: Into<TypedMessage>,
    {
        self.channel.write(Envelope::to(self.id, Into::<TypedMessage>::into(msg)).into());
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

    /// Send message to a component and wait for a response that satisfies `f`.
    /// Ignores and discards any intervening messages.
    pub async fn send_and_wait_for<M, R, T>(
        &mut self,
        msg: M,
        mut f: impl FnMut(R) -> Option<T>,
    ) -> Result<T>
    where
        M: Into<TypedMessage>,
        TypedMessage: TryInto<MaybeSplit<TypedMessage, R>>,
    {
        self.channel.write(Envelope::to(self.id, Into::<TypedMessage>::into(msg)).into());
        'outer: while let Some(mut batch) = self.rx.next().await {
            for (_, msg) in batch.drain(..) {
                match msg {
                    Event::Unsubscribed => break 'outer,
                    Event::Update(Value::Null) => (),
                    Event::Update(v) => match Envelope::<TypedMessage>::from_value(v) {
                        Ok(en) => {
                            if let Ok((_orig, msg)) =
                                en.msg.try_into().map(MaybeSplit::parts)
                            {
                                if let Some(t) = f(msg) {
                                    return Ok(t);
                                }
                            } else {
                                warn!("got message not downcastable");
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
    pub async fn request_and_wait_for<M, R, T>(
        &mut self,
        msg: M,
        unwrap: impl Fn(R) -> Result<T>,
    ) -> Result<T>
    where
        M: MaybeRequest + Into<TypedMessage>,
        R: MaybeRequest,
        TypedMessage: TryInto<MaybeSplit<TypedMessage, R>>,
    {
        let req_id = msg.request_id();
        self.send_and_wait_for(msg, |res| {
            if res.response_id() == req_id {
                Some(unwrap(res))
            } else {
                None
            }
        })
        .await?
    }
}
