//! Core channel driver--wraps the underlying netidx pack_channel with
//! useful specialized functions.

use crate::Common;
use anyhow::{anyhow, Result};
use api::{
    utils::messaging::MaybeRequest, Address, ComponentId, Envelope, MaybeSplit, Stamp,
    TypedMessage,
};
use netidx_protocols::pack_channel;
use std::sync::Arc;
use tokio::{sync::broadcast, task};

pub struct ChannelDriver {
    channel: Arc<pack_channel::client::Connection>,
    src: Address,
    tx: broadcast::Sender<Arc<Vec<Envelope<TypedMessage>>>>,
    _rx: broadcast::Receiver<Arc<Vec<Envelope<TypedMessage>>>>,
}

impl ChannelDriver {
    pub async fn connect(common: &Common) -> Result<Self> {
        let channel = Arc::new(
            pack_channel::client::Connection::connect(
                &common.subscriber,
                common.paths.channel(),
            )
            .await?,
        );
        let src = channel.recv_one().await?;
        let (tx, rx) = broadcast::channel(1000);
        {
            let channel = channel.clone();
            let tx = tx.clone();
            task::spawn(async move {
                let mut messages: Vec<Envelope<TypedMessage>> = vec![];
                while let Ok(()) = channel
                    .recv(|m| {
                        messages.push(m);
                        true
                    })
                    .await
                {
                    let buf = std::mem::replace(&mut messages, Vec::new());
                    tx.send(Arc::new(buf)).unwrap();
                }
            });
        }
        Ok(Self { channel, src, tx, _rx: rx })
    }

    pub fn src(&self) -> Address {
        self.src
    }

    /// Access to the underlying netidx pack_channel connection
    pub fn channel(&self) -> &pack_channel::client::Connection {
        &self.channel
    }

    // CR alee: probably want to give these type signatures some more thought;
    // one disadvantage to using Into<TypedMessage> as a bound is how to support
    // custom builds without having to make a new api/sdk;
    pub fn send_to<M>(&self, dst: ComponentId, msg: M) -> Result<()>
    where
        M: Into<TypedMessage>,
    {
        self.channel.send_one(&Envelope {
            src: self.src,
            dst: Address::Component(dst),
            stamp: Stamp::Unstamped,
            msg: msg.into(),
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Vec<Envelope<TypedMessage>>>> {
        self.tx.subscribe()
    }

    /// Wait for a message that satisfies predicate `f`.
    /// The dumber version of [wait_for].
    pub async fn wait_until<R>(&self, mut f: impl FnMut(R) -> bool) -> Result<()>
    where
        TypedMessage: TryInto<MaybeSplit<TypedMessage, R>>,
    {
        self.wait_for(|msg| if f(msg) { Some(()) } else { None }).await
    }

    /// Wait for a response that satisfies `f`.
    /// Ignores and discards any intervening messages.
    pub async fn wait_for<R, T>(&self, mut f: impl FnMut(R) -> Option<T>) -> Result<T>
    where
        TypedMessage: TryInto<MaybeSplit<TypedMessage, R>>,
    {
        let mut rx = self.tx.subscribe();
        while let Ok(envs) = rx.recv().await {
            for env in envs.iter() {
                if let Ok((_orig, msg)) =
                    env.msg.clone().try_into().map(MaybeSplit::parts)
                {
                    if let Some(t) = f(msg) {
                        return Ok(t);
                    }
                }
            }
        }
        Err(anyhow!("lost connection to component channel"))
    }

    /// Send message to a component and wait for a response that satisfies `f`.
    /// Ignores and discards any intervening messages.
    pub async fn send_to_and_wait_for<M, R, T>(
        &self,
        dst: ComponentId,
        msg: M,
        f: impl FnMut(R) -> Option<T>,
    ) -> Result<T>
    where
        M: Into<TypedMessage>,
        TypedMessage: TryInto<MaybeSplit<TypedMessage, R>>,
    {
        let waiter = self.wait_for(f);
        self.send_to(dst, msg)?;
        waiter.await
    }

    /// Send a request to a component and wait for the corresponding response.  Calls the
    /// provided `unwrap` function on the response and returns the result.
    ///
    /// Ignores and discards any intervening messages.
    pub async fn request_and_wait_for<M, R, T>(
        &self,
        dst: ComponentId,
        msg: M,
        unwrap: impl Fn(R) -> Result<T>,
    ) -> Result<T>
    where
        M: MaybeRequest + Into<TypedMessage>,
        R: MaybeRequest,
        TypedMessage: TryInto<MaybeSplit<TypedMessage, R>>,
    {
        let req_id = msg.request_id();
        self.send_to_and_wait_for(dst, msg, |res| {
            if res.response_id() == req_id {
                Some(unwrap(res))
            } else {
                None
            }
        })
        .await?
    }
}
