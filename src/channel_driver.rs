//! Core channel driver--wraps the underlying netidx pack_channel with
//! useful specialized functions.

use crate::Common;
use anyhow::{anyhow, bail, Result};
use api::{
    utils::messaging::MaybeRequest, Address, ComponentId, Envelope, MaybeSplit, Stamp,
    TypedMessage,
};
use futures_util::{select_biased, FutureExt};
use log::{debug, error};
use netidx_protocols::pack_channel;
use std::sync::Arc;
use tokio::{
    sync::{broadcast, oneshot},
    task,
};

static DEFAULT_CHANNEL_ID: u32 = 1;

pub struct ChannelDriver {
    channel: Arc<pack_channel::client::Connection>,
    src: Address,
    tx: broadcast::Sender<Arc<Vec<Envelope<TypedMessage>>>>,
    _rx: broadcast::Receiver<Arc<Vec<Envelope<TypedMessage>>>>,
    close: Option<(oneshot::Sender<()>, task::JoinHandle<()>)>,
}

impl ChannelDriver {
    pub async fn connect(common: &Common, channel_id: Option<u32>) -> Result<Self> {
        let channel_id = channel_id.unwrap_or(DEFAULT_CHANNEL_ID);
        let channel = Arc::new(
            pack_channel::client::Connection::connect(
                &common.subscriber,
                common.paths.channel(),
            )
            .await?,
        );
        debug!("beginning channel handshake, channel_id = {}", channel_id);
        channel.send_one(&channel_id)?;
        let src: Address = channel.recv_one().await?;
        debug!("channel handshake complete, channel = {}", src);
        let (close_tx, close_rx) = oneshot::channel();
        let (tx, rx) = broadcast::channel(1000);
        let join = {
            let channel = channel.clone();
            let tx = tx.clone();
            task::spawn(async move {
                let mut messages: Vec<Envelope<TypedMessage>> = vec![];
                let mut close_rx = close_rx.fuse();
                loop {
                    let mut closed = false;
                    select_biased! {
                        _ = &mut close_rx => { closed = true; },
                        _ = channel.recv(|m| { messages.push(m); true }).fuse() => {}
                    }
                    let buf = std::mem::replace(&mut messages, Vec::new());
                    if let Err(e) = tx.send(Arc::new(buf)) {
                        error!("channel driver send error, dropping: {}", e);
                    }
                    if closed {
                        break;
                    }
                }
            })
        };
        Ok(Self { channel, src, tx, _rx: rx, close: Some((close_tx, join)) })
    }

    /// Close the channel, waiting for all queued messages to send
    pub async fn close(&mut self) -> Result<()> {
        if let Some((close_tx, join)) = self.close.take() {
            close_tx.send(()).map_err(|_| anyhow!("channel already closed"))?;
            join.await?;
            Ok(())
        } else {
            bail!("channel already closed")
        }
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
            stamp: Stamp::default(),
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
