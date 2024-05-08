//! Core channel driver--wraps the underlying netidx pack_channel with
//! useful specialized functions.

use anyhow::{anyhow, bail, Result};
use api::{
    system_control::SystemControlMessage, utils::messaging::MaybeRequest, Address,
    ComponentId, Envelope, MaybeSplit, MessageTopic, Stamp, TypedMessage, UserId,
};
use enumflags2::BitFlags;
use futures_util::{select_biased, FutureExt};
use log::{debug, error};
use netidx::{path::Path, subscriber::Subscriber};
use netidx_protocols::pack_channel;
use std::sync::{Arc, RwLock};
use tokio::{
    sync::{broadcast, oneshot, watch},
    task,
};

static DEFAULT_CHANNEL_ID: u32 = 1;

struct Channel {
    channel: Arc<pack_channel::client::Connection>,
    src: Address,
}

pub struct ChannelDriver {
    channel: Arc<RwLock<Option<Channel>>>,
    channel_ready: watch::Receiver<bool>,
    channel_path: Path,
    tx: broadcast::Sender<Arc<Vec<Envelope<TypedMessage>>>>,
    _rx: broadcast::Receiver<Arc<Vec<Envelope<TypedMessage>>>>,
    close: Option<(oneshot::Sender<()>, task::JoinHandle<()>)>,
}

impl ChannelDriver {
    pub fn new(
        subscriber: &Subscriber,
        channel_path: Path,
        channel_id: Option<u32>,
    ) -> Self {
        let channel = Arc::new(RwLock::new(None));
        let (mut channel_ready_tx, channel_ready_rx) = watch::channel(false);
        let (close_tx, mut close_rx) = oneshot::channel();
        let (tx, rx) = broadcast::channel(1000);
        let channel_task = {
            let subscriber = subscriber.clone();
            let channel_path = channel_path.clone();
            let channel = channel.clone();
            let tx = tx.clone();
            task::spawn({
                async move {
                    loop {
                        let res = Self::connect_inner(
                            &subscriber,
                            channel_path.clone(),
                            channel_id,
                            channel.clone(),
                            &mut channel_ready_tx,
                            &mut close_rx,
                            tx.clone(),
                        )
                        .await;
                        channel_ready_tx.send_replace(false);
                        if let Err(e) = res {
                            error!("channel driver error, reconnecting in 1s: {}", e);
                            let delay = std::time::Duration::from_secs(1);
                            tokio::time::sleep(delay).await;
                        } else {
                            // graceful shutdown
                            break;
                        }
                    }
                }
            })
        };
        Self {
            channel,
            channel_ready: channel_ready_rx,
            channel_path,
            tx,
            _rx: rx,
            close: Some((close_tx, channel_task)),
        }
    }

    async fn connect_inner(
        subscriber: &Subscriber,
        channel_path: Path,
        channel_id: Option<u32>,
        channel: Arc<RwLock<Option<Channel>>>,
        channel_ready_tx: &mut watch::Sender<bool>,
        close_rx: &mut oneshot::Receiver<()>,
        tx: broadcast::Sender<Arc<Vec<Envelope<TypedMessage>>>>,
    ) -> Result<()> {
        let channel_id = channel_id.unwrap_or(DEFAULT_CHANNEL_ID);
        let conn = Arc::new(
            pack_channel::client::Connection::connect(subscriber, channel_path.clone())
                .await?,
        );
        debug!("beginning channel handshake, channel_id = {}", channel_id);
        conn.send_one(&channel_id)?;
        let src: Address = conn.recv_one().await?;
        {
            if let Ok(mut channel) = channel.write() {
                *channel = Some(Channel { channel: conn.clone(), src: src.clone() });
            } else {
                bail!("BUG: channel ready lock poisoned");
            }
        }
        channel_ready_tx.send_replace(true);
        debug!("channel handshake complete, channel = {}", src);
        let mut messages: Vec<Envelope<TypedMessage>> = vec![];
        let mut close_rx = close_rx.fuse();
        loop {
            let mut closed = false;
            select_biased! {
                _ = &mut close_rx => { closed = true; },
                _ = conn.recv(|m| { messages.push(m); true }).fuse() => {}
            }
            let buf = std::mem::replace(&mut messages, Vec::new());
            if !buf.is_empty() {
                if let Err(e) = tx.send(Arc::new(buf)) {
                    error!("channel driver send error, dropping: {}", e);
                }
            }
            if closed {
                break Ok(());
            }
        }
    }

    pub async fn wait_connected(&self) -> Result<()> {
        let mut channel_ready = self.channel_ready.clone();
        let _ = channel_ready.wait_for(|ready| *ready).await?;
        Ok(())
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

    pub fn path(&self) -> &Path {
        &self.channel_path
    }

    pub fn user_id(&self) -> Result<UserId> {
        let channel =
            self.channel.read().map_err(|_| anyhow!("channel ready lock poisoned"))?;
        if let Some(channel) = &*channel {
            match channel.src {
                Address::Channel(user_id, _) => Ok(user_id),
                _ => bail!("channel not a user channel"),
            }
        } else {
            bail!("channel not ready")
        }
    }

    /// Access to the underlying netidx pack_channel connection
    pub fn with_channel<R>(
        &self,
        f: impl FnOnce(&pack_channel::client::Connection, Address) -> R,
    ) -> Result<R> {
        if let Ok(cr) = self.channel.read() {
            if let Some(cr) = &*cr {
                Ok(f(&cr.channel, cr.src.clone()))
            } else {
                bail!("channel not ready")
            }
        } else {
            bail!("channel ready lock poisoned")
        }
    }

    // CR alee: probably want to give these type signatures some more thought;
    // one disadvantage to using Into<TypedMessage> as a bound is how to support
    // custom builds without having to make a new api/sdk;
    pub fn send_to<M>(&self, dst: ComponentId, msg: M) -> Result<()>
    where
        M: Into<TypedMessage>,
    {
        self.with_channel(|conn, src| {
            conn.send_one(&Envelope {
                src,
                dst: Address::Component(dst),
                stamp: Stamp::new(Default::default()),
                msg: msg.into(),
            })
        })?
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Vec<Envelope<TypedMessage>>>> {
        self.tx.subscribe()
    }

    /// Subscribe this channel to the given message topics.
    /// Set topics to empty to unsubscribe.
    pub fn subscribe_channel_to_topics(
        &self,
        topics: BitFlags<MessageTopic>,
    ) -> Result<()> {
        self.with_channel(|conn, src| {
            if let Address::Channel(uid, chan) = src {
                conn.send_one(&Envelope::system_control(TypedMessage::SystemControl(
                    SystemControlMessage::ChannelSubscribe(uid, chan, topics),
                )))
            } else {
                bail!("channel not a user channel")
            }
        })?
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
