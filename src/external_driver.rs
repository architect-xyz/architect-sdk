use anyhow::{anyhow, bail, Result};
use api::external::*;
use async_stream::try_stream;
use futures::Stream;
use futures_util::{select_biased, FutureExt, SinkExt, StreamExt};
use fxhash::FxHashMap;
use log::{error, warn};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use url::Url;

#[derive(Debug)]
struct Subscription {
    topic: String,
    updates: mpsc::UnboundedSender<String>,
}

#[derive(Debug)]
pub struct ExternalDriver {
    ids: AtomicU64,
    ws_task: JoinHandle<()>,
    ws_write: mpsc::Sender<Message>,
    requests: Arc<Mutex<FxHashMap<u64, oneshot::Sender<String>>>>,
    subscriptions: Arc<Mutex<FxHashMap<u64, Subscription>>>,
}

impl Drop for ExternalDriver {
    fn drop(&mut self) {
        self.ws_task.abort();
    }
}

impl ExternalDriver {
    pub fn new(url: Url) -> Self {
        let (ws_write, mut to_write) = mpsc::channel(1000);
        let requests = Arc::new(Mutex::new(FxHashMap::default()));
        let subscriptions = Arc::new(Mutex::new(FxHashMap::default()));
        let ws_task = {
            let requests = requests.clone();
            let subscriptions = subscriptions.clone();
            tokio::spawn(async move {
                loop {
                    if let Err(e) =
                        Self::run(url.clone(), &mut to_write, &requests, &subscriptions)
                            .await
                    {
                        error!("error in external driver ws connection: {e:?}");
                        warn!("external driver ws reconnecting in 3s...");
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                    }
                }
            })
        };
        Self { ids: AtomicU64::new(0), ws_task, ws_write, requests, subscriptions }
    }

    async fn run(
        url: Url,
        to_write: &mut mpsc::Receiver<Message>,
        requests: &Mutex<FxHashMap<u64, oneshot::Sender<String>>>,
        subscriptions: &Mutex<FxHashMap<u64, Subscription>>,
    ) -> Result<()> {
        let (mut ws, _) = connect_async(url).await?;
        loop {
            select_biased! {
                m = to_write.recv().fuse() => {
                    if let Some(m) = m {
                        ws.send(m).await?;
                    } else {
                        break;
                    }
                }
                r = ws.next() => {
                    if let Some(r) = r {
                        let msg = r?;
                        Self::process_message(&mut ws, requests, subscriptions, msg).await?;
                    } else {
                        break;
                    }
                }
            }
        }
        bail!("run terminated unexpectedly");
    }

    async fn process_message(
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        requests: &Mutex<FxHashMap<u64, oneshot::Sender<String>>>,
        subscriptions: &Mutex<FxHashMap<u64, Subscription>>,
        msg: Message,
    ) -> Result<()> {
        match msg {
            msg @ (Message::Text(_) | Message::Binary(_)) => {
                if let Ok(text) = msg.into_text() {
                    let hdr: ProtocolMessageHeader = serde_json::from_str(&text)?;
                    if hdr.r#type == "response" {
                        let reply = {
                            let mut fries = requests.lock().expect("lock poisoned");
                            fries.remove(&hdr.id)
                        };
                        if let Some(reply) = reply {
                            if let Err(e) = reply.send(text) {
                                warn!("while delivering response: {e}");
                            }
                        } else {
                            warn!("unwaited response from server: {text}");
                        }
                    } else if hdr.r#type == "update" {
                        let id = hdr.id;
                        let dropped_sub = {
                            let mut dropped = false;
                            let mut subs = subscriptions.lock().expect("lock poisoned");
                            if let Some(sub) = subs.get_mut(&id) {
                                if let Err(_) = sub.updates.send(text) {
                                    dropped = true;
                                }
                            } else {
                                // CR alee: this will print a few times on each unsubscription
                                warn!("skipping unexpected update from server: {text}");
                            }
                            if dropped {
                                subs.remove(&id)
                            } else {
                                None
                            }
                        };
                        if let Some(sub) = dropped_sub {
                            ws.send(Message::text(&serde_json::to_string(
                                &ProtocolUnsubscribeMessage {
                                    id,
                                    topic: sub.topic,
                                    sub_id: Some(id),
                                },
                            )?))
                            .await?;
                        }
                    } else {
                        warn!("skipping unexpected message from server: {text}");
                    }
                } else {
                    warn!("unexpected message from server, invalid utf8");
                }
            }
            Message::Frame(_) => {}
            Message::Ping(data) => {
                ws.send(Message::Pong(data)).await?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => {
                bail!("connection closed");
            }
        }
        Ok(())
    }

    pub async fn query<T: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: Option<T>,
    ) -> Result<R> {
        let id = self.ids.fetch_add(1, Ordering::Relaxed);
        let message = Message::text(&serde_json::to_string(&ProtocolQueryMessage {
            method: method.to_string(),
            id,
            params,
        })?);
        let rx = {
            let (tx, rx) = oneshot::channel();
            let mut fries = self.requests.lock().expect("lock poisoned");
            fries.insert(id, tx);
            rx
        };
        self.ws_write.send(message).await?;
        let res = rx.await?;
        let r: ProtocolResponseMessage<R> = serde_json::from_str(&res)?;
        if let Some(e) = r.error {
            bail!("error from server {}: {}", e.code, e.message);
        } else if let Some(r) = r.result {
            Ok(r)
        } else {
            bail!("no result or error from server: {res}");
        }
    }

    pub async fn subscribe<U: DeserializeOwned + 'static>(
        &self,
        topic: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<U>>>>> {
        let id = self.ids.fetch_add(1, Ordering::Relaxed);
        let message = Message::text(&serde_json::to_string(&ProtocolSubscribeMessage {
            id,
            topic: topic.to_string(),
        })?);
        self.ws_write.send(message).await?;
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        {
            let mut subs = self.subscriptions.lock().expect("lock poisoned");
            subs.insert(id, Subscription { topic: topic.to_string(), updates: tx });
        }
        let stream = try_stream! {
            loop {
                let text = rx.recv().await.ok_or_else(|| anyhow!("subscription channel closed"))?;
                let u: ProtocolUpdateMessage<U> = serde_json::from_str(&text)?;
                yield u.data;
            }
        };
        Ok(Box::pin(stream))
    }
}
