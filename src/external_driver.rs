use anyhow::{bail, Result};
use api::external::*;
use futures_util::{SinkExt, StreamExt};
use log::{error, warn};
use serde::{de::DeserializeOwned, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use url::Url;

#[derive(Debug)]
pub struct ExternalDriver {
    ids: u64,
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl ExternalDriver {
    pub async fn connect(url: Url) -> Result<Self> {
        let (ws, _) = connect_async(url).await?;
        Ok(Self { ids: 0, ws })
    }

    pub async fn send_query<T: Serialize>(
        &mut self,
        method: &str,
        params: Option<T>,
    ) -> Result<()> {
        let id = self.ids;
        self.ids += 1;
        self.ws
            .send(Message::text(&serde_json::to_string(&ProtocolQueryMessage {
                method: method.to_string(),
                id,
                params,
            })?))
            .await?;
        Ok(())
    }

    pub async fn next_response<T: DeserializeOwned>(&mut self) -> Result<T> {
        while let Some(r) = self.ws.next().await {
            match r? {
                msg @ (Message::Text(_) | Message::Binary(_)) => {
                    if let Ok(text) = msg.into_text() {
                        let hdr: ProtocolMessageHeader = serde_json::from_str(&text)?;
                        if hdr.r#type == "response" {
                            let t = serde_json::from_str::<ProtocolResponseMessage<T>>(
                                &text,
                            )?;
                            if let Some(e) = t.error {
                                bail!("error from server {}: {}", e.code, e.message);
                            } else if let Some(r) = t.result {
                                return Ok(r);
                            } else {
                                bail!("no result or error from server: {text}");
                            }
                        } else {
                            warn!("skipping unexpected message from server: {text}");
                        }
                    } else {
                        error!("unexpected message from server, invalid utf8");
                    }
                }
                Message::Frame(_) => {}
                Message::Ping(data) => {
                    self.ws.send(Message::Pong(data)).await?;
                }
                Message::Pong(_) => {}
                Message::Close(_) => break,
            }
        }
        bail!("connection closed");
    }
}
