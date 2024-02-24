use super::IncomingMessage;
use futures::{SinkExt, StreamExt};
use gloo_net::websocket::Message;
use std::sync::Arc;

pub use gloo_net::websocket::futures::WebSocket;

pub async fn connect(url: &str) -> anyhow::Result<WebSocket> {
    Ok(WebSocket::open(url)?)
}

pub async fn send_text(sock: &mut WebSocket, msg: String) -> anyhow::Result<()> {
    Ok(sock.send(Message::Text(msg)).await?)
}

pub async fn next(sock: &mut WebSocket) -> anyhow::Result<IncomingMessage<String>> {
    let Some(msg) = sock.next().await else {
        anyhow::bail!("Got websocket end-of-stream, expected a message");
    };
    match msg? {
        Message::Text(s) => Ok(IncomingMessage::Text(s)),
        Message::Bytes(b) => Ok(IncomingMessage::Binary(b.into_boxed_slice().into())),
    }
}

pub async fn send_sidecar(sock: &mut WebSocket, sidecar: &[Arc<[u8]>]) -> anyhow::Result<()> {
    for bin in sidecar {
        // TODO(perf-med): have gloo-websocket not require a full copy of the binary
        sock.send(Message::Bytes((&**bin).to_vec())).await?;
    }
    Ok(())
}
