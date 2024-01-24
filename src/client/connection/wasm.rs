use futures::{SinkExt, StreamExt};
pub use gloo_net::websocket::{futures::WebSocket, Message};

pub async fn connect(url: &str) -> anyhow::Result<WebSocket> {
    Ok(WebSocket::open(url)?)
}

pub async fn send_text(sock: &mut WebSocket, msg: String) -> anyhow::Result<()> {
    Ok(sock.send(Message::Text(msg)).await?)
}

pub async fn next_text(sock: &mut WebSocket) -> anyhow::Result<String> {
    let Some(msg) = sock.next().await else {
        anyhow::bail!("Got websocket end-of-stream, expected a message");
    };
    match msg? {
        Message::Text(s) => Ok(s),
        Message::Bytes(_) => anyhow::bail!("Got binary websocket message, expected a text one"),
    }
}
