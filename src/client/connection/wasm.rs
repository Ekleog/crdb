use futures::{SinkExt, StreamExt};
pub use gloo_net::websocket::{futures::WebSocket, Message};

pub async fn connect(url: &str) -> Result<WebSocket, String> {
    WebSocket::open(url).map_err(|err| format!("{err}"))
}

pub async fn send(sock: &mut WebSocket, msg: Vec<u8>) -> Result<(), String> {
    sock.send(Message::Bytes(msg))
        .await
        .map_err(|err| format!("{err}"))
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
