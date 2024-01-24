use futures::SinkExt;
pub use gloo_net::websocket::{futures::WebSocket, Message};

pub async fn connect(url: &str) -> Result<WebSocket, String> {
    WebSocket::open(url).map_err(|err| format!("{err}"))
}

pub async fn send(sock: &mut WebSocket, msg: Vec<u8>) -> Result<(), String> {
    sock.send(Message::Bytes(msg))
        .await
        .map_err(|err| format!("{err}"))
}
