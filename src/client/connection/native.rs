use super::IncomingMessage;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(_url: &str) -> anyhow::Result<WebSocket> {
    unimplemented!() // TODO(sqlite-high)
}

pub async fn send_text(_sock: &mut WebSocket, _msg: String) -> anyhow::Result<()> {
    unimplemented!() // TODO(sqlite-high)
}

pub async fn next(_sock: &mut WebSocket) -> anyhow::Result<IncomingMessage<String>> {
    let _ = IncomingMessage::Text(());
    let _ = IncomingMessage::<()>::Binary(Arc::new([]));
    unimplemented!() // TODO(sqlite-high)
}

pub async fn send_sidecar(_sock: &mut WebSocket, _sidecar: &Vec<Arc<[u8]>>) -> anyhow::Result<()> {
    unimplemented!() // TODO(sqlite-high)
}
