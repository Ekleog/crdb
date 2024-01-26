use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(_url: &str) -> anyhow::Result<WebSocket> {
    unimplemented!() // TODO(sqlite)
}

pub async fn send_text(_sock: &mut WebSocket, _msg: String) -> anyhow::Result<()> {
    unimplemented!() // TODO(sqlite)
}

pub async fn next_text(_sock: &mut WebSocket) -> anyhow::Result<String> {
    unimplemented!() // TODO(sqlite)
}

pub async fn send_sidecar(_sock: &mut WebSocket, _sidecar: &Vec<Arc<[u8]>>) -> anyhow::Result<()> {
    unimplemented!() // TODO(sqlite)
}
