use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(_url: &str) -> Result<WebSocket, String> {
    unimplemented!() // TODO(sqlite)
}

pub async fn send(_sock: &mut WebSocket, _msg: Vec<u8>) -> Result<(), String> {
    unimplemented!() // TODO(sqlite)
}

pub async fn next_text(_sock: &mut WebSocket) -> anyhow::Result<String> {
    unimplemented!() // TODO(sqlite)
}
