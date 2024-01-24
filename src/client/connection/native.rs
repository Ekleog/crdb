use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect(_url: &str) -> Result<WebSocket, String> {
    unimplemented!() // TODO(sqlite)
}
