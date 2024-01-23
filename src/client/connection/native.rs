use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
