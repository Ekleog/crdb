pub use gloo_net::websocket::futures::WebSocket;

pub async fn connect(url: &str) -> Result<WebSocket, String> {
    WebSocket::open(url).map_err(|err| format!("{err}"))
}
