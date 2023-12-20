#[tokio::main]
async fn main() {
    crdb::server::Server::new(api::db::ServerConfig, "test").await.unwrap();
}
