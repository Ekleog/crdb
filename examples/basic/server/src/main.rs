#[tokio::main]
async fn main() {
    crdb::Server::new(api::db::ServerConfig, "test", 8 * 1024 * 1024)
        .await
        .unwrap();
}
