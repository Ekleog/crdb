#[tokio::main]
async fn main() {
    crdb::Server::new(api::db::ServerConfig, "test")
        .await
        .unwrap();
}
