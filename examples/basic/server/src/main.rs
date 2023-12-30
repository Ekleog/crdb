use anyhow::Context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db_url = "test";
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(50)
        .connect(&db_url)
        .await
        .with_context(|| format!("opening database {db_url:?}"))?;
    crdb::Server::new(api::db::ServerConfig, db, 8 * 1024 * 1024)
        .await
        .unwrap();
    Ok(())
}
