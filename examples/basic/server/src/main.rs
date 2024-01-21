use anyhow::Context;
use std::str::FromStr;

// Make sure that the code works fine with multi-threading enabled
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let db_url = "test";
    let db = crdb::sqlx::postgres::PgPoolOptions::new()
        .max_connections(50)
        .connect(&db_url)
        .await
        .with_context(|| format!("opening database {db_url:?}"))?;
    crdb::Server::new(
        api::db::ServerConfig,
        db,
        8 * 1024 * 1024,
        crdb::ServerVacuumSchedule::new(
            crdb::cron::Schedule::from_str("").unwrap(),
            crdb::chrono::Utc,
        ),
    )
    .await
    .unwrap();
    Ok(())
}
