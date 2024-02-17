use anyhow::Context;
use std::str::FromStr;

// Make sure that the code works fine with multi-threading enabled
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let db_url = "basic-crdb";
    let db = crdb::sqlx::postgres::PgPoolOptions::new()
        .max_connections(50)
        .connect(&db_url)
        .await
        .with_context(|| format!("opening database {db_url:?}"))?;
    let _crdb_server = crdb::Server::new(
        basic_api::db::ServerConfig,
        db,
        8 * 1024 * 1024,
        // Vacuum every 2 minutes, recreating objects older than 5 minutes
        crdb::ServerVacuumSchedule::new(
            crdb::cron::Schedule::from_str("0 2 * * * * *").unwrap(),
            crdb::chrono::Utc,
        )
        .recreate_older_than(std::time::Duration::from_secs(5 * 60)),
    )
    .await
    .unwrap();
    Ok(())
}
