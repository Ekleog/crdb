use anyhow::Context;
use std::{str::FromStr, time::Duration};

const CACHE_SIZE: usize = 32 * 1024 * 1024;
const RECREATE_OLDER_THAN: Duration = Duration::from_secs(5 * 60);
const KILL_SESSIONS_OLDER_THAN: Duration = Duration::from_secs(24 * 3600);

// Make sure that the code works fine with multi-threading enabled
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let db_url = "postgres:///basic-crdb";
    let db = crdb::sqlx::postgres::PgPoolOptions::new()
        .max_connections(50)
        .connect(&db_url)
        .await
        .with_context(|| format!("opening database {db_url:?}"))?;
    let _crdb_server = crdb::Server::new(
        basic_api::db::ServerConfig,
        db,
        CACHE_SIZE,
        // Vacuum every 2 minutes, recreating objects older than 5 minutes
        crdb::ServerVacuumSchedule::new(
            crdb::cron::Schedule::from_str("0 */2 * * * * *").unwrap(),
            crdb::chrono::Utc,
        )
        .recreate_older_than(RECREATE_OLDER_THAN)
        .kill_sessions_older_than(KILL_SESSIONS_OLDER_THAN),
    )
    .await
    .context("creating crdb server")?;
    Ok(())
}
