use super::PostgresDb;
use crate::test_utils::db::ServerConfig;
use std::time::Duration;

mod fuzz_battle_royale;
mod fuzz_object_full;
mod fuzz_remote_perms;
mod fuzz_sessions;
mod fuzz_two_threads;

const CHECK_NAMED_LOCKS_FOR: Duration = Duration::from_millis(500);
const MAYBE_LOCK_TIMEOUT: Duration = Duration::from_millis(500);

#[sqlx::test]
async fn smoke_test(db: sqlx::PgPool) {
    let db = PostgresDb::<ServerConfig>::connect(db)
        .await
        .expect("connecting to db");
    crate::smoke_test!(
        db: db,
        vacuum: db.vacuum(Some(EVENT_ID_3.time()), Some(OBJECT_ID_3.time()), &db, |_| ()),
        test_remove: false,
    );
}

struct TmpDb {
    url: String,
    dir: tempfile::TempDir,
}

impl TmpDb {
    fn new() -> TmpDb {
        let dir = tempfile::Builder::new()
            .prefix("crdb-test-pg-")
            .tempdir()
            .expect("Failed creating a temporary directory");
        let p = dir.path();
        let db = p;
        let logs = p.join("logs");
        std::process::Command::new("pg_ctl")
            .env("PGDATA", &db)
            .env("PGHOST", &db)
            .args(["init", "-s", "-o", "-E utf8 --locale C -A trust"])
            .env("TZ", "UTC")
            .status()
            .expect("Failed creating the database");
        std::process::Command::new("pg_ctl")
            .env("PGDATA", &db)
            .env("PGHOST", &db)
            .args([
                "start",
                "-s",
                "-l",
                logs.to_str().unwrap(),
                "-w",
                "-o",
                &format!("-F -h '' -k {db:?}"),
            ])
            .status()
            .expect("Failed starting the postgres server");
        let url = format!("postgres://?host={}&dbname=postgres", db.to_str().unwrap());
        TmpDb { url, dir }
    }

    async fn pool(&self) -> sqlx::PgPool {
        sqlx::PgPool::connect(&self.url)
            .await
            .expect("Failed connecting to running cluster")
    }
}

impl Drop for TmpDb {
    fn drop(&mut self) {
        std::process::Command::new("pg_ctl")
            .env("PGDATA", &self.dir.path().join("db"))
            .env("PGHOST", &self.dir.path().join("db"))
            .args(["stop", "-s", "-w", "-m", "fast"])
            .output()
            .expect("Failed stopping the postgres server");
    }
}

mod fuzz_helpers {
    use super::TmpDb;
    use crate::server::PostgresDb;
    use crate::test_utils::db::ServerConfig;

    pub use tokio::test;

    pub type Database = PostgresDb<ServerConfig>;
    pub type SetupState = TmpDb;

    pub fn setup() -> TmpDb {
        TmpDb::new()
    }

    pub async fn make_db(cluster: &TmpDb) -> Database {
        let pool = cluster.pool().await;
        let db = PostgresDb::connect(pool.clone()).await.unwrap();
        sqlx::query(include_str!("./cleanup-db.sql"))
            .execute(&pool)
            .await
            .unwrap();
        db
    }
}

mod fuzz_simple {
    include!("../../test_utils/fuzz_simple.rs");
}
