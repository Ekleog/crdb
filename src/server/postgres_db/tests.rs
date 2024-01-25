use super::PostgresDb;
use crate::test_utils::db::ServerConfig;

// TODO(test): re-enable these tests after making them actually test things
// mod fuzz_battle_royale;
mod fuzz_sessions;
// mod fuzz_two_threads;

// const CHECK_NAMED_LOCKS_FOR: Duration = Duration::from_millis(1000);
// const MAYBE_LOCK_TIMEOUT: Duration = Duration::from_millis(1000);

#[sqlx::test]
async fn smoke_test(db: sqlx::PgPool) {
    let db = PostgresDb::<ServerConfig>::connect(db)
        .await
        .expect("connecting to db");
    crate::smoke_test!(
        db: db,
        vacuum: db.vacuum(Some(EVENT_ID_3.time()), Some(OBJECT_ID_3.time()), &db, |_| ()),
        query_all: db
            .query::<TestObjectSimple>(USER_ID_NULL, None, &Query::All(vec![]))
            .await
            .unwrap(),
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
    use crate::{
        server::{postgres_db::tests::TmpDb, PostgresDb},
        test_utils::{db::ServerConfig, *},
        Object, Query, ResultExt, Timestamp, User,
    };

    pub use crate as crdb;
    pub use tokio::test;

    pub type Database = PostgresDb<ServerConfig>;
    pub type SetupState = TmpDb;

    pub fn setup() -> (TmpDb, bool) {
        (TmpDb::new(), true)
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

    macro_rules! make_fuzzer {
        ($fuzzer_name:expr, $function_name:ident, $fuzz_impl:ident) => {
            #[test]
            fn $function_name() {
                let cluster = setup();
                bolero::check!()
                    .with_iterations(10)
                    .with_arbitrary()
                    .for_each(move |ops| {
                        tokio::runtime::Runtime::new()
                            .unwrap()
                            .block_on($fuzz_impl(&cluster, Arc::clone(ops)));
                    })
            }
        };
    }

    pub(crate) use make_fuzzer;

    pub async fn run_query<T: Object>(
        db: &Database,
        mem_db: &MemDb,
        user: User,
        only_updated_since: Option<Timestamp>,
        query: &Query,
    ) -> anyhow::Result<()> {
        let pg = db
            .query::<T>(user, only_updated_since, query)
            .await
            .wrap_context("querying postgres");
        let mem = mem_db
            .query::<T>(user, only_updated_since, query)
            .await
            .wrap_context("querying mem");
        cmp(pg, mem)
    }

    pub async fn run_vacuum(
        db: &Database,
        mem_db: &MemDb,
        recreate_at: Option<Timestamp>,
    ) -> anyhow::Result<()> {
        match recreate_at {
            None => {
                db.vacuum(None, None, db, |r| {
                    panic!("got unexpected recreation {r:?}")
                })
                .await
                .unwrap();
            }
            Some(recreate_at) => {
                let mem = (|| async {
                    mem_db.recreate_all::<TestObjectSimple>(recreate_at).await?;
                    mem_db.recreate_all::<TestObjectPerms>(recreate_at).await?;
                    mem_db
                        .recreate_all::<TestObjectDelegatePerms>(recreate_at)
                        .await?;
                    Ok(())
                })()
                .await;
                let pg = db.vacuum(Some(recreate_at), None, db, |_| ()).await;
                cmp(pg, mem)?;
            }
        }
        Ok(())
    }
}

// TODO(test): add tests that validate that upgrading the version of objects or normalizer works fine

mod fuzz_simple {
    include!("../../test_utils/fuzz_simple.rs");
}

mod fuzz_remote_perms {
    include!("../../test_utils/fuzz_remote_perms.rs");
}

mod fuzz_object_full {
    include!("../../test_utils/fuzz_object_full.rs");
}
