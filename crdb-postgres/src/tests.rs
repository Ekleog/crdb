use super::PostgresDb;
use std::time::Duration;
use web_time::SystemTime;

// mod fuzz_battle_royale;
mod fuzz_sessions;
mod fuzz_two_threads;

const CHECK_NAMED_LOCKS_FOR: Duration = Duration::from_millis(500);
const MAYBE_LOCK_TIMEOUT: Duration = Duration::from_millis(500);

#[sqlx::test]
async fn smoke_test(db: sqlx::PgPool) {
    let db = PostgresDb::<crdb_test_utils::Config>::connect(db, 0)
        .await
        .expect("connecting to db");
    crdb_test_utils::smoke_test!(
        db: db,
        query_all: db
            .server_query(USER_ID_NULL, *TestObjectSimple::type_ulid(), None, Arc::new(Query::All(vec![])))
            .await
            .unwrap(),
        db_type: server,
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
    use std::{collections::HashSet, ops::Deref, sync::Arc};

    use crate::{tests::TmpDb, PostgresDb};
    use crdb_cache::CacheDb;
    use crdb_core::{Object, Query, ResultExt, ServerSideDb, Updatedness, User};
    use crdb_test_utils::{Config, *};

    pub use tokio::test;

    pub struct Database(Arc<CacheDb<PostgresDb<Config>>>);
    pub type SetupState = TmpDb;

    // TODO(api-high): remove this type and especially this Deref once CacheDb is properly always updated whatever the operation
    // that happens (here in particular, Vacuum)
    impl Deref for Database {
        type Target = PostgresDb<Config>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    pub fn setup() -> (TmpDb, bool) {
        (TmpDb::new(), true)
    }

    pub async fn make_db(cluster: &TmpDb) -> (Database, ()) {
        let pool = cluster.pool().await;
        let db = PostgresDb::connect(pool.clone(), 0).await.unwrap();
        sqlx::query(include_str!("./cleanup-db.sql"))
            .execute(&pool)
            .await
            .unwrap();
        (Database(db), ())
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
        only_updated_since: Option<Updatedness>,
        query: &Arc<Query>,
    ) -> anyhow::Result<()> {
        let pg = db
            .server_query(user, *T::type_ulid(), only_updated_since, query.clone())
            .await
            .wrap_context("querying postgres")
            .map(|r| r.into_iter().collect::<HashSet<_>>());
        let mem = mem_db
            .memdb_query::<T>(user, only_updated_since, &query)
            .await
            .map(|r| r.into_iter().collect::<HashSet<_>>());
        cmp(pg, mem)
    }
}

// TODO(test-high): add tests that validate that upgrading the version of objects or normalizer works fine

mod fuzz_simple {
    crdb_test_utils::fuzz_simple!(server);
}

mod fuzz_remote_perms {
    crdb_test_utils::fuzz_remote_perms!(server);
}

mod fuzz_object_full {
    crdb_test_utils::fuzz_object_full!(server);
}
