use super::PostgresDb;
use crate::{test_utils::db::ServerConfig, Object};
use std::time::Duration;

// mod fuzz_battle_royale;
mod fuzz_sessions;
mod fuzz_two_threads;

const CHECK_NAMED_LOCKS_FOR: Duration = Duration::from_millis(500);
const MAYBE_LOCK_TIMEOUT: Duration = Duration::from_millis(500);

#[sqlx::test]
async fn smoke_test(db: sqlx::PgPool) {
    let (db, _keepalive) = PostgresDb::<ServerConfig>::connect(db, 0)
        .await
        .expect("connecting to db");
    crate::smoke_test!(
        db: db,
        vacuum: db.vacuum(Some(EVENT_ID_3), Updatedness::from_u128(128), Some(OBJECT_ID_3.time()), |_, _| ()),
        query_all: db
            .query(USER_ID_NULL, *TestObjectSimple::type_ulid(), None, Arc::new(Query::All(vec![])))
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
    use std::{collections::HashSet, sync::Arc};

    use crate::{
        cache::CacheDb,
        server::{postgres_db::tests::TmpDb, PostgresDb},
        test_utils::{db::ServerConfig, *},
        EventId, Object, Query, ResultExt, Updatedness, User,
    };

    pub use crate as crdb;
    pub use tokio::test;

    pub type Database = Arc<PostgresDb<ServerConfig>>;
    pub type KeepAlive = Arc<CacheDb<PostgresDb<ServerConfig>>>;
    pub type SetupState = TmpDb;

    pub fn setup() -> (TmpDb, bool) {
        (TmpDb::new(), true)
    }

    pub async fn make_db(cluster: &TmpDb) -> (Database, KeepAlive) {
        let pool = cluster.pool().await;
        let db = PostgresDb::connect(pool.clone(), 0).await.unwrap();
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
        only_updated_since: Option<Updatedness>,
        query: &Arc<Query>,
    ) -> anyhow::Result<()> {
        let pg = db
            .query(user, *T::type_ulid(), only_updated_since, query.clone())
            .await
            .wrap_context("querying postgres")
            .map(|r| r.into_iter().collect::<HashSet<_>>());
        let mem = mem_db
            .query::<T>(user, only_updated_since, &query)
            .await
            .map(|r| r.into_iter().collect::<HashSet<_>>());
        cmp(pg, mem)
    }

    pub async fn run_vacuum(
        db: &Database,
        mem_db: &MemDb,
        recreate_at: Option<(EventId, Updatedness)>,
    ) -> anyhow::Result<()> {
        match recreate_at {
            None => {
                let db = db
                    .vacuum(None, Updatedness::now(), None, |r, _| {
                        panic!("got unexpected recreation {r:?}");
                    })
                    .await;
                let mem = mem_db.vacuum().await;
                cmp(db, mem)
            }
            Some((recreate_at, updatedness)) => {
                let db = db
                    .vacuum(Some(recreate_at), updatedness, None, |_, _| {
                        // TODO(test-high): validate that the notified recreations are the same as in memdb
                    })
                    .await;
                let mem = async move {
                    mem_db
                        .recreate_all::<TestObjectSimple>(recreate_at, Some(updatedness))
                        .await?;
                    mem_db
                        .recreate_all::<TestObjectPerms>(recreate_at, Some(updatedness))
                        .await?;
                    mem_db
                        .recreate_all::<TestObjectDelegatePerms>(recreate_at, Some(updatedness))
                        .await?;
                    mem_db
                        .recreate_all::<TestObjectFull>(recreate_at, Some(updatedness))
                        .await?;
                    mem_db.vacuum().await?;
                    Ok(())
                }
                .await;
                cmp(db, mem)
            }
        }
    }
}

// TODO(test-high): add tests that validate that upgrading the version of objects or normalizer works fine

mod fuzz_simple {
    include!("../../test_utils/fuzz_simple.rs");
}

mod fuzz_remote_perms {
    include!("../../test_utils/fuzz_remote_perms.rs");
}

mod fuzz_object_full {
    include!("../../test_utils/fuzz_object_full.rs");
}
