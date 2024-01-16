use super::PostgresDb;
use crate::{
    db_trait::Db,
    test_utils::{
        db::ServerConfig, TestEventSimple, TestObjectSimple, EVENT_ID_1, EVENT_ID_2, EVENT_ID_3,
        EVENT_ID_4, OBJECT_ID_1, OBJECT_ID_3,
    },
};
use std::{sync::Arc, time::Duration};

mod fuzz_battle_royale;
mod fuzz_object_full;
mod fuzz_remote_perms;
mod fuzz_sessions;
mod fuzz_simple;
mod fuzz_two_threads;

const CHECK_NAMED_LOCKS_FOR: Duration = Duration::from_millis(500);
const MAYBE_LOCK_TIMEOUT: Duration = Duration::from_millis(500);

#[sqlx::test]
async fn smoke_test(db: sqlx::PgPool) {
    let db = PostgresDb::<ServerConfig>::connect(db)
        .await
        .expect("connecting to db");
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObjectSimple::stub_1()),
        &db,
    )
    .await
    .expect("creating test object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.create(
        OBJECT_ID_1,
        EVENT_ID_2,
        Arc::new(TestObjectSimple::stub_2()),
        &db,
    )
    .await
    .expect_err("creating duplicate test object 1 spuriously worked");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObjectSimple::stub_1()),
        &db,
    )
    .await
    .expect("creating exact copy test object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.submit::<TestObjectSimple, _>(
        OBJECT_ID_1,
        EVENT_ID_3,
        Arc::new(TestEventSimple::Clear),
        &db,
    )
    .await
    .expect("clearing object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.submit::<TestObjectSimple, _>(
        OBJECT_ID_1,
        EVENT_ID_3,
        Arc::new(TestEventSimple::Clear),
        &db,
    )
    .await
    .expect("submitting duplicate event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.submit::<TestObjectSimple, _>(
        OBJECT_ID_1,
        EVENT_ID_3,
        Arc::new(TestEventSimple::Set(b"foo".to_vec())),
        &db,
    )
    .await
    .expect_err("submitting duplicate event with different contents worked");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    assert_eq!(
        Vec::<u8>::new(),
        db.get::<TestObjectSimple>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .last_snapshot::<TestObjectSimple>()
            .expect("getting last snapshot")
            .0
    );
    db.submit::<TestObjectSimple, _>(
        OBJECT_ID_1,
        EVENT_ID_2,
        Arc::new(TestEventSimple::Set(b"bar".to_vec())),
        &db,
    )
    .await
    .expect("submitting event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    assert_eq!(
        Vec::<u8>::new(),
        db.get::<TestObjectSimple>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .last_snapshot::<TestObjectSimple>()
            .expect("getting last snapshot")
            .0
    );
    db.submit::<TestObjectSimple, _>(
        OBJECT_ID_1,
        EVENT_ID_4,
        Arc::new(TestEventSimple::Set(b"baz".to_vec())),
        &db,
    )
    .await
    .expect("submitting event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    assert_eq!(
        b"baz".to_vec(),
        db.get::<TestObjectSimple>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .last_snapshot::<TestObjectSimple>()
            .expect("getting last snapshot")
            .0
    );
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.vacuum(
        Some(EVENT_ID_3.time()),
        Some(OBJECT_ID_3.time()),
        &db,
        |_| (),
    )
    .await
    .unwrap();
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
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
