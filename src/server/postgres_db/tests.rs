use super::PostgresDb;
use crate::{
    cache::ObjectCache,
    db_trait::{Db, DbOpError, EventId, ObjectId, Timestamp},
    test_utils::{
        TestEvent1, TestObject1, EVENT_ID_1, EVENT_ID_2, EVENT_ID_3, EVENT_ID_4, OBJECT_ID_1,
    },
};
use anyhow::Context;
use std::{fmt::Debug, sync::Arc};
use ulid::Ulid;

#[sqlx::test]
async fn smoke_test(db: sqlx::PgPool) {
    let db = PostgresDb::connect(db).await.expect("connecting to db");
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObject1::stub_1()),
        &db,
    )
    .await
    .expect("creating test object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    db.create(
        OBJECT_ID_1,
        EVENT_ID_2,
        Arc::new(TestObject1::stub_2()),
        &db,
    )
    .await
    .expect_err("creating duplicate test object 1 spuriously worked");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObject1::stub_1()),
        &db,
    )
    .await
    .expect("creating exact copy test object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    db.submit::<TestObject1, _>(OBJECT_ID_1, EVENT_ID_3, Arc::new(TestEvent1::Clear), &db)
        .await
        .expect("clearing object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    db.submit::<TestObject1, _>(OBJECT_ID_1, EVENT_ID_3, Arc::new(TestEvent1::Clear), &db)
        .await
        .expect("submitting duplicate event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    db.submit::<TestObject1, _>(
        OBJECT_ID_1,
        EVENT_ID_3,
        Arc::new(TestEvent1::Set(b"foo".to_vec())),
        &db,
    )
    .await
    .expect_err("submitting duplicate event with different contents worked");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    assert_eq!(
        Vec::<u8>::new(),
        db.get::<TestObject1>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .expect("object 1 is supposed to exist")
            .last_snapshot::<TestObject1>()
            .expect("getting last snapshot")
            .0
    );
    db.submit::<TestObject1, _>(
        OBJECT_ID_1,
        EVENT_ID_2,
        Arc::new(TestEvent1::Set(b"bar".to_vec())),
        &db,
    )
    .await
    .expect("submitting event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    assert_eq!(
        Vec::<u8>::new(),
        db.get::<TestObject1>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .expect("object 1 is supposed to exist")
            .last_snapshot::<TestObject1>()
            .expect("getting last snapshot")
            .0
    );
    db.submit::<TestObject1, _>(
        OBJECT_ID_1,
        EVENT_ID_4,
        Arc::new(TestEvent1::Set(b"baz".to_vec())),
        &db,
    )
    .await
    .expect("submitting event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    assert_eq!(
        b"baz".to_vec(),
        db.get::<TestObject1>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .expect("object 1 is supposed to exist")
            .last_snapshot::<TestObject1>()
            .expect("getting last snapshot")
            .0
    );
}

#[derive(Debug, bolero::generator::TypeGenerator)]
enum Op {
    Create {
        id: ObjectId,
        created_at: EventId,
        object: Arc<TestObject1>,
    },
    Submit {
        object: usize,
        event_id: EventId,
        event: Arc<TestEvent1>,
    },
    Get {
        object: usize,
    },
    /* TODO: `user` should be a usize, and TestObject should have some auth info
    Query {
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Timestamp,
        q: Query,
    },
    */
    Recreate {
        object: usize,
        time: Timestamp,
    },
    /* TODO: TestObject should have some binary info
    CreateBinary {
        data: Vec<u8>,
    },
    */
}

struct FuzzState {
    objects: Vec<ObjectId>,
    mem_db: ObjectCache,
}

impl FuzzState {
    fn new() -> FuzzState {
        FuzzState {
            objects: Vec::new(),
            mem_db: ObjectCache::new(usize::MAX),
        }
    }
}

fn cmp_anyhow<T: Debug + Eq>(pg: anyhow::Result<T>, mem: anyhow::Result<T>) -> anyhow::Result<()> {
    anyhow::ensure!(
        (pg.is_err() && mem.is_err()) || (pg.as_ref().unwrap() == mem.as_ref().unwrap()),
        "postgres result != mem result:\n==========\nPostgres:\n{pg:?}\n==========\nMem:\n{mem:?}"
    );
    Ok(())
}

fn cmp_db<T: Debug + Eq>(
    pg_res: Result<T, DbOpError>,
    mem_res: anyhow::Result<T>,
) -> anyhow::Result<()> {
    match (&pg_res, &mem_res) {
        (Ok(pg), Ok(mem)) => anyhow::ensure!(
            pg == mem,
            "postgres result != mem result:\n==========\nPostgres:\n{pg_res:?}\n==========\nMem:\n{mem_res:?}"
        ),
        (Err(DbOpError::Other(_pg)), Err(_mem)) => (), // TODO: add more checks? (and in cmp_anyhow too)
        (Err(DbOpError::MissingBinPtrs(_)), _) => todo!(),
        (_, _) => anyhow::bail!("postgres result != mem result:\n==========\nPostgres:\n{pg_res:?}\n==========\nMem:\n{mem_res:?}"),
    }
    Ok(())
}

async fn apply_op(db: &PostgresDb, s: &mut FuzzState, op: &Op) -> anyhow::Result<()> {
    match op {
        Op::Create {
            id,
            created_at,
            object,
        } => {
            s.objects.push(*id);
            let pg = db.create(*id, *created_at, object.clone(), db).await;
            let mem = s
                .mem_db
                .create(*id, *created_at, object.clone())
                .map(|_| ());
            cmp_db(pg, mem)?;
        }
        Op::Submit {
            object,
            event_id,
            event,
        } => {
            let o = s
                .objects
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg = db
                .submit::<TestObject1, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObject1>(o, *event_id, event.clone())
                .map(|_| ());
            cmp_db(pg, mem)?;
        }
        Op::Get { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg: anyhow::Result<Option<Arc<TestObject1>>> = match db.get::<TestObject1>(o).await
            {
                Err(e) => Err(e).context("getting {o:?} in database"),
                Ok(None) => Ok(None),
                Ok(Some(o)) => match o.last_snapshot::<TestObject1>() {
                    Ok(o) => Ok(Some(o)),
                    Err(e) => Err(e).context("getting last snapshot of {o:?}"),
                },
            };
            let mem: anyhow::Result<Option<Arc<TestObject1>>> = s
                .mem_db
                .get(&o)
                .map(|o| o.last_snapshot::<TestObject1>())
                .transpose();
            cmp_anyhow(pg, mem)?;
        }
        Op::Recreate { object, time } => {
            let o = s
                .objects
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg = db.recreate::<TestObject1, _>(*time, o, db).await;
            let mem = s.mem_db.recreate::<TestObject1>(o, *time);
            cmp_anyhow(pg, mem)?;
        }
    }
    Ok(())
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

fn db_keeps_invariants_impl(cluster: &TmpDb, ops: &Vec<Op>) {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let pool = cluster.pool().await;
            let db = PostgresDb::connect(pool.clone()).await.unwrap();
            sqlx::query(include_str!("cleanup-db.sql"))
                .execute(&pool)
                .await
                .unwrap();
            let mut s = FuzzState::new();
            for (i, op) in ops.iter().enumerate() {
                apply_op(&db, &mut s, op)
                    .await
                    .with_context(|| format!("applying {i}th op: {op:?}"))
                    .unwrap();
                db.assert_invariants_generic().await;
                db.assert_invariants_for::<TestObject1>().await;
            }
        });
}

#[test]
fn db_keeps_invariants() {
    let cluster = TmpDb::new();
    bolero::check!()
        .with_iterations(50)
        .with_type()
        .for_each(move |ops| db_keeps_invariants_impl(&cluster, ops))
}

// TODO: add a fuzzer using `reord` that checks for concurrency
