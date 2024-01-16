use super::TmpDb;
use crate::{
    db_trait::Db,
    error::ResultExt,
    server::postgres_db::PostgresDb,
    test_utils::{db::ServerConfig, *},
    EventId, ObjectId, Query, Timestamp, User,
};
use std::{ops::Bound, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use ulid::Ulid;

#[derive(Debug, bolero::generator::TypeGenerator)]
enum Op {
    Create {
        id: ObjectId,
        created_at: EventId,
        object: Arc<TestObjectSimple>,
    },
    Submit {
        object: usize,
        event_id: EventId,
        event: Arc<TestEventSimple>,
    },
    Get {
        object: usize,
        at: EventId,
    },
    Query {
        user: User,
        #[generator(bolero::gen_arbitrary())]
        q: Query,
    },
    Recreate {
        object: usize,
        time: Timestamp,
    },
    Remove {
        object: usize,
    },
    Vacuum {
        recreate_at: Option<Timestamp>,
    },
}

struct FuzzState {
    objects: Mutex<Vec<ObjectId>>,
}

impl FuzzState {
    fn new() -> FuzzState {
        FuzzState {
            objects: Mutex::new(Vec::new()),
        }
    }
}

async fn apply_op(db: &PostgresDb<ServerConfig>, s: &FuzzState, op: &Op) -> anyhow::Result<()> {
    match op {
        Op::Create {
            id,
            created_at,
            object,
        } => {
            s.objects.lock().await.push(*id);
            let _pg = db.create(*id, *created_at, object.clone(), db).await;
        }
        Op::Submit {
            object,
            event_id,
            event,
        } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg = db
                .submit::<TestObjectSimple, _>(o, *event_id, event.clone(), db)
                .await;
        }
        Op::Get { object, at } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg: crate::Result<Arc<TestObjectSimple>> =
                match db.get::<TestObjectSimple>(o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                    Ok(o) => match o.get_snapshot_at::<TestObjectSimple>(Bound::Included(*at)) {
                        Ok(o) => Ok(o.1),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
        }
        Op::Query { user, q } => {
            let _pg = db
                .query::<TestObjectSimple>(*user, None, &q)
                .await
                .wrap_context("querying postgres");
        }
        Op::Recreate { object, time } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg = db.recreate::<TestObjectSimple, _>(*time, o, db).await;
        }
        Op::Remove { object } => {
            let _object = object; // TODO: implement for non-postgres databases
        }
        Op::Vacuum { recreate_at: None } => {
            db.vacuum(None, None, db, |r| {
                panic!("got unexpected recreation {r:?}")
            })
            .await
            .unwrap();
        }
        Op::Vacuum {
            recreate_at: Some(recreate_at),
        } => {
            let _pg = db.vacuum(Some(*recreate_at), None, db, |_| ()).await;
        }
    }
    Ok(())
}

async fn apply_ops(
    thread: usize,
    db: Arc<PostgresDb<ServerConfig>>,
    s: Arc<FuzzState>,
    ops: Arc<Vec<Op>>,
) {
    for (i, op) in ops.iter().enumerate() {
        apply_op(&db, &s, op)
            .await
            .wrap_with_context(|| format!("applying {i}th op of thread {thread}: {op:?}"))
            .unwrap();
    }
}

fn fuzz_impl(cluster: &TmpDb, ops: &(Arc<Vec<Op>>, Arc<Vec<Op>>), config: reord::Config) {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let pool = cluster.pool().await;
            let db = Arc::new(PostgresDb::connect(pool.clone()).await.unwrap());
            sqlx::query(include_str!("../cleanup-db.sql"))
                .execute(&pool)
                .await
                .unwrap();
            let s = Arc::new(FuzzState::new());

            // Start the test
            reord::init_test(config).await;
            let a = {
                let db = db.clone();
                let s = s.clone();
                let ops = ops.0.clone();
                tokio::task::spawn(reord::new_task(apply_ops(0, db, s, ops)))
            };
            let b = {
                let db = db.clone();
                let s = s.clone();
                let ops = ops.1.clone();
                tokio::task::spawn(reord::new_task(apply_ops(1, db, s, ops)))
            };
            let h = reord::start(2).await;
            let (a, b, h) = tokio::join!(a, b, h);
            a.unwrap();
            b.unwrap();
            h.unwrap();
            db.assert_invariants_generic().await;
            db.assert_invariants_for::<TestObjectSimple>().await;
        });
}

#[test]
fn fuzz_no_lock_check() {
    let cluster = TmpDb::new();
    bolero::check!()
        .with_iterations(20)
        .with_shrink_time(std::time::Duration::from_millis(0))
        .with_type()
        .for_each(move |(seed, ops)| fuzz_impl(&cluster, ops, reord::Config::from_seed(*seed)))
}

#[test]
fn fuzz_checking_locks() {
    let cluster = TmpDb::new();
    bolero::check!()
        .with_iterations(20)
        .with_type()
        .for_each(move |(seed, ops)| {
            let mut config = reord::Config::from_seed(*seed);
            // DO NOT check addressed locks, postgres locks with weird semantics are stashed there
            config.check_named_locks_work_for = Some(Duration::from_millis(200));
            fuzz_impl(&cluster, ops, config)
        })
}

// TODO: These fuzzers do not check anything, just non-crash/deadlock. We should have fuzzers that:
// - make sure that one thread does not submit to an object without creating it before
// - make sure that if two threads create an object it's with the same initial value
// - make sure that if two threads submit an event it's with the same value
// - somehow synchronize for recreations? to avoid event submissions being rejected dependent on interleaving
// - assert only once all operations are complete, that MemDb and PostgresDb have the same last_snapshot
// Also, same for the battle-royale fuzzer.
