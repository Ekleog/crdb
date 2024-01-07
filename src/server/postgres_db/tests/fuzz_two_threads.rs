use super::{cmp_db, TmpDb};
use crate::{
    db_trait::Db,
    error::ResultExt,
    server::postgres_db::PostgresDb,
    test_utils::{self, db::ServerConfig, TestEvent1, TestObject1},
    EventId, ObjectId, Timestamp,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use ulid::Ulid;

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
    Recreate {
        object: usize,
        time: Timestamp,
    },
    Vacuum {
        recreate_at: Option<Timestamp>,
    },
}

struct FuzzState {
    objects: Mutex<Vec<ObjectId>>,
    mem_db: test_utils::MemDb,
}

impl FuzzState {
    fn new() -> FuzzState {
        FuzzState {
            objects: Mutex::new(Vec::new()),
            mem_db: test_utils::MemDb::new(),
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
            let pg = db.create(*id, *created_at, object.clone(), db).await;
            let mem = s
                .mem_db
                .create(*id, *created_at, object.clone(), &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
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
            let pg = db
                .submit::<TestObject1, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObject1, _>(o, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
        }
        Op::Get { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg: crate::Result<Arc<TestObject1>> = match db.get::<TestObject1>(o).await {
                Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                Ok(o) => match o.last_snapshot::<TestObject1>() {
                    Ok(o) => Ok(o),
                    Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                },
            };
            let mem: crate::Result<Arc<TestObject1>> = match s.mem_db.get::<TestObject1>(o).await {
                Err(e) => Err(e).wrap_context(&format!("getting {o:?} in mem db")),
                Ok(o) => match o.last_snapshot::<TestObject1>() {
                    Ok(o) => Ok(o),
                    Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                },
            };
            cmp_db(pg, mem)?;
        }
        Op::Recreate { object, time } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg = db.recreate::<TestObject1, _>(*time, o, db).await;
            let mem = s
                .mem_db
                .recreate::<TestObject1, _>(*time, o, &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
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
            let mem = s.mem_db.recreate_all::<TestObject1>(*recreate_at).await;
            let pg = db.vacuum(Some(*recreate_at), None, db, |_| ()).await;
            cmp_db(pg, mem)?;
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
            db.assert_invariants_for::<TestObject1>().await;
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
            config.check_addressed_locks_work_for = Some(Duration::from_millis(100));
            config.check_named_locks_work_for = Some(Duration::from_millis(100));
            fuzz_impl(&cluster, ops, config)
        })
}
