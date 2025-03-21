use super::{TmpDb, CHECK_NAMED_LOCKS_FOR, MAYBE_LOCK_TIMEOUT};
use crate::PostgresDb;
use crdb_cache::CacheDb;
use crdb_core::{Db, EventId, Importance, ObjectId, ResultExt, ServerSideDb, Updatedness};
use crdb_test_utils::{Config, *};
use std::sync::Arc;
use tokio::sync::Mutex;
use ulid::Ulid;

#[derive(Debug, arbitrary::Arbitrary, serde::Deserialize, serde::Serialize)]
enum Op {
    Create {
        object_id: ObjectId,
        created_at: EventId,
        updatedness: Updatedness,
        object: Arc<TestObjectFull>,
    },
    Submit {
        object: usize,
        event_id: EventId,
        updatedness: Updatedness,
        event: Arc<TestEventFull>,
    },
    GetLatest {
        object: usize,
    },
    // TODO(test-low): also test query
    Recreate {
        object: usize,
        updatedness: Updatedness,
        event_id: EventId,
    },
    Remove {
        object: usize,
    },
    Vacuum {
        recreate_at: Option<EventId>,
        updatedness: Updatedness,
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

async fn apply_op(db: &CacheDb<PostgresDb<Config>>, s: &FuzzState, op: &Op) -> anyhow::Result<()> {
    match op {
        Op::Create {
            object_id,
            created_at,
            updatedness,
            object,
        } => {
            let mut object = object.clone();
            Arc::make_mut(&mut object).standardize(*object_id);
            s.objects.lock().await.push(*object_id);
            let _pg = db
                .create(
                    *object_id,
                    *created_at,
                    object.clone(),
                    Some(*updatedness),
                    Importance::LOCK,
                )
                .await;
        }
        Op::Submit {
            object,
            event_id,
            updatedness,
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
                .submit::<TestObjectFull>(
                    o,
                    *event_id,
                    event.clone(),
                    Some(*updatedness),
                    Importance::LOCK,
                )
                .await;
        }
        Op::GetLatest { object } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg: crate::Result<Arc<TestObjectFull>> = db
                .get_latest::<TestObjectFull>(o, Importance::LOCK)
                .await
                .wrap_context(&format!("getting {o:?} in database"));
        }
        Op::Recreate {
            object,
            updatedness,
            event_id,
        } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg = db
                .recreate_at::<TestObjectFull, _>(o, *event_id, *updatedness, db)
                .await;
        }
        Op::Remove { object } => {
            let _object = object; // TODO(test-low): implement for non-postgres databases
        }
        Op::Vacuum {
            recreate_at: None,
            updatedness,
        } => {
            db.server_vacuum(None, *updatedness, None, |_, _| {
                panic!("got unexpected recreation")
            })
            .await
            .unwrap();
        }
        Op::Vacuum {
            recreate_at: Some(recreate_at),
            updatedness,
        } => {
            let _pg = db
                .server_vacuum(Some(*recreate_at), *updatedness, None, |_, _| ())
                .await;
        }
    }
    Ok(())
}

async fn apply_ops(
    thread: usize,
    db: Arc<CacheDb<PostgresDb<Config>>>,
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
            let db = PostgresDb::connect(pool.clone(), 1024 * 1024)
                .await
                .unwrap();
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
            db.assert_invariants_for::<TestObjectFull>().await;
        });
}

#[test]
fn fuzz_no_lock_check() {
    let cluster = TmpDb::new();
    bolero::check!()
        .with_iterations(20)
        .with_shrink_time(std::time::Duration::from_millis(0))
        .with_arbitrary()
        .for_each(move |(seed, ops)| {
            #[cfg(not(fuzzing))]
            eprintln!(
                "Fuzzing with:\n---\n{}\n---",
                serde_json::to_string(&(seed, ops)).unwrap()
            );
            let mut config = reord::Config::from_seed(*seed);
            config.maybe_lock_timeout = MAYBE_LOCK_TIMEOUT;
            fuzz_impl(&cluster, ops, config)
        })
}

#[test]
fn fuzz_checking_locks() {
    let cluster = TmpDb::new();
    bolero::check!()
        .with_iterations(20)
        .with_arbitrary()
        .for_each(move |(seed, ops)| {
            #[cfg(not(fuzzing))]
            eprintln!(
                "Fuzzing with:\n---\n{}\n---",
                serde_json::to_string(&(seed, ops)).unwrap()
            );
            let mut config = reord::Config::from_seed(*seed);
            config.check_named_locks_work_for = Some(CHECK_NAMED_LOCKS_FOR);
            config.maybe_lock_timeout = MAYBE_LOCK_TIMEOUT;
            fuzz_impl(&cluster, ops, config)
        })
}

// TODO(test-med): These fuzzers do not check anything, just non-crash/deadlock. We should have fuzzers that:
// - make sure that one thread does not submit to an object without creating it before
// - make sure that if two threads create an object it's with the same initial value
// - make sure that if two threads submit an event it's with the same value
// - somehow synchronize for recreations? to avoid event submissions being rejected dependent on interleaving
// - assert only once all operations are complete, that MemDb and PostgresDb have the same last_snapshot
// Also, same for the battle-royale fuzzer.

#[test]
#[cfg(any())] // Disabled
fn impl_reproducer() {
    tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
        .init();
    let cluster = TmpDb::new();
    let (seed, ops) = serde_json::from_str(include_str!("../../../../repro.json")).unwrap();
    let mut config = reord::Config::from_seed(seed);
    config.maybe_lock_timeout = MAYBE_LOCK_TIMEOUT;
    fuzz_impl(&cluster, &ops, config);
}
