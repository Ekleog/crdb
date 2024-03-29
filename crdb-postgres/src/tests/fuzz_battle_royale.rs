use super::{TmpDb, CHECK_NAMED_LOCKS_FOR, MAYBE_LOCK_TIMEOUT};
use crate::{
    db_trait::Db,
    error::ResultExt,
    server::postgres_db::PostgresDb,
    test_utils::{db::ServerConfig, *},
    BinPtr, EventId, ObjectId, Query, Timestamp, User,
};
use std::{ops::Bound, sync::Arc};
use tokio::sync::Mutex;
use ulid::Ulid;

#[derive(Debug, arbitrary::Arbitrary)]
enum Op {
    CreateSimple {
        id: ObjectId,
        created_at: EventId,
        object: Arc<TestObjectSimple>,
    },
    SubmitSimple {
        object: usize,
        event_id: EventId,
        event: Arc<TestEventSimple>,
    },
    GetSimple {
        object: usize,
        at: EventId,
    },
    QuerySimple {
        _user: User,
        _q: Query,
    },
    RecreateSimple {
        object: usize,
        time: Timestamp,
    },
    CreatePerms {
        id: ObjectId,
        created_at: EventId,
        object: Arc<TestObjectPerms>,
    },
    SubmitPerms {
        object: usize,
        event_id: EventId,
        event: Arc<TestEventPerms>,
    },
    GetPerms {
        object: usize,
        at: EventId,
    },
    QueryPerms {
        _user: User,
        _q: Query,
    },
    RecreatePerms {
        object: usize,
        time: Timestamp,
    },
    CreateDelegatePerms {
        id: ObjectId,
        created_at: EventId,
        object: Arc<TestObjectDelegatePerms>,
    },
    SubmitDelegatePerms {
        object: usize,
        event_id: EventId,
        event: Arc<TestEventDelegatePerms>,
    },
    GetDelegatePerms {
        object: usize,
        at: EventId,
    },
    QueryDelegatePerms {
        _user: User,
        _q: Query,
    },
    RecreateDelegatePerms {
        object: usize,
        time: Timestamp,
    },
    CreateFull {
        id: ObjectId,
        created_at: EventId,
        object: Arc<TestObjectFull>,
    },
    SubmitFull {
        object: usize,
        event_id: EventId,
        event: Arc<TestEventFull>,
    },
    GetFull {
        object: usize,
        at: EventId,
    },
    QueryFull {
        _user: User,
        _q: Query,
    },
    RecreateFull {
        object: usize,
        time: Timestamp,
    },
    Remove {
        object: usize,
    },
    CreateBinary {
        data: Arc<[u8]>,
        fake_id: Option<BinPtr>,
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
        Op::CreateSimple {
            id,
            created_at,
            object,
        } => {
            s.objects.lock().await.push(*id);
            let _pg = db.create(*id, *created_at, object.clone(), true, db).await;
        }
        Op::SubmitSimple {
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
        Op::GetSimple { object, at } => {
            // TODO(test-low): test when there's something actually tested
        }
        Op::QuerySimple { .. } => {
            // TODO(test-low): when there's something actually tested
            // run_query::<TestObjectSimple>(&db, &s.mem, *user, None, q).await?;
        }
        Op::RecreateSimple { object, time } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg = db.recreate::<TestObjectSimple, _>(o, *time, db).await;
        }
        Op::CreatePerms {
            id,
            created_at,
            object,
        } => {
            s.objects.lock().await.push(*id);
            let _pg = db.create(*id, *created_at, object.clone(), true, db).await;
        }
        Op::SubmitPerms {
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
                .submit::<TestObjectPerms, _>(o, *event_id, event.clone(), db)
                .await;
        }
        Op::GetPerms { object, at } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg: crate::Result<Arc<TestObjectPerms>> =
                match db.get::<TestObjectPerms>(true, o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                    Ok(o) => match o.get_snapshot_at::<TestObjectPerms>(Bound::Included(*at)) {
                        Ok(o) => Ok(o.1),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
        }
        Op::QueryPerms { .. } => {
            // TODO(test-low): when there's something actually tested
            // run_query::<TestObjectSimple>(&db, &s.mem, *user, None, q).await?;
        }
        Op::RecreatePerms { object, time } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg = db.recreate::<TestObjectPerms, _>(o, *time, db).await;
        }
        Op::CreateDelegatePerms {
            id,
            created_at,
            object,
        } => {
            s.objects.lock().await.push(*id);
            let _pg = db.create(*id, *created_at, object.clone(), true, db).await;
        }
        Op::SubmitDelegatePerms {
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
                .submit::<TestObjectDelegatePerms, _>(o, *event_id, event.clone(), db)
                .await;
        }
        Op::GetDelegatePerms { object, at } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg: crate::Result<Arc<TestObjectDelegatePerms>> = match db
                .get::<TestObjectDelegatePerms>(true, o)
                .await
            {
                Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                Ok(o) => match o.get_snapshot_at::<TestObjectDelegatePerms>(Bound::Included(*at)) {
                    Ok(o) => Ok(o.1),
                    Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                },
            };
        }
        Op::QueryDelegatePerms { .. } => {
            // TODO(test-low): when there's something actually tested
            // run_query::<TestObjectSimple>(&db, &s.mem, *user, None, q).await?;
        }
        Op::RecreateDelegatePerms { object, time } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg = db
                .recreate::<TestObjectDelegatePerms, _>(o, *time, db)
                .await;
        }
        Op::CreateFull {
            id,
            created_at,
            object,
        } => {
            s.objects.lock().await.push(*id);
            let _pg = db.create(*id, *created_at, object.clone(), true, db).await;
        }
        Op::SubmitFull {
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
                .submit::<TestObjectFull, _>(o, *event_id, event.clone(), db)
                .await;
        }
        Op::GetFull { object, at } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg: crate::Result<Arc<TestObjectFull>> =
                match db.get::<TestObjectFull>(true, o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                    Ok(o) => match o.get_snapshot_at::<TestObjectFull>(Bound::Included(*at)) {
                        Ok(o) => Ok(o.1),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
        }
        Op::QueryFull { .. } => {
            // TODO(test-low): when there's something actually tested
            // run_query::<TestObjectSimple>(&db, &s.mem, *user, None, q).await?;
        }
        Op::RecreateFull { object, time } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let _pg = db.recreate::<TestObjectFull, _>(o, *time, db).await;
        }
        Op::Remove { object } => {
            let _object = object; // TODO(test-low): implement for non-postgres databases
        }
        Op::CreateBinary { data, fake_id } => {
            let id = fake_id.unwrap_or_else(|| crate::hash_binary(&data));
            let _pg = db.create_binary(id, data.clone()).await;
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

fn fuzz_impl(
    cluster: &TmpDb,
    ops: &(Arc<Vec<Op>>, Arc<Vec<Op>>, Arc<Vec<Op>>),
    config: reord::Config,
) {
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
            let c = {
                let db = db.clone();
                let s = s.clone();
                let ops = ops.2.clone();
                tokio::task::spawn(reord::new_task(apply_ops(2, db, s, ops)))
            };
            let h = reord::start(3).await;
            tokio::try_join!(a, b, c, h).unwrap();
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
        .with_arbitrary()
        .for_each(move |(seed, ops)| {
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
            let mut config = reord::Config::from_seed(*seed);
            config.check_named_locks_work_for = Some(CHECK_NAMED_LOCKS_FOR);
            config.maybe_lock_timeout = MAYBE_LOCK_TIMEOUT;
            fuzz_impl(&cluster, ops, config)
        })
}
