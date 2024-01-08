use super::{cmp_db, TmpDb};
use crate::{
    db_trait::Db,
    error::ResultExt,
    server::postgres_db::PostgresDb,
    test_utils::{
        self, db::ServerConfig, TestEventDelegatePerms, TestEventFull, TestEventPerms,
        TestEventSimple, TestObjectDelegatePerms, TestObjectFull, TestObjectPerms,
        TestObjectSimple,
    },
    BinPtr, EventId, ObjectId, Timestamp,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use ulid::Ulid;

#[derive(Debug, bolero::generator::TypeGenerator)]
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
    },
    RecreateFull {
        object: usize,
        time: Timestamp,
    },
    CreateBinary {
        data: Arc<Vec<u8>>,
        fake_id: Option<BinPtr>,
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
        Op::CreateSimple {
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
            let pg = db
                .submit::<TestObjectSimple, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObjectSimple, _>(o, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
        }
        Op::GetSimple { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg: crate::Result<Arc<TestObjectSimple>> = match db.get::<TestObjectSimple>(o).await
            {
                Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                Ok(o) => match o.last_snapshot::<TestObjectSimple>() {
                    Ok(o) => Ok(o),
                    Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                },
            };
            let mem: crate::Result<Arc<TestObjectSimple>> =
                match s.mem_db.get::<TestObjectSimple>(o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in mem db")),
                    Ok(o) => match o.last_snapshot::<TestObjectSimple>() {
                        Ok(o) => Ok(o),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            cmp_db(pg, mem)?;
        }
        Op::RecreateSimple { object, time } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg = db.recreate::<TestObjectSimple, _>(*time, o, db).await;
            let mem = s
                .mem_db
                .recreate::<TestObjectSimple, _>(*time, o, &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
        }
        Op::CreatePerms {
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
            let pg = db
                .submit::<TestObjectPerms, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObjectPerms, _>(o, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
        }
        Op::GetPerms { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg: crate::Result<Arc<TestObjectPerms>> = match db.get::<TestObjectPerms>(o).await {
                Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                Ok(o) => match o.last_snapshot::<TestObjectPerms>() {
                    Ok(o) => Ok(o),
                    Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                },
            };
            let mem: crate::Result<Arc<TestObjectPerms>> =
                match s.mem_db.get::<TestObjectPerms>(o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in mem db")),
                    Ok(o) => match o.last_snapshot::<TestObjectPerms>() {
                        Ok(o) => Ok(o),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            cmp_db(pg, mem)?;
        }
        Op::RecreatePerms { object, time } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg = db.recreate::<TestObjectPerms, _>(*time, o, db).await;
            let mem = s
                .mem_db
                .recreate::<TestObjectPerms, _>(*time, o, &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
        }
        Op::CreateDelegatePerms {
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
            let pg = db
                .submit::<TestObjectDelegatePerms, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObjectDelegatePerms, _>(o, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
        }
        Op::GetDelegatePerms { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg: crate::Result<Arc<TestObjectDelegatePerms>> =
                match db.get::<TestObjectDelegatePerms>(o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                    Ok(o) => match o.last_snapshot::<TestObjectDelegatePerms>() {
                        Ok(o) => Ok(o),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            let mem: crate::Result<Arc<TestObjectDelegatePerms>> =
                match s.mem_db.get::<TestObjectDelegatePerms>(o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in mem db")),
                    Ok(o) => match o.last_snapshot::<TestObjectDelegatePerms>() {
                        Ok(o) => Ok(o),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            cmp_db(pg, mem)?;
        }
        Op::RecreateDelegatePerms { object, time } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg = db
                .recreate::<TestObjectDelegatePerms, _>(*time, o, db)
                .await;
            let mem = s
                .mem_db
                .recreate::<TestObjectDelegatePerms, _>(*time, o, &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
        }
        Op::CreateFull {
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
            let pg = db
                .submit::<TestObjectFull, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObjectFull, _>(o, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
        }
        Op::GetFull { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg: crate::Result<Arc<TestObjectFull>> = match db.get::<TestObjectFull>(o).await {
                Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                Ok(o) => match o.last_snapshot::<TestObjectFull>() {
                    Ok(o) => Ok(o),
                    Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                },
            };
            let mem: crate::Result<Arc<TestObjectFull>> =
                match s.mem_db.get::<TestObjectFull>(o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in mem db")),
                    Ok(o) => match o.last_snapshot::<TestObjectFull>() {
                        Ok(o) => Ok(o),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            cmp_db(pg, mem)?;
        }
        Op::RecreateFull { object, time } => {
            let o = s
                .objects
                .lock()
                .await
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg = db.recreate::<TestObjectFull, _>(*time, o, db).await;
            let mem = s
                .mem_db
                .recreate::<TestObjectFull, _>(*time, o, &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
        }
        Op::CreateBinary { data, fake_id } => {
            let id = fake_id.unwrap_or_else(|| crate::hash_binary(&data));
            let mem = s.mem_db.create_binary(id, data.clone()).await;
            let pg = db.create_binary(id, data.clone()).await;
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
            let mem = s
                .mem_db
                .recreate_all::<TestObjectSimple>(*recreate_at)
                .await;
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
            config.check_addressed_locks_work_for = Some(Duration::from_millis(500));
            config.check_named_locks_work_for = Some(Duration::from_millis(100));
            fuzz_impl(&cluster, ops, config)
        })
}
