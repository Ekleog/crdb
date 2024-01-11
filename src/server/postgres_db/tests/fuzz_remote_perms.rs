use super::TmpDb;
use crate::{
    db_trait::Db,
    error::ResultExt,
    server::postgres_db::PostgresDb,
    test_utils::{self, db::ServerConfig, *},
    DbPtr, EventId, ObjectId, Query, Timestamp, User,
};
use std::sync::Arc;
use ulid::Ulid;

#[derive(Debug, bolero::generator::TypeGenerator)]
enum Op {
    CreatePerm {
        id: ObjectId,
        created_at: EventId,
        object: Arc<TestObjectPerms>,
    },
    CreateDelegator {
        id: ObjectId,
        created_at: EventId,
        object: Arc<TestObjectDelegatePerms>,
    },
    SubmitPerm {
        object: usize,
        event_id: EventId,
        event: Arc<TestEventPerms>,
    },
    SubmitDelegator {
        object: usize,
        event_id: EventId,
        event: Arc<TestEventDelegatePerms>,
    },
    GetPerm {
        object: usize,
    },
    GetDelegator {
        object: usize,
    },
    QueryPerms {
        user: User,
        #[generator(bolero::gen_arbitrary())]
        q: Query,
    },
    QueryDelegatePerms {
        user: User,
        #[generator(bolero::gen_arbitrary())]
        q: Query,
    },
    RecreatePerm {
        object: usize,
        time: Timestamp,
    },
    RecreateDelegator {
        object: usize,
        time: Timestamp,
    },
    /* TODO: TestObject should have some binary info
    CreateBinary {
        data: Vec<u8>,
    },
    */
    Vacuum {
        recreate_at: Option<Timestamp>,
    },
}

struct FuzzState {
    objects: Vec<ObjectId>,
    mem_db: test_utils::MemDb,
}

impl FuzzState {
    fn new() -> FuzzState {
        FuzzState {
            objects: Vec::new(),
            mem_db: test_utils::MemDb::new(),
        }
    }
}

async fn apply_op(db: &PostgresDb<ServerConfig>, s: &mut FuzzState, op: &Op) -> anyhow::Result<()> {
    match op {
        Op::CreatePerm {
            id,
            created_at,
            object,
        } => {
            s.objects.push(*id);
            let pg = db.create(*id, *created_at, object.clone(), db).await;
            let mem = s
                .mem_db
                .create(*id, *created_at, object.clone(), &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::CreateDelegator {
            id,
            created_at,
            object,
        } => {
            s.objects.push(*id);
            let pg = db.create(*id, *created_at, object.clone(), db).await;
            let mem = s
                .mem_db
                .create(*id, *created_at, object.clone(), &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::SubmitPerm {
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
                .submit::<TestObjectPerms, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObjectPerms, _>(o, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::SubmitDelegator {
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
                .submit::<TestObjectDelegatePerms, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObjectDelegatePerms, _>(o, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::GetPerm { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg: crate::Result<Arc<TestObjectPerms>> = match db.get::<TestObjectPerms>(o).await {
                Err(e) => Err(e).wrap_context("getting {o:?} in database"),
                Ok(o) => match o.last_snapshot::<TestObjectPerms>() {
                    Ok(o) => Ok(o),
                    Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                },
            };
            let mem: crate::Result<Arc<TestObjectPerms>> =
                match s.mem_db.get::<TestObjectPerms>(o).await {
                    Err(e) => Err(e).wrap_context("getting {o:?} in mem db"),
                    Ok(o) => match o.last_snapshot::<TestObjectPerms>() {
                        Ok(o) => Ok(o),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            cmp(pg, mem)?;
        }
        Op::GetDelegator { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg: crate::Result<Arc<TestObjectDelegatePerms>> =
                match db.get::<TestObjectDelegatePerms>(o).await {
                    Err(e) => Err(e).wrap_context("getting {o:?} in database"),

                    Ok(o) => match o.last_snapshot::<TestObjectDelegatePerms>() {
                        Ok(o) => Ok(o),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            let mem: crate::Result<Arc<TestObjectDelegatePerms>> =
                match s.mem_db.get::<TestObjectDelegatePerms>(o).await {
                    Err(e) => Err(e).wrap_context("getting {o:?} in mem db"),

                    Ok(o) => match o.last_snapshot::<TestObjectDelegatePerms>() {
                        Ok(o) => Ok(o),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            cmp(pg, mem)?;
        }
        Op::QueryPerms { user, q } => {
            let pg = db
                .query::<TestObjectPerms>(*user, None, &q)
                .await
                .wrap_context("querying postgres");
            let mem = s
                .mem_db
                .query::<TestObjectPerms>(*user, None, &q)
                .await
                .wrap_context("querying mem");
            cmp_query_results::<TestObjectPerms>(pg, mem).await?;
        }
        Op::QueryDelegatePerms { user, q } => {
            let pg = db
                .query::<TestObjectDelegatePerms>(*user, None, &q)
                .await
                .wrap_context("querying postgres");
            let mem = s
                .mem_db
                .query::<TestObjectDelegatePerms>(*user, None, &q)
                .await
                .wrap_context("querying mem");
            cmp_query_results::<TestObjectDelegatePerms>(pg, mem).await?;
        }
        Op::RecreatePerm { object, time } => {
            let o = s
                .objects
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg = db.recreate::<TestObjectPerms, _>(*time, o, db).await;
            let mem = s
                .mem_db
                .recreate::<TestObjectPerms, _>(*time, o, &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::RecreateDelegator { object, time } => {
            let o = s
                .objects
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
            cmp(pg, mem)?;
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
            let mem = (|| async {
                s.mem_db
                    .recreate_all::<TestObjectPerms>(*recreate_at)
                    .await?;
                s.mem_db
                    .recreate_all::<TestObjectDelegatePerms>(*recreate_at)
                    .await?;
                Ok(())
            })()
            .await;
            let pg = db.vacuum(Some(*recreate_at), None, db, |_| ()).await;
            cmp(pg, mem)?;
        }
    }
    Ok(())
}

fn fuzz_impl(cluster: &TmpDb, ops: &Vec<Op>) {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let pool = cluster.pool().await;
            let db = PostgresDb::connect(pool.clone()).await.unwrap();
            sqlx::query(include_str!("../cleanup-db.sql"))
                .execute(&pool)
                .await
                .unwrap();
            let mut s = FuzzState::new();
            for (i, op) in ops.iter().enumerate() {
                apply_op(&db, &mut s, op)
                    .await
                    .wrap_with_context(|| format!("applying {i}th op: {op:?}"))
                    .unwrap();
                db.assert_invariants_generic().await;
                db.assert_invariants_for::<TestObjectPerms>().await;
                db.assert_invariants_for::<TestObjectDelegatePerms>().await;
            }
        });
}

#[test]
fn fuzz() {
    let cluster = TmpDb::new();
    bolero::check!()
        .with_iterations(20)
        .with_type()
        .for_each(move |ops| fuzz_impl(&cluster, ops))
}

#[test]
fn regression_get_with_wrong_type_did_not_fail() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![
            CreatePerm {
                id: ObjectId(Ulid::from_string("0000000000000000000000002D").unwrap()),
                created_at: EventId(Ulid::from_string("000000000000000000006001S7").unwrap()),
                object: Arc::new(TestObjectPerms(User(
                    Ulid::from_string("002C00C00000001280RG0G0000").unwrap(),
                ))),
            },
            GetDelegator { object: 0 },
        ],
    );
}

#[test]
fn regression_changing_remote_objects_did_not_refresh_perms() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![
            CreateDelegator {
                id: ObjectId(Ulid::from_string("00000000000G000000000G0000").unwrap()),
                created_at: EventId(Ulid::from_string("00ZYNG001A2C09BP0708000000").unwrap()),
                object: Arc::new(TestObjectDelegatePerms(
                    DbPtr::from_string("00000000000000000000000000").unwrap(),
                )),
            },
            CreatePerm {
                id: ObjectId(Ulid::from_string("00000000000000000000000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000001QZZ40FSZ7WZKY26000").unwrap()),
                object: Arc::new(TestObjectPerms(User(
                    Ulid::from_string("00000002004G0004007G054MJJ").unwrap(),
                ))),
            },
        ],
    );
}

#[test]
fn regression_self_referencing_object_deadlocks() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![CreateDelegator {
            id: ObjectId(Ulid::from_string("00008000000030000000000000").unwrap()),
            created_at: EventId(Ulid::from_string("00000000002000001J00000001").unwrap()),
            object: Arc::new(TestObjectDelegatePerms(
                DbPtr::from_string("00008000000030000000000000").unwrap(),
            )),
        }],
    );
}

#[test]
fn regression_submit_wrong_type_ignores_failure() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![
            CreateDelegator {
                id: ObjectId(Ulid::from_string("00000000000000000000000002").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000410000000000X3K").unwrap()),
                object: Arc::new(TestObjectDelegatePerms(
                    DbPtr::from_string("0000062VK4C5S68QV3DXQ6CAG7").unwrap(),
                )),
            },
            SubmitPerm {
                object: 0,
                event_id: EventId(Ulid::from_string("0003ZZZZR00000000000000000").unwrap()),
                event: Arc::new(TestEventPerms::Set(User(
                    Ulid::from_string("00000000000000000000000000").unwrap(),
                ))),
            },
        ],
    );
}
