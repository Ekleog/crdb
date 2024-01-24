use super::fuzz_helpers::{
    self,
    crdb::{
        self,
        crdb_internal::{
            test_utils::{self, *},
            Db, DbPtr, EventId, ObjectId, Query, ResultExt, Timestamp, User,
        },
    },
    make_db, make_fuzzer, run_vacuum, setup, Database, SetupState,
};
use std::{ops::Bound, sync::Arc};
use ulid::Ulid;

#[derive(Debug, arbitrary::Arbitrary, serde::Deserialize, serde::Serialize)]
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
        at: EventId,
    },
    GetDelegator {
        object: usize,
        at: EventId,
    },
    QueryPerms {
        user: User,
        q: Query,
    },
    QueryDelegatePerms {
        user: User,
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
    Remove {
        object: usize,
    },
    Vacuum {
        recreate_at: Option<Timestamp>,
    },
}

struct FuzzState {
    objects: Vec<ObjectId>,
    mem_db: test_utils::MemDb,
}

impl FuzzState {
    fn new(is_server: bool) -> FuzzState {
        FuzzState {
            objects: Vec::new(),
            mem_db: test_utils::MemDb::new(is_server),
        }
    }

    fn object(&self, id: usize) -> ObjectId {
        #[cfg(target_arch = "wasm32")]
        let id = id % (self.objects.len() + 1); // make valid inputs more likely
        self.objects.get(id).copied().unwrap_or_else(ObjectId::now)
    }
}

async fn apply_op(db: &Database, s: &mut FuzzState, op: &Op) -> anyhow::Result<()> {
    match op {
        Op::CreatePerm {
            id,
            created_at,
            object,
        } => {
            s.objects.push(*id);
            let pg = db.create(*id, *created_at, object.clone(), true, db).await;
            let mem = s
                .mem_db
                .create(*id, *created_at, object.clone(), true, &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::CreateDelegator {
            id,
            created_at,
            object,
        } => {
            s.objects.push(*id);
            let pg = db.create(*id, *created_at, object.clone(), true, db).await;
            let mem = s
                .mem_db
                .create(*id, *created_at, object.clone(), true, &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::SubmitPerm {
            object,
            event_id,
            event,
        } => {
            let o = s.object(*object);
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
            let o = s.object(*object);
            let pg = db
                .submit::<TestObjectDelegatePerms, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObjectDelegatePerms, _>(o, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::GetPerm { object, at } => {
            let o = s.object(*object);
            let pg: crdb::Result<Arc<TestObjectPerms>> =
                match db.get::<TestObjectPerms>(true, o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                    Ok(o) => match o.get_snapshot_at::<TestObjectPerms>(Bound::Included(*at)) {
                        Ok(o) => Ok(o.1),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            let mem: crdb::Result<Arc<TestObjectPerms>> =
                match s.mem_db.get::<TestObjectPerms>(true, o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in mem d)b")),
                    Ok(o) => match o.get_snapshot_at::<TestObjectPerms>(Bound::Included(*at)) {
                        Ok(o) => Ok(o.1),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            cmp(pg, mem)?;
        }
        Op::GetDelegator { object, at } => {
            let o = s.object(*object);
            let pg: crdb::Result<Arc<TestObjectDelegatePerms>> = match db
                .get::<TestObjectDelegatePerms>(true, o)
                .await
            {
                Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                Ok(o) => match o.get_snapshot_at::<TestObjectDelegatePerms>(Bound::Included(*at)) {
                    Ok(o) => Ok(o.1),
                    Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                },
            };
            let mem: crdb::Result<Arc<TestObjectDelegatePerms>> =
                match s.mem_db.get::<TestObjectDelegatePerms>(true, o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in mem d)b")),
                    Ok(o) => match o
                        .get_snapshot_at::<TestObjectDelegatePerms>(Bound::Included(*at))
                    {
                        Ok(o) => Ok(o.1),
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
            cmp(pg, mem)?;
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
            cmp(pg, mem)?;
        }
        Op::RecreatePerm { object, time } => {
            let o = s.object(*object);
            let pg = db.recreate::<TestObjectPerms, _>(*time, o, db).await;
            let mem = s
                .mem_db
                .recreate::<TestObjectPerms, _>(*time, o, &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::RecreateDelegator { object, time } => {
            let o = s.object(*object);
            let pg = db
                .recreate::<TestObjectDelegatePerms, _>(*time, o, db)
                .await;
            let mem = s
                .mem_db
                .recreate::<TestObjectDelegatePerms, _>(*time, o, &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::Remove { object } => {
            let _object = object; // TODO(test): implement for non-postgres databases
        }
        Op::Vacuum { recreate_at } => {
            run_vacuum(&db, &s.mem_db, *recreate_at).await?;
        }
    }
    Ok(())
}

async fn fuzz_impl((cluster, is_server): &(SetupState, bool), ops: Arc<Vec<Op>>) -> Database {
    let db = make_db(cluster).await;
    let mut s = FuzzState::new(*is_server);
    for (i, op) in ops.iter().enumerate() {
        apply_op(&db, &mut s, op)
            .await
            .wrap_with_context(|| format!("applying {i}th op: {op:?}"))
            .unwrap();
        db.assert_invariants_generic().await;
        db.assert_invariants_for::<TestObjectPerms>().await;
        db.assert_invariants_for::<TestObjectDelegatePerms>().await;
    }
    db
}

make_fuzzer!("fuzz_remote_perms", fuzz, fuzz_impl);

#[fuzz_helpers::test]
async fn regression_get_with_wrong_type_did_not_fail() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            CreatePerm {
                id: ObjectId(Ulid::from_string("0000000000000000000000002D").unwrap()),
                created_at: EventId(Ulid::from_string("000000000000000000006001S7").unwrap()),
                object: Arc::new(TestObjectPerms(User(
                    Ulid::from_string("002C00C00000001280RG0G0000").unwrap(),
                ))),
            },
            GetDelegator {
                object: 0,
                at: EVENT_ID_MAX,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_changing_remote_objects_did_not_refresh_perms() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
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
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_self_referencing_object_deadlocks() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![CreateDelegator {
            id: ObjectId(Ulid::from_string("00008000000030000000000000").unwrap()),
            created_at: EventId(Ulid::from_string("00000000002000001J00000001").unwrap()),
            object: Arc::new(TestObjectDelegatePerms(
                DbPtr::from_string("00008000000030000000000000").unwrap(),
            )),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_submit_wrong_type_ignores_failure() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
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
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_postgres_not_null_was_null() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::CreatePerm {
                id: ObjectId(Ulid::from_string("00040G2081040G2081040G2081").unwrap()),
                created_at: EventId(Ulid::from_string("01040G20810400C1G60R30C1G6").unwrap()),
                object: Arc::new(TestObjectPerms(User(
                    Ulid::from_string("060R30C1G60R30C1G60R30C1G6").unwrap(),
                ))),
            },
            Op::QueryPerms {
                user: User(Ulid::from_string("060R30C1G60R30C1G60R30C1G6").unwrap()),
                q: Query::Not(Box::new(Query::Eq(
                    vec![crdb::JsonPathItem::Key("".to_string())],
                    serde_json::Value::Null,
                ))),
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_indexeddb_did_not_check_recreation_type_on_nothing_to_do() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::CreatePerm {
                id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectPerms(USER_ID_1)),
            },
            Op::RecreateDelegator {
                object: 0,
                time: Timestamp::from_ms(1),
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_indexeddb_did_not_check_recreation_type_on_stuff_to_do() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::CreatePerm {
                id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectPerms(USER_ID_1)),
            },
            Op::SubmitPerm {
                object: 0,
                event_id: EVENT_ID_2,
                event: Arc::new(TestEventPerms::Set(USER_ID_2)),
            },
            Op::RecreateDelegator {
                object: 0,
                time: Timestamp::from_ms(1 << 47),
            },
        ]),
    )
    .await;
}
