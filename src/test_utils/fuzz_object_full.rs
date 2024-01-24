use super::fuzz_helpers::{
    self,
    crdb::{
        self,
        crdb_internal::{Db, ResultExt},
        fts::SearchableString,
        test_utils::{self, *},
        BinPtr, EventId, Object, ObjectId, Query, Timestamp, User,
    },
    make_db, make_fuzzer, run_vacuum, setup, Database, SetupState,
};
use anyhow::Context;
use std::{ops::Bound, sync::Arc};
use ulid::Ulid;

#[derive(Debug, arbitrary::Arbitrary, serde::Deserialize, serde::Serialize)]
enum Op {
    Create {
        id: ObjectId,
        created_at: EventId,
        object: Arc<TestObjectFull>,
    },
    Submit {
        object: usize,
        event_id: EventId,
        event: Arc<TestEventFull>,
    },
    Get {
        object: usize,
        at: EventId,
    },
    Query {
        user: User,
        q: Query,
    },
    Recreate {
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
        Op::Create {
            id,
            created_at,
            object,
        } => {
            let mut object = object.clone();
            Arc::make_mut(&mut object).standardize();
            if !object.can_create(USER_ID_NULL, *id, &s.mem_db).await? {
                return Ok(());
            }
            s.objects.push(*id);
            let pg = db.create(*id, *created_at, object.clone(), true, db).await;
            let mem = s
                .mem_db
                .create(*id, *created_at, object.clone(), true, &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::Submit {
            object,
            event_id,
            event,
        } => {
            let o = s.object(*object);
            let pg = db
                .submit::<TestObjectFull, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObjectFull, _>(o, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::Get { object, at } => {
            let o = s.object(*object);
            let pg: crdb::Result<Arc<TestObjectFull>> =
                match db.get::<TestObjectFull>(true, o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                    Ok(o) => match o.get_snapshot_at::<TestObjectFull>(Bound::Included(*at)) {
                        Ok(o) => Ok(o.1),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            let mem: crdb::Result<Arc<TestObjectFull>> =
                match s.mem_db.get::<TestObjectFull>(true, o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in mem d)b")),
                    Ok(o) => match o.get_snapshot_at::<TestObjectFull>(Bound::Included(*at)) {
                        Ok(o) => Ok(o.1),
                        Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                    },
                };
            cmp(pg, mem)?;
        }
        Op::Query { user, q } => {
            let pg = db
                .query::<TestObjectFull>(*user, None, &q)
                .await
                .wrap_context("querying postgres");
            let mem = s
                .mem_db
                .query::<TestObjectFull>(*user, None, &q)
                .await
                .wrap_context("querying mem");
            cmp(pg, mem)?;
        }
        Op::Recreate { object, time } => {
            let o = s.object(*object);
            let pg = db.recreate::<TestObjectFull, _>(*time, o, db).await;
            let mem = s
                .mem_db
                .recreate::<TestObjectFull, _>(*time, o, &s.mem_db)
                .await;
            cmp(pg, mem)?;
        }
        Op::Remove { object } => {
            let _object = object; // TODO(test): implement for non-postgres databases
        }
        Op::CreateBinary { data, fake_id } => {
            let id = fake_id.unwrap_or_else(|| crdb::hash_binary(&data));
            let mem = s.mem_db.create_binary(id, data.clone()).await;
            let pg = db.create_binary(id, data.clone()).await;
            cmp(pg, mem)?;
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
            .with_context(|| format!("applying {i}th op: {op:?}"))
            .unwrap();
        db.assert_invariants_generic().await;
        db.assert_invariants_for::<TestObjectFull>().await;
    }
    db
}

make_fuzzer!("fuzz_object_full", fuzz, fuzz_impl);

#[fuzz_helpers::test]
async fn regression_create_binary_always_failed() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![CreateBinary {
            data: Arc::new([60u8, 164, 171, 171, 123, 98, 174, 193, 202, 183, 86]) as _,
            fake_id: None,
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_postgres_and_indexeddb_considered_missing_binaries_the_other_way_around() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Create {
            id: ObjectId(Ulid::from_string("7R000000000000000000000026").unwrap()),
            created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
            object: Arc::new(TestObjectFull {
                name: SearchableString::new(),
                deps: vec![],
                bins: vec![BinPtr(
                    Ulid::from_string("1TF80000000000000000000000").unwrap(),
                )],
                users: vec![],
            }),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_postgres_crashed_on_null_byte_in_string() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Create {
                id: ObjectId(Ulid::from_string("00000000000000000000000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectFull {
                    name: SearchableString::from("foo\0bar"),
                    deps: vec![],
                    bins: vec![],
                    users: vec![],
                }),
            },
            Submit {
                object: 0,
                event_id: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                event: Arc::new(TestEventFull::Rename(String::from("bar\0foo"))),
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_indexeddb_vacuum_was_borken() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::CreateBinary {
                data: Arc::new([1u8, 2, 3]) as _,
                fake_id: None,
            },
            Op::Vacuum { recreate_at: None },
        ]),
    )
    .await;
}
