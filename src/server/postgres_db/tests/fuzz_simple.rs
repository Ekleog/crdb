use super::{cmp_db, TmpDb};
use crate::{
    db_trait::Db,
    error::ResultExt,
    server::postgres_db::PostgresDb,
    test_utils::{
        self, db::ServerConfig, TestEvent1, TestObject1, EVENT_ID_1, EVENT_ID_2, EVENT_ID_3,
        EVENT_ID_4, OBJECT_ID_1, OBJECT_ID_2,
    },
    EventId, ObjectId, Timestamp,
};
use anyhow::Context;
use std::sync::Arc;
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
        Op::Create {
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
                .submit::<TestObject1, _>(o, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp_db(pg, mem)?;
        }
        Op::Get { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
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
                Err(e) => Err(e).wrap_context(&format!("getting {o:?} in mem d)b")),
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
                    .with_context(|| format!("applying {i}th op: {op:?}"))
                    .unwrap();
                db.assert_invariants_generic().await;
                db.assert_invariants_for::<TestObject1>().await;
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
fn regression_events_1342_fails_to_notice_conflict_on_3() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![
            Create {
                id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObject1(b"123".to_vec())),
            },
            Submit {
                object: 0,
                event_id: EVENT_ID_3,
                event: Arc::new(TestEvent1::Clear),
            },
            Submit {
                object: 0,
                event_id: EVENT_ID_4,
                event: Arc::new(TestEvent1::Clear),
            },
            Submit {
                object: 0,
                event_id: EVENT_ID_2,
                event: Arc::new(TestEvent1::Clear),
            },
            Create {
                id: OBJECT_ID_2,
                created_at: EVENT_ID_3,
                object: Arc::new(TestObject1(b"456".to_vec())),
            },
        ],
    );
}

#[test]
fn regession_proper_error_on_recreate_inexistent() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![Recreate {
            object: 0,
            time: Timestamp::from_ms(0),
        }],
    )
}

#[test]
fn regression_wrong_error_on_object_already_exists() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![
            Create {
                id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObject1(vec![0, 0, 0, 0, 0, 2, 0, 252])),
            },
            Create {
                id: OBJECT_ID_1,
                created_at: EVENT_ID_2,
                object: Arc::new(TestObject1(vec![0, 0, 0, 0, 0, 0, 0, 0])),
            },
        ],
    )
}

#[test]
fn regression_postgres_did_not_distinguish_between_object_and_event_conflicts() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![
            Create {
                id: ObjectId(Ulid::from_string("0001SPAWVKD5QPWQV100000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObject1(vec![0, 143, 0, 0, 0, 0, 126, 59])),
            },
            Create {
                id: ObjectId(Ulid::from_string("0058076SBKEDMPYVJZC4000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObject1(vec![0, 0, 244, 0, 105, 111, 110, 0])),
            },
        ],
    )
}

#[test]
fn regression_submit_on_other_snapshot_date_fails() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![
            Create {
                id: ObjectId(Ulid::from_string("0000000000000004PAVG100000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObject1(vec![0, 0, 0, 0, 0, 0, 214, 0])),
            },
            Create {
                id: ObjectId(Ulid::from_string("00000000000000000JS8000000").unwrap()),
                created_at: EventId(Ulid::from_string("0000001ZZZ1BYFZZRVZZZZY000").unwrap()),
                object: Arc::new(TestObject1(vec![0, 0, 0, 0, 0, 0, 1, 0])),
            },
            Submit {
                object: 0,
                event_id: EventId(Ulid::from_string("0000001ZZZ1BYFZZRVZZZZY000").unwrap()),
                event: Arc::new(TestEvent1::Set(vec![0, 0, 0, 0, 0, 0, 0, 0])),
            },
        ],
    );
}

#[test]
fn regression_vacuum_did_not_actually_recreate_objects() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![
            Create {
                id: ObjectId(Ulid::from_string("00000A58N21A8JM00000000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObject1(vec![55, 0, 0, 0, 0, 0, 0, 0])),
            },
            Submit {
                object: 0,
                event_id: EventId(Ulid::from_string("00001000040000000000000000").unwrap()),
                event: Arc::new(TestEvent1::Set(vec![15, 0, 255, 0, 0, 255, 0, 32])),
            },
            Vacuum {
                recreate_at: Some(Timestamp::from_ms(408021893130)),
            },
            Submit {
                object: 0,
                event_id: EventId(Ulid::from_string("00000000000000000000000200").unwrap()),
                event: Arc::new(TestEvent1::Set(vec![6, 0, 0, 0, 0, 0, 0, 0])),
            },
        ],
    );
}
