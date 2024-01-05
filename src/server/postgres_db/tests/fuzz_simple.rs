use super::{cmp_anyhow, cmp_db, TmpDb};
use crate::{
    db_trait::{Db, EventId, ObjectId},
    server::postgres_db::PostgresDb,
    test_utils::{
        self, TestEvent1, TestObject1, EVENT_ID_1, EVENT_ID_2, EVENT_ID_3, EVENT_ID_4, OBJECT_ID_1,
        OBJECT_ID_2,
    },
    Timestamp,
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
            let pg: anyhow::Result<Option<Arc<TestObject1>>> = match db.get::<TestObject1>(o).await
            {
                Err(e) => Err(e).context("getting {o:?} in database"),
                Ok(None) => Ok(None),
                Ok(Some(o)) => match o.last_snapshot::<TestObject1>() {
                    Ok(o) => Ok(Some(o)),
                    Err(e) => Err(e).context("getting last snapshot of {o:?}"),
                },
            };
            let mem: anyhow::Result<Option<Arc<TestObject1>>> =
                match s.mem_db.get::<TestObject1>(o).await {
                    Err(e) => Err(e).context("getting {o:?} in mem db"),
                    Ok(None) => Ok(None),
                    Ok(Some(o)) => match o.last_snapshot::<TestObject1>() {
                        Ok(o) => Ok(Some(o)),
                        Err(e) => Err(e).context("getting last snapshot of {o:?}"),
                    },
                };
            cmp_anyhow(pg, mem)?;
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
            cmp_anyhow(pg, mem)?;
        }
    }
    Ok(())
}

fn db_keeps_invariants_impl(cluster: &TmpDb, ops: &Vec<Op>) {
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
fn db_keeps_invariants() {
    let cluster = TmpDb::new();
    bolero::check!()
        .with_iterations(20)
        .with_shrink_time(std::time::Duration::from_millis(0))
        .with_type()
        .for_each(move |ops| db_keeps_invariants_impl(&cluster, ops))
}

#[test]
fn regression_events_1342_fails_to_notice_conflict_on_3() {
    use Op::*;
    let cluster = TmpDb::new();
    db_keeps_invariants_impl(
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
