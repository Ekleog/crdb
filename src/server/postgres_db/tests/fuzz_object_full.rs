use super::{cmp_db, TmpDb};
use crate::{
    db_trait::Db,
    error::ResultExt,
    server::postgres_db::PostgresDb,
    test_utils::{self, db::ServerConfig, TestEventFull, TestObjectFull},
    BinPtr, EventId, ObjectId, Timestamp,
};
use anyhow::Context;
use std::sync::Arc;
use ulid::Ulid;

#[derive(Debug, bolero::generator::TypeGenerator)]
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
    CreateBinary {
        data: Arc<Vec<u8>>,
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
                .submit::<TestObjectFull, _>(o, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObjectFull, _>(o, *event_id, event.clone(), &s.mem_db)
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
            let pg: crate::Result<Arc<TestObjectFull>> = match db.get::<TestObjectFull>(o).await {
                Err(e) => Err(e).wrap_context(&format!("getting {o:?} in database")),
                Ok(o) => match o.last_snapshot::<TestObjectFull>() {
                    Ok(o) => Ok(o),
                    Err(e) => Err(e).wrap_context(&format!("getting last snapshot of {o:?}")),
                },
            };
            let mem: crate::Result<Arc<TestObjectFull>> =
                match s.mem_db.get::<TestObjectFull>(o).await {
                    Err(e) => Err(e).wrap_context(&format!("getting {o:?} in mem d)b")),
                    Ok(o) => match o.last_snapshot::<TestObjectFull>() {
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
            let mem = s.mem_db.recreate_all::<TestObjectFull>(*recreate_at).await;
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
                db.assert_invariants_for::<TestObjectFull>().await;
            }
        });
}

#[test]
fn fuzz() {
    let cluster = TmpDb::new();
    bolero::check!()
        .with_iterations(10)
        .with_type()
        .for_each(move |ops| fuzz_impl(&cluster, ops))
}

#[test]
fn regression_create_binary_always_failed() {
    use Op::*;
    let cluster = TmpDb::new();
    fuzz_impl(
        &cluster,
        &vec![CreateBinary {
            data: Arc::new(vec![60, 164, 171, 171, 123, 98, 174, 193, 202, 183, 86]),
            fake_id: None,
        }],
    )
}
