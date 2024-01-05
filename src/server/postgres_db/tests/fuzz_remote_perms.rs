use super::{cmp_anyhow, cmp_db, TmpDb};
use crate::{
    db_trait::{Db, EventId, ObjectId},
    server::postgres_db::PostgresDb,
    test_utils::{
        self, TestEventDelegatePerms, TestEventPerms, TestObjectDelegatePerms, TestObjectPerms,
    },
    Timestamp,
};
use anyhow::Context;
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
    /* TODO: `user` should be a usize, and TestObject should have some auth info
    Query {
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Timestamp,
        q: Query,
    },
    */
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
            cmp_db(pg, mem)?;
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
            cmp_db(pg, mem)?;
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
            cmp_db(pg, mem)?;
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
            cmp_db(pg, mem)?;
        }
        Op::GetPerm { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg: anyhow::Result<Option<Arc<TestObjectPerms>>> =
                match db.get::<TestObjectPerms>(o).await {
                    Err(e) => Err(e).context("getting {o:?} in database"),
                    Ok(None) => Ok(None),
                    Ok(Some(o)) => match o.last_snapshot::<TestObjectPerms>() {
                        Ok(o) => Ok(Some(o)),
                        Err(e) => Err(e).with_context(|| format!("getting last snapshot of {o:?}")),
                    },
                };
            let mem: anyhow::Result<Option<Arc<TestObjectPerms>>> =
                match s.mem_db.get::<TestObjectPerms>(o).await {
                    Err(e) => Err(e).context("getting {o:?} in mem db"),
                    Ok(None) => Ok(None),
                    Ok(Some(o)) => match o.last_snapshot::<TestObjectPerms>() {
                        Ok(o) => Ok(Some(o)),
                        Err(e) => Err(e).with_context(|| format!("getting last snapshot of {o:?}")),
                    },
                };
            cmp_anyhow(pg, mem)?;
        }
        Op::GetDelegator { object } => {
            // TODO: use get_snapshot_at instead of last_snapshot
            let o = s
                .objects
                .get(*object)
                .copied()
                .unwrap_or_else(|| ObjectId(Ulid::new()));
            let pg: anyhow::Result<Option<Arc<TestObjectDelegatePerms>>> =
                match db.get::<TestObjectDelegatePerms>(o).await {
                    Err(e) => Err(e).context("getting {o:?} in database"),
                    Ok(None) => Ok(None),
                    Ok(Some(o)) => match o.last_snapshot::<TestObjectDelegatePerms>() {
                        Ok(o) => Ok(Some(o)),
                        Err(e) => Err(e).with_context(|| format!("getting last snapshot of {o:?}")),
                    },
                };
            let mem: anyhow::Result<Option<Arc<TestObjectDelegatePerms>>> =
                match s.mem_db.get::<TestObjectDelegatePerms>(o).await {
                    Err(e) => Err(e).context("getting {o:?} in mem db"),
                    Ok(None) => Ok(None),
                    Ok(Some(o)) => match o.last_snapshot::<TestObjectDelegatePerms>() {
                        Ok(o) => Ok(Some(o)),
                        Err(e) => Err(e).with_context(|| format!("getting last snapshot of {o:?}")),
                    },
                };
            cmp_anyhow(pg, mem)?;
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
            cmp_anyhow(pg, mem)?;
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
            cmp_anyhow(pg, mem)?;
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
        .with_shrink_time(std::time::Duration::from_millis(0))
        .with_type()
        .for_each(move |ops| fuzz_impl(&cluster, ops))
}
