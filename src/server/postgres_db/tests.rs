use super::PostgresDb;
use crate::{
    db_trait::{Db, EventId, ObjectId, Timestamp},
    test_utils::{
        TestEvent1, TestObject1, EVENT_ID_1, EVENT_ID_2, EVENT_ID_3, EVENT_ID_4, OBJECT_ID_1,
    },
};
use anyhow::Context;
use std::{collections::HashMap, sync::Arc};
use ulid::Ulid;

#[sqlx::test]
async fn smoke_test(db: sqlx::PgPool) {
    let db = PostgresDb::connect(db).await.expect("connecting to db");
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObject1::stub_1()),
        &db,
    )
    .await
    .expect("creating test object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    db.create(
        OBJECT_ID_1,
        EVENT_ID_2,
        Arc::new(TestObject1::stub_2()),
        &db,
    )
    .await
    .expect_err("creating duplicate test object 1 spuriously worked");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObject1::stub_1()),
        &db,
    )
    .await
    .expect("creating exact copy test object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    db.submit::<TestObject1, _>(OBJECT_ID_1, EVENT_ID_3, Arc::new(TestEvent1::Clear), &db)
        .await
        .expect("clearing object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    db.submit::<TestObject1, _>(OBJECT_ID_1, EVENT_ID_3, Arc::new(TestEvent1::Clear), &db)
        .await
        .expect("submitting duplicate event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    db.submit::<TestObject1, _>(
        OBJECT_ID_1,
        EVENT_ID_3,
        Arc::new(TestEvent1::Set(b"foo".to_vec())),
        &db,
    )
    .await
    .expect_err("submitting duplicate event with different contents worked");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    assert_eq!(
        Vec::<u8>::new(),
        db.get::<TestObject1>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .expect("object 1 is supposed to exist")
            .last_snapshot::<TestObject1>()
            .expect("getting last snapshot")
            .0
    );
    db.submit::<TestObject1, _>(
        OBJECT_ID_1,
        EVENT_ID_2,
        Arc::new(TestEvent1::Set(b"bar".to_vec())),
        &db,
    )
    .await
    .expect("submitting event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    assert_eq!(
        Vec::<u8>::new(),
        db.get::<TestObject1>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .expect("object 1 is supposed to exist")
            .last_snapshot::<TestObject1>()
            .expect("getting last snapshot")
            .0
    );
    db.submit::<TestObject1, _>(
        OBJECT_ID_1,
        EVENT_ID_4,
        Arc::new(TestEvent1::Set(b"baz".to_vec())),
        &db,
    )
    .await
    .expect("submitting event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObject1>().await;
    assert_eq!(
        b"baz".to_vec(),
        db.get::<TestObject1>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .expect("object 1 is supposed to exist")
            .last_snapshot::<TestObject1>()
            .expect("getting last snapshot")
            .0
    );
}

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
    object_creations: Vec<Arc<TestObject1>>,
    object_created_at: Vec<EventId>,
}

impl FuzzState {
    fn new() -> FuzzState {
        FuzzState {
            objects: Vec::new(),
            object_creations: Vec::new(),
            object_created_at: Vec::new(),
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
            let preexisting = s.objects.iter().position(|o| o == id);
            if preexisting.is_none() {
                s.objects.push(*id);
                s.object_creations.push(object.clone());
                s.object_created_at.push(*created_at);
                db.create(*id, *created_at, object.clone(), db).await?;
            } else {
                let o = preexisting.unwrap();
                if s.object_creations[o] == *object && *created_at == s.object_created_at[o] {
                    db.create(*id, *created_at, object.clone(), db).await?;
                } else {
                    // TODO: be more restrictive on exactly what errors are allowed here and below
                    anyhow::ensure!(
                        db.create(*id, *created_at, object.clone(), db)
                            .await
                            .is_err(),
                        "Creating an object multiple times with different values should fail"
                    );
                }
            }
        }
        Op::Submit {
            object,
            event_id,
            event,
        } => {
            if let Some(o) = s.objects.get(*object) {
                db.submit::<TestObject1, _>(*o, *event_id, event.clone(), db)
                    .await?;
            } else {
                anyhow::ensure!(
                    db.submit::<TestObject1, _>(
                        ObjectId(Ulid::new()),
                        *event_id,
                        event.clone(),
                        db
                    )
                    .await
                    .is_err(),
                    "Submitting event to unknown object should fail"
                );
            }
        }
        Op::Get { object } => {
            if let Some(o) = s.objects.get(*object) {
                // TODO: be a bit more specific?
                anyhow::ensure!(db.get::<TestObject1>(*o).await?.is_some());
            } else {
                anyhow::ensure!(
                    db.get::<TestObject1>(ObjectId(Ulid::new()))
                        .await?
                        .is_none(),
                    "Getting unknown object should fail"
                );
            }
        }
        Op::Recreate { object, time } => {
            if let Some(o) = s.objects.get(*object) {
                // TODO: update s.object_creations and s.created_at
                db.recreate::<TestObject1, _>(*time, *o, db).await?;
            } else {
                anyhow::ensure!(
                    db.recreate::<TestObject1, _>(*time, ObjectId(Ulid::new()), db)
                        .await
                        .is_err(),
                    "Recreating unknown object should fail"
                );
            }
        }
    }
    Ok(())
}

fn db_keeps_invariants_impl(ops: &Vec<Op>) {
    let mut args = HashMap::new();
    args.insert(String::from("tablespace"), String::from("in_memory"));
    sqlx::test::run_test_with_pool(
        sqlx::test::TestArgs {
            test_path: "db_keeps_invariants_impl",
            migrator: None,
            fixtures: &[],
            args,
        },
        |db| async move {
            let db = PostgresDb::connect(db).await.unwrap();
            let mut s = FuzzState::new();
            for (i, op) in ops.iter().enumerate() {
                apply_op(&db, &mut s, op)
                    .await
                    .with_context(|| format!("applying {i}th op: {op:?}"))
                    .unwrap();
                db.assert_invariants_generic().await;
                db.assert_invariants_for::<TestObject1>().await;
            }
        },
    );
}

#[test]
fn db_keeps_invariants() {
    bolero::check!()
        .with_iterations(50)
        .with_type()
        .for_each(db_keeps_invariants_impl)
}

// TODO: add a fuzzer using `reord` that checks for concurrency
