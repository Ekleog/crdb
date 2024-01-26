use super::fuzz_helpers::{
    self,
    crdb::{
        crdb_internal::{
            test_utils::{self, *},
            Db, ResultExt,
        },
        generic_op, EventId, JsonPathItem, ObjectId, Query, Timestamp, User,
    },
    make_db, make_fuzzer, run_query, run_vacuum, setup, Database, SetupState,
};

use anyhow::Context;
use rust_decimal::Decimal;
use std::{str::FromStr, sync::Arc};
use ulid::Ulid;

generic_op!(GenericOp);

#[derive(Debug, arbitrary::Arbitrary, serde::Deserialize, serde::Serialize)]
enum Op {
    Create {
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<TestObjectSimple>,
        lock: bool,
    },
    Submit {
        object_id: usize,
        event_id: EventId,
        event: Arc<TestEventSimple>,
    },
    GetLatest {
        object_id: usize,
        lock: bool,
    },
    // TODO(test): also test GetAll
    Query {
        user: User,
        // TODO(test): figure out a way to test only_updated_since
        q: Query,
    },
    Recreate {
        object_id: usize,
        new_created_at: EventId,
        object: Arc<TestObjectSimple>,
        force_lock: bool,
    },
    Generic(GenericOp),
}

struct FuzzState {
    is_server: bool,
    objects: Vec<ObjectId>,
    mem_db: test_utils::MemDb,
}

impl FuzzState {
    fn new(is_server: bool) -> FuzzState {
        FuzzState {
            is_server,
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
            object_id,
            created_at,
            object,
            mut lock,
        } => {
            s.objects.push(*object_id);
            lock |= s.is_server;
            let db = db
                .create(*object_id, *created_at, object.clone(), lock, db)
                .await;
            let mem = s
                .mem_db
                .create(*object_id, *created_at, object.clone(), lock, &s.mem_db)
                .await;
            cmp(db, mem)?;
        }
        Op::Submit {
            object_id,
            event_id,
            event,
        } => {
            let object_id = s.object(*object_id);
            let db = db
                .submit::<TestObjectSimple, _>(object_id, *event_id, event.clone(), db)
                .await;
            let mem = s
                .mem_db
                .submit::<TestObjectSimple, _>(object_id, *event_id, event.clone(), &s.mem_db)
                .await;
            cmp(db, mem)?;
        }
        Op::GetLatest {
            object_id,
            mut lock,
        } => {
            let object_id = s.object(*object_id);
            lock |= s.is_server;
            let db = db
                .get_latest::<TestObjectSimple>(lock, object_id)
                .await
                .wrap_context(&format!("getting {object_id:?} in database"));
            let mem = s
                .mem_db
                .get_latest::<TestObjectSimple>(lock, object_id)
                .await
                .wrap_context(&format!("getting {object_id:?} in mem db"));
            cmp(db, mem)?;
        }
        Op::Query { user, q } => {
            run_query::<TestObjectSimple>(&db, &s.mem_db, *user, None, q).await?;
        }
        Op::Recreate {
            object_id,
            new_created_at,
            object,
            force_lock,
        } => {
            if !s.is_server {
                let object_id = s.object(*object_id);
                let db = db
                    .recreate::<TestObjectSimple, _>(
                        object_id,
                        *new_created_at,
                        object.clone(),
                        *force_lock,
                        db,
                    )
                    .await;
                let mem = s
                    .mem_db
                    .recreate::<TestObjectSimple, _>(
                        object_id,
                        *new_created_at,
                        object.clone(),
                        *force_lock,
                        &s.mem_db,
                    )
                    .await;
                cmp(db, mem)?;
            }
        }
        Op::Generic(op) => op.apply(db, s).await?,
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
        db.assert_invariants_for::<TestObjectSimple>().await;
    }
    db
}

make_fuzzer!("fuzz_simple", fuzz, fuzz_impl);

#[fuzz_helpers::test]
async fn regression_events_1342_fails_to_notice_conflict_on_3() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Create {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectSimple(b"123".to_vec())),
                lock: true,
            },
            Submit {
                object_id: 0,
                event_id: EVENT_ID_3,
                event: Arc::new(TestEventSimple::Clear),
            },
            Submit {
                object_id: 0,
                event_id: EVENT_ID_4,
                event: Arc::new(TestEventSimple::Clear),
            },
            Submit {
                object_id: 0,
                event_id: EVENT_ID_2,
                event: Arc::new(TestEventSimple::Clear),
            },
            Create {
                object_id: OBJECT_ID_2,
                created_at: EVENT_ID_3,
                object: Arc::new(TestObjectSimple(b"456".to_vec())),
                lock: true,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_proper_error_on_recreate_inexistent() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Recreate {
            object_id: 0,
            new_created_at: EVENT_ID_NULL,
            object: Arc::new(TestObjectSimple::stub_1()),
            force_lock: true,
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_wrong_error_on_object_already_exists() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Create {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 0, 0, 2, 0, 252])),
                lock: true,
            },
            Create {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_2,
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 0, 0, 0, 0, 0])),
                lock: true,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_postgres_did_not_distinguish_between_object_and_event_conflicts() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Create {
                object_id: ObjectId(Ulid::from_string("0001SPAWVKD5QPWQV100000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 143, 0, 0, 0, 0, 126, 59])),
                lock: true,
            },
            Create {
                object_id: ObjectId(Ulid::from_string("0058076SBKEDMPYVJZC4000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 244, 0, 105, 111, 110, 0])),
                lock: true,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_submit_on_other_snapshot_date_fails() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Create {
                object_id: ObjectId(Ulid::from_string("0000000000000004PAVG100000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 0, 0, 0, 214, 0])),
                lock: true,
            },
            Create {
                object_id: ObjectId(Ulid::from_string("00000000000000000JS8000000").unwrap()),
                created_at: EventId(Ulid::from_string("0000001ZZZ1BYFZZRVZZZZY000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 0, 0, 0, 1, 0])),
                lock: true,
            },
            Submit {
                object_id: 0,
                event_id: EventId(Ulid::from_string("0000001ZZZ1BYFZZRVZZZZY000").unwrap()),
                event: Arc::new(TestEventSimple::Set(vec![0, 0, 0, 0, 0, 0, 0, 0])),
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_vacuum_did_not_actually_recreate_objects() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Create {
                object_id: ObjectId(Ulid::from_string("00000A58N21A8JM00000000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![55, 0, 0, 0, 0, 0, 0, 0])),
                lock: true,
            },
            Submit {
                object_id: 0,
                event_id: EventId(Ulid::from_string("00001000040000000000000000").unwrap()),
                event: Arc::new(TestEventSimple::Set(vec![15, 0, 255, 0, 0, 255, 0, 32])),
            },
            Generic(GenericOp::Vacuum {
                recreate_at: Some(Timestamp::from_ms(408021893130)),
            }),
            Submit {
                object_id: 0,
                event_id: EventId(Ulid::from_string("00000000000000000000000200").unwrap()),
                event: Arc::new(TestEventSimple::Set(vec![6, 0, 0, 0, 0, 0, 0, 0])),
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_object_with_two_snapshots_was_not_detected_as_object_id_conflict() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Create {
                object_id: ObjectId(Ulid::from_string("00000000000000000000000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 0, 0, 0, 75, 0])),
                lock: true,
            },
            Submit {
                object_id: 0,
                event_id: EventId(Ulid::from_string("00000000510002P00000000000").unwrap()),
                event: Arc::new(TestEventSimple::Append(vec![0, 0, 0, 0, 0, 0, 0, 0])),
            },
            Create {
                object_id: ObjectId(Ulid::from_string("00000000000000000000000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000188000NG0000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 0, 1, 0, 0, 4])),
                lock: true,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_any_query_crashed_postgres() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Query {
            user: User(Ulid::from_string("00000020000G10000000006000").unwrap()),
            q: Query::All(vec![]),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_postgres_bignumeric_comparison_with_json_needs_cast() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Query {
            user: User(Ulid::from_string("00000020000G10000000006000").unwrap()),
            q: Query::Lt(vec![], Decimal::from_str("0").unwrap()),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_keyed_comparison_was_still_wrong_syntax() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Query {
            user: User(Ulid::from_string("00000020000G10000000006000").unwrap()),
            q: Query::Ge(
                vec![JsonPathItem::Key(String::new())],
                Decimal::from_str("0").unwrap(),
            ),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_too_big_decimal_failed_postgres() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Query {
            user: User(Ulid::from_string("00000020000G10000000006000").unwrap()),
            q: Query::Ge(
                vec![JsonPathItem::Key(String::new())],
                Decimal::from_str(&format!("0.{:030000}1", 0)).unwrap(),
            ),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_postgresql_syntax_for_equality() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Query {
            user: User(Ulid::from_string("00000020000G10000000006000").unwrap()),
            q: Query::Eq(
                vec![JsonPathItem::Key(String::new())],
                serde_json::Value::Null,
            ),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_checked_add_signed_for_u64_cannot_go_below_zero() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Query {
            user: User(Ulid::from_string("00000020000G10000000006000").unwrap()),
            q: Query::Le(
                vec![],
                Decimal::from_str(&format!("0.{:0228}1", 0)).unwrap(),
            ),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_way_too_big_decimal_caused_problems() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Query {
            user: User(Ulid::from_string("00000020000G10000000006000").unwrap()),
            q: Query::Le(
                vec![],
                Decimal::from_str(&format!("0.{:057859}1", 0)).unwrap(),
            ),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_strings_are_in_keys_too() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Query {
            user: User(Ulid::from_string("00000020000G10000000006000").unwrap()),
            q: Query::Le(
                vec![JsonPathItem::Key(String::from("\0"))],
                Decimal::from_str("0").unwrap(),
            ),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_cast_error() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::Create {
                object_id: ObjectId(Ulid::from_string("000000000000000000000002G0").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 4, 6, 75, 182, 0])),
                lock: true,
            },
            Op::Query {
                user: User(Ulid::from_string("00000000000000000000000001").unwrap()),
                q: Query::Le(vec![], Decimal::from_str("0").unwrap()),
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_sql_injection_in_path_key() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Query {
            user: User(Ulid::from_string("030C1G60R30C1G60R30C1G60R3").unwrap()),
            q: Query::Eq(
                vec![JsonPathItem::Key(String::from("'a"))],
                serde_json::Value::Null,
            ),
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_sqlx_had_a_bug_with_prepared_queries_of_different_types() {
    // See https://github.com/launchbadge/sqlx/issues/2981
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::Recreate {
                object_id: 0,
                new_created_at: EVENT_ID_NULL,
                object: Arc::new(TestObjectSimple::stub_1()),
                force_lock: true,
            },
            Op::Query {
                user: USER_ID_NULL,
                q: Query::Eq(
                    vec![
                        JsonPathItem::Key(String::from("a")),
                        JsonPathItem::Key(String::from("a")),
                    ],
                    serde_json::Value::Null,
                ),
            },
            Op::Query {
                user: USER_ID_NULL,
                q: Query::Eq(vec![], serde_json::Value::Null),
            },
            Op::Query {
                user: USER_ID_NULL,
                q: Query::Eq(vec![], serde_json::Value::Null),
            },
            Op::Query {
                user: USER_ID_NULL,
                q: Query::Eq(vec![], serde_json::Value::Null),
            },
            Op::Query {
                user: USER_ID_NULL,
                q: Query::Eq(
                    vec![JsonPathItem::Id(1), JsonPathItem::Key(String::from("a"))],
                    serde_json::Value::Null,
                ),
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_postgres_null_led_to_not_being_wrong() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::Create {
                object_id: ObjectId(Ulid::from_string("000002C1800G08000000000000").unwrap()),
                created_at: EventId(Ulid::from_string("0000000000200000000002G000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 255, 255, 255, 0, 0])),
                lock: true,
            },
            Op::Query {
                user: User(Ulid::from_string("00000000000000000000000000").unwrap()),
                q: Query::Not(Box::new(Query::ContainsStr(vec![], String::new()))),
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_postgres_handled_numbers_as_one_element_arrays() {
    // See also https://www.postgresql.org/message-id/87h6jbbxma.fsf%40coegni.ekleog.org
    // tracing_subscriber::FmtSubscriber::builder()
    //     .with_max_level(tracing::Level::TRACE)
    //     .init();

    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::Create {
                object_id: ObjectId(Ulid::from_string("0000001YR00020000002G002G0").unwrap()),
                created_at: EventId(Ulid::from_string("0003XA00000G22PB005R1G6000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 3, 3, 3, 3, 3, 3])),
                lock: true,
            },
            Op::Query {
                user: User(Ulid::from_string("00000000000000000000000000").unwrap()),
                q: Query::Lt(
                    vec![JsonPathItem::Id(-1), JsonPathItem::Id(-1)],
                    Decimal::from(158),
                ),
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_indexeddb_recreation_considered_dates_the_other_way_around() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::Create {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectSimple(vec![221, 218])),
                lock: true,
            },
            Op::Recreate {
                object_id: 0,
                new_created_at: EVENT_ID_2,
                object: Arc::new(TestObjectSimple(vec![])),
                force_lock: true,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_indexeddb_recreation_did_not_fail_upon_back_in_time() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::Create {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_2,
                object: Arc::new(TestObjectSimple(vec![221, 218])),
                lock: true,
            },
            Op::Recreate {
                object_id: 0,
                new_created_at: EVENT_ID_1,
                object: Arc::new(TestObjectSimple(vec![])),
                force_lock: true,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_memdb_recreation_of_non_existent_deadlocked() {
    // tracing_wasm::set_as_global_default();
    // std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Recreate {
            object_id: 0,
            new_created_at: EVENT_ID_1,
            object: Arc::new(TestObjectSimple(vec![])),
            force_lock: true,
        }]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_indexeddb_removal_of_nonexistent_object_had_wrong_error_message() {
    // tracing_wasm::set_as_global_default();
    // std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Generic(GenericOp::Remove { object_id: 0 })]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_memdb_unlocking_of_nonexistent_object_had_wrong_error_message() {
    // tracing_wasm::set_as_global_default();
    // std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Generic(GenericOp::Unlock { object_id: 0 })]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_memdb_did_not_vacuum_unlocked_objects() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::Create {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_2,
                object: Arc::new(TestObjectSimple(vec![1])),
                lock: false,
            },
            Op::Generic(GenericOp::Vacuum { recreate_at: None }),
            Op::Recreate {
                object_id: 0,
                new_created_at: EVENT_ID_1,
                object: Arc::new(TestObjectSimple(vec![2])),
                force_lock: true,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
#[cfg(disable)]
async fn impl_reproducer() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        serde_json::from_str(include_str!("../../repro.json")).unwrap(),
    )
    .await;
}
