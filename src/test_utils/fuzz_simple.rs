use super::fuzz_helpers::{
    self,
    crdb::{
        self,
        crdb_internal::{
            test_utils::{self, *},
            Db, ResultExt,
        },
        make_fuzzer_stuffs, BinPtr, EventId, JsonPathItem, ObjectId, Query, Timestamp, User,
    },
    make_db, make_fuzzer, run_query, run_vacuum, setup, Database, SetupState,
};

use anyhow::Context;
use rust_decimal::Decimal;
use std::{str::FromStr, sync::Arc};
use ulid::Ulid;

make_fuzzer_stuffs! {
    (Simple, TestObjectSimple, TestEventSimple),
}

make_fuzzer!("fuzz_simple", fuzz, fuzz_impl);

#[fuzz_helpers::test]
async fn regression_events_1342_fails_to_notice_conflict_on_3() {
    use Op::*;
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            CreateSimple {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectSimple(b"123".to_vec())),
                lock: true,
            },
            SubmitSimple {
                object_id: 0,
                event_id: EVENT_ID_3,
                event: Arc::new(TestEventSimple::Clear),
            },
            SubmitSimple {
                object_id: 0,
                event_id: EVENT_ID_4,
                event: Arc::new(TestEventSimple::Clear),
            },
            SubmitSimple {
                object_id: 0,
                event_id: EVENT_ID_2,
                event: Arc::new(TestEventSimple::Clear),
            },
            CreateSimple {
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
        Arc::new(vec![RecreateSimple {
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
            CreateSimple {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 0, 0, 2, 0, 252])),
                lock: true,
            },
            CreateSimple {
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
            CreateSimple {
                object_id: ObjectId(Ulid::from_string("0001SPAWVKD5QPWQV100000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 143, 0, 0, 0, 0, 126, 59])),
                lock: true,
            },
            CreateSimple {
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
            CreateSimple {
                object_id: ObjectId(Ulid::from_string("0000000000000004PAVG100000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 0, 0, 0, 214, 0])),
                lock: true,
            },
            CreateSimple {
                object_id: ObjectId(Ulid::from_string("00000000000000000JS8000000").unwrap()),
                created_at: EventId(Ulid::from_string("0000001ZZZ1BYFZZRVZZZZY000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 0, 0, 0, 1, 0])),
                lock: true,
            },
            SubmitSimple {
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
            CreateSimple {
                object_id: ObjectId(Ulid::from_string("00000A58N21A8JM00000000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![55, 0, 0, 0, 0, 0, 0, 0])),
                lock: true,
            },
            SubmitSimple {
                object_id: 0,
                event_id: EventId(Ulid::from_string("00001000040000000000000000").unwrap()),
                event: Arc::new(TestEventSimple::Set(vec![15, 0, 255, 0, 0, 255, 0, 32])),
            },
            Vacuum {
                recreate_at: Some(Timestamp::from_ms(408021893130)),
            },
            SubmitSimple {
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
            CreateSimple {
                object_id: ObjectId(Ulid::from_string("00000000000000000000000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 0, 0, 0, 75, 0])),
                lock: true,
            },
            SubmitSimple {
                object_id: 0,
                event_id: EventId(Ulid::from_string("00000000510002P00000000000").unwrap()),
                event: Arc::new(TestEventSimple::Append(vec![0, 0, 0, 0, 0, 0, 0, 0])),
            },
            CreateSimple {
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
        Arc::new(vec![Op::QuerySimple {
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
        Arc::new(vec![Op::QuerySimple {
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
        Arc::new(vec![Op::QuerySimple {
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
        Arc::new(vec![Op::QuerySimple {
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
        Arc::new(vec![Op::QuerySimple {
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
        Arc::new(vec![Op::QuerySimple {
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
        Arc::new(vec![Op::QuerySimple {
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
        Arc::new(vec![Op::QuerySimple {
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
            Op::CreateSimple {
                object_id: ObjectId(Ulid::from_string("000000000000000000000002G0").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 4, 6, 75, 182, 0])),
                lock: true,
            },
            Op::QuerySimple {
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
        Arc::new(vec![Op::QuerySimple {
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
            Op::RecreateSimple {
                object_id: 0,
                new_created_at: EVENT_ID_NULL,
                object: Arc::new(TestObjectSimple::stub_1()),
                force_lock: true,
            },
            Op::QuerySimple {
                user: USER_ID_NULL,
                q: Query::Eq(
                    vec![
                        JsonPathItem::Key(String::from("a")),
                        JsonPathItem::Key(String::from("a")),
                    ],
                    serde_json::Value::Null,
                ),
            },
            Op::QuerySimple {
                user: USER_ID_NULL,
                q: Query::Eq(vec![], serde_json::Value::Null),
            },
            Op::QuerySimple {
                user: USER_ID_NULL,
                q: Query::Eq(vec![], serde_json::Value::Null),
            },
            Op::QuerySimple {
                user: USER_ID_NULL,
                q: Query::Eq(vec![], serde_json::Value::Null),
            },
            Op::QuerySimple {
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
            Op::CreateSimple {
                object_id: ObjectId(Ulid::from_string("000002C1800G08000000000000").unwrap()),
                created_at: EventId(Ulid::from_string("0000000000200000000002G000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 0, 255, 255, 255, 0, 0])),
                lock: true,
            },
            Op::QuerySimple {
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
            Op::CreateSimple {
                object_id: ObjectId(Ulid::from_string("0000001YR00020000002G002G0").unwrap()),
                created_at: EventId(Ulid::from_string("0003XA00000G22PB005R1G6000").unwrap()),
                object: Arc::new(TestObjectSimple(vec![0, 0, 3, 3, 3, 3, 3, 3])),
                lock: true,
            },
            Op::QuerySimple {
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
            Op::CreateSimple {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectSimple(vec![221, 218])),
                lock: true,
            },
            Op::RecreateSimple {
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
            Op::CreateSimple {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_2,
                object: Arc::new(TestObjectSimple(vec![221, 218])),
                lock: true,
            },
            Op::RecreateSimple {
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
        Arc::new(vec![Op::RecreateSimple {
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
    fuzz_impl(&cluster, Arc::new(vec![Op::Remove { object_id: 0 }])).await;
}

#[fuzz_helpers::test]
async fn regression_memdb_unlocking_of_nonexistent_object_had_wrong_error_message() {
    // tracing_wasm::set_as_global_default();
    // std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let cluster = setup();
    fuzz_impl(&cluster, Arc::new(vec![Op::Unlock { object_id: 0 }])).await;
}

#[fuzz_helpers::test]
async fn regression_memdb_did_not_vacuum_unlocked_objects() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::CreateSimple {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_2,
                object: Arc::new(TestObjectSimple(vec![1])),
                lock: false,
            },
            Op::Vacuum { recreate_at: None },
            Op::RecreateSimple {
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
async fn regression_memdb_recreate_did_not_recompute_latest_snapshot_right() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::CreateSimple {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectSimple(vec![231])),
                lock: true,
            },
            Op::SubmitSimple {
                object_id: 1296584126,
                event_id: EVENT_ID_3,
                event: Arc::new(TestEventSimple::Append(vec![111])),
            },
            Op::RecreateSimple {
                object_id: 2039216500,
                new_created_at: EVENT_ID_2,
                object: Arc::new(TestObjectSimple(vec![])),
                force_lock: true,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_memdb_vacuum_very_late_gave_error_outside_cmp() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![Op::Vacuum {
            recreate_at: Some(Timestamp::from_ms(17831803561258567669)),
        }]),
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
