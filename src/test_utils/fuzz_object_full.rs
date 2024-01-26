use super::fuzz_helpers::{
    self,
    crdb::{
        self,
        crdb_internal::{Db, ResultExt},
        fts::SearchableString,
        make_fuzzer_stuffs,
        test_utils::{self, *},
        BinPtr, DbPtr, EventId, ObjectId, Query, Timestamp, User,
    },
    make_db, make_fuzzer, run_query, run_vacuum, setup, Database, SetupState,
};
use anyhow::Context;
use std::sync::Arc;
use ulid::Ulid;

make_fuzzer_stuffs! {
    (Full, TestObjectFull, TestEventFull),
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
        Arc::new(vec![CreateFull {
            object_id: ObjectId(Ulid::from_string("7R000000000000000000000026").unwrap()),
            created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
            object: Arc::new(TestObjectFull {
                name: SearchableString::new(),
                deps: vec![],
                bins: vec![BinPtr(
                    Ulid::from_string("1TF80000000000000000000000").unwrap(),
                )],
                users: vec![],
            }),
            lock: true,
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
            CreateFull {
                object_id: ObjectId(Ulid::from_string("00000000000000000000000000").unwrap()),
                created_at: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                object: Arc::new(TestObjectFull {
                    name: SearchableString::from("foo\0bar"),
                    deps: vec![],
                    bins: vec![],
                    users: vec![],
                }),
                lock: true,
            },
            SubmitFull {
                object_id: 0,
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

#[fuzz_helpers::test]
async fn regression_memdb_vacuum_did_not_clean_binaries() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::CreateBinary {
                data: vec![1].into(),
                fake_id: None,
            },
            Op::Vacuum { recreate_at: None },
            Op::GetBinary { binary_id: 0 },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_stack_overflow() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::CreateFull {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectFull {
                    name: SearchableString::from(""),
                    deps: vec![DbPtr::from(OBJECT_ID_2)],
                    bins: vec![],
                    users: vec![],
                }),
                lock: true,
            },
            Op::CreateFull {
                object_id: OBJECT_ID_2,
                created_at: EVENT_ID_2,
                object: Arc::new(TestObjectFull {
                    name: SearchableString::from(""),
                    deps: vec![DbPtr::from(OBJECT_ID_2)],
                    bins: vec![],
                    users: vec![],
                }),
                lock: false,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
async fn regression_indexeddb_recreate_did_not_check_for_null_bytes_in_string() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        Arc::new(vec![
            Op::CreateFull {
                object_id: OBJECT_ID_1,
                created_at: EVENT_ID_1,
                object: Arc::new(TestObjectFull {
                    name: SearchableString::from(""),
                    deps: vec![],
                    bins: vec![],
                    users: vec![],
                }),
                lock: true,
            },
            Op::RecreateFull {
                object_id: 0,
                new_created_at: EVENT_ID_2,
                object: Arc::new(TestObjectFull {
                    name: SearchableString::from("\0"),
                    deps: vec![],
                    bins: vec![],
                    users: vec![],
                }),
                force_lock: true,
            },
        ]),
    )
    .await;
}

#[fuzz_helpers::test]
#[cfg(disabled)]
async fn impl_reproducer() {
    let cluster = setup();
    fuzz_impl(
        &cluster,
        serde_json::from_str(include_str!("../../repro.json")).unwrap(),
    )
    .await;
}
