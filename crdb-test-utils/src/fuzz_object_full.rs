#[macro_export]
macro_rules! fuzz_object_full {
    () => {
        use super::fuzz_helpers::{
            self, make_db, make_fuzzer, run_query, run_vacuum, setup, Database, SetupState,
        };
        use anyhow::Context;
        use std::{collections::BTreeSet, sync::Arc};
        use ulid::Ulid;
        use $crate::*;

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
        async fn regression_postgres_and_indexeddb_considered_missing_binaries_the_other_way_around(
        ) {
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
                        users: BTreeSet::new(),
                    }),
                    updatedness: Some(Updatedness::from_u128(1)),
                    lock: Lock::OBJECT.bits(),
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
                        object_id: ObjectId(
                            Ulid::from_string("00000000000000000000000000").unwrap(),
                        ),
                        created_at: EventId(
                            Ulid::from_string("00000000000000000000000000").unwrap(),
                        ),
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from("foo\0bar"),
                            deps: vec![],
                            bins: vec![],
                            users: BTreeSet::new(),
                        }),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: Lock::OBJECT.bits(),
                    },
                    SubmitFull {
                        object_id: 0,
                        event_id: EventId(Ulid::from_string("00000000000000000000000000").unwrap()),
                        event: Arc::new(TestEventFull::Rename(String::from("bar\0foo"))),
                        updatedness: Some(Updatedness::from_u128(1)),
                        force_lock: Lock::OBJECT.bits(),
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
                            users: BTreeSet::new(),
                        }),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::CreateFull {
                        object_id: OBJECT_ID_2,
                        created_at: EVENT_ID_2,
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from(""),
                            deps: vec![DbPtr::from(OBJECT_ID_2)],
                            bins: vec![],
                            users: BTreeSet::new(),
                        }),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: 0,
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
                            users: BTreeSet::new(),
                        }),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::RecreateFull {
                        object_id: 0,
                        new_created_at: EVENT_ID_2,
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from("\0"),
                            deps: vec![],
                            bins: vec![],
                            users: BTreeSet::new(),
                        }),
                        updatedness: Some(Updatedness::from_u128(1)),
                        force_lock: Lock::OBJECT.bits(),
                    },
                ]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_creating_missing_object_did_not_refresh_perms() {
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    Op::CreateFull {
                        object_id: OBJECT_ID_1,
                        created_at: EVENT_ID_1,
                        updatedness: Some(UPDATEDNESS_1),
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from(""),
                            deps: vec![DbPtr::from(OBJECT_ID_2)],
                            bins: vec![],
                            users: BTreeSet::new(),
                        }),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::CreateFull {
                        object_id: OBJECT_ID_2,
                        created_at: EVENT_ID_2,
                        updatedness: Some(UPDATEDNESS_2),
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from(""),
                            deps: vec![],
                            bins: vec![],
                            users: [USER_ID_1].into_iter().collect(),
                        }),
                        lock: Lock::OBJECT.bits(),
                    },
                ]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_serde_serializes_hashmap_order_at_random() {
            // Create a lot of objects that are all the same with a lot of HashMap items
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    Op::CreateFull {
                        object_id: OBJECT_ID_1,
                        created_at: EVENT_ID_1,
                        updatedness: Some(UPDATEDNESS_1),
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from(""),
                            deps: vec![],
                            bins: vec![],
                            users: [USER_ID_1, USER_ID_2, USER_ID_3, USER_ID_4, USER_ID_5]
                                .into_iter()
                                .collect(),
                        }),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::CreateFull {
                        object_id: OBJECT_ID_1,
                        created_at: EVENT_ID_1,
                        updatedness: Some(UPDATEDNESS_2),
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from(""),
                            deps: vec![],
                            bins: vec![],
                            users: [USER_ID_1, USER_ID_2, USER_ID_3, USER_ID_4, USER_ID_5]
                                .into_iter()
                                .collect(),
                        }),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::CreateFull {
                        object_id: OBJECT_ID_1,
                        created_at: EVENT_ID_1,
                        updatedness: Some(UPDATEDNESS_2),
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from(""),
                            deps: vec![],
                            bins: vec![],
                            users: [USER_ID_1, USER_ID_2, USER_ID_3, USER_ID_4, USER_ID_5]
                                .into_iter()
                                .collect(),
                        }),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::CreateFull {
                        object_id: OBJECT_ID_1,
                        created_at: EVENT_ID_1,
                        updatedness: Some(UPDATEDNESS_2),
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from(""),
                            deps: vec![],
                            bins: vec![],
                            users: [USER_ID_1, USER_ID_2, USER_ID_3, USER_ID_4, USER_ID_5]
                                .into_iter()
                                .collect(),
                        }),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::CreateFull {
                        object_id: OBJECT_ID_1,
                        created_at: EVENT_ID_1,
                        updatedness: Some(UPDATEDNESS_2),
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from(""),
                            deps: vec![],
                            bins: vec![],
                            users: [USER_ID_1, USER_ID_2, USER_ID_3, USER_ID_4, USER_ID_5]
                                .into_iter()
                                .collect(),
                        }),
                        lock: Lock::OBJECT.bits(),
                    },
                ]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_fts_query_behavior() {
            // TODO(test-high): fuzz specifically the FTS search, this was not found by fuzzers
            // In particular, postgresql special-cased empty-string requests and always returned false, whereas
            // we would meaningfully always return true, as every string contains the empty string
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    Op::CreateFull {
                        object_id: OBJECT_ID_1,
                        created_at: EVENT_ID_1,
                        updatedness: Some(UPDATEDNESS_1),
                        object: Arc::new(TestObjectFull {
                            name: SearchableString::from("foo bar baz"),
                            deps: vec![],
                            bins: vec![],
                            users: BTreeSet::new(),
                        }),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::QueryFull {
                        user: USER_ID_NULL,
                        only_updated_since: None,
                        query: Arc::new(Query::ContainsStr(
                            vec![JsonPathItem::Key(String::from("name"))],
                            String::from(""),
                        )),
                    },
                    Op::QueryFull {
                        user: USER_ID_NULL,
                        only_updated_since: None,
                        query: Arc::new(Query::ContainsStr(
                            vec![JsonPathItem::Key(String::from("name"))],
                            String::from("fo"),
                        )),
                    },
                    Op::QueryFull {
                        user: USER_ID_NULL,
                        only_updated_since: None,
                        query: Arc::new(Query::ContainsStr(
                            vec![JsonPathItem::Key(String::from("name"))],
                            String::from("foo"),
                        )),
                    },
                    Op::QueryFull {
                        user: USER_ID_NULL,
                        only_updated_since: None,
                        query: Arc::new(Query::ContainsStr(
                            vec![JsonPathItem::Key(String::from("name"))],
                            String::from("bar baz"),
                        )),
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
    };
}
