#[macro_export]
macro_rules! fuzz_remote_perms {
    ($db_type:tt) => {
        use super::fuzz_helpers::{
            self, make_db, make_fuzzer, run_query, run_vacuum, setup, Database, SetupState,
        };
        use anyhow::Context;
        use std::sync::Arc;
        use ulid::Ulid;
        use $crate::*;

        make_fuzzer_stuffs! {
            $db_type,
            (Perm, TestObjectPerms, TestEventPerms),
            (Delegator, TestObjectDelegatePerms, TestEventDelegatePerms),
        }

        make_fuzzer!("fuzz_remote_perms", fuzz, fuzz_impl);

        #[fuzz_helpers::test]
        async fn regression_get_with_wrong_type_did_not_fail() {
            use Op::*;
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    CreatePerm {
                        object_id: ObjectId(
                            Ulid::from_string("0000000000000000000000002D").unwrap(),
                        ),
                        created_at: EventId(
                            Ulid::from_string("000000000000000000006001S7").unwrap(),
                        ),
                        object: Arc::new(TestObjectPerms(User(
                            Ulid::from_string("002C00C00000001280RG0G0000").unwrap(),
                        ))),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: Lock::OBJECT.bits(),
                    },
                    GetLatestDelegator {
                        object_id: 0,
                        lock: Lock::OBJECT.bits(),
                    },
                ]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_changing_remote_objects_did_not_refresh_perms() {
            use Op::*;
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    CreateDelegator {
                        object_id: ObjectId(
                            Ulid::from_string("00000000000G000000000G0000").unwrap(),
                        ),
                        created_at: EventId(
                            Ulid::from_string("00ZYNG001A2C09BP0708000000").unwrap(),
                        ),
                        object: Arc::new(TestObjectDelegatePerms(
                            DbPtr::from_string("00000000000000000000000000").unwrap(),
                        )),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: Lock::OBJECT.bits(),
                    },
                    CreatePerm {
                        object_id: ObjectId(
                            Ulid::from_string("00000000000000000000000000").unwrap(),
                        ),
                        created_at: EventId(
                            Ulid::from_string("00000001QZZ40FSZ7WZKY26000").unwrap(),
                        ),
                        object: Arc::new(TestObjectPerms(User(
                            Ulid::from_string("00000002004G0004007G054MJJ").unwrap(),
                        ))),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: Lock::OBJECT.bits(),
                    },
                ]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_self_referencing_object_deadlocks() {
            use Op::*;
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![CreateDelegator {
                    object_id: ObjectId(Ulid::from_string("00008000000030000000000000").unwrap()),
                    created_at: EventId(Ulid::from_string("00000000002000001J00000001").unwrap()),
                    object: Arc::new(TestObjectDelegatePerms(
                        DbPtr::from_string("00008000000030000000000000").unwrap(),
                    )),
                    updatedness: Some(Updatedness::from_u128(1)),
                    lock: Lock::OBJECT.bits(),
                }]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_submit_wrong_type_ignores_failure() {
            use Op::*;
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    CreateDelegator {
                        object_id: ObjectId(
                            Ulid::from_string("00000000000000000000000002").unwrap(),
                        ),
                        created_at: EventId(
                            Ulid::from_string("00000000000410000000000X3K").unwrap(),
                        ),
                        object: Arc::new(TestObjectDelegatePerms(
                            DbPtr::from_string("0000062VK4C5S68QV3DXQ6CAG7").unwrap(),
                        )),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: Lock::OBJECT.bits(),
                    },
                    SubmitPerm {
                        object_id: 0,
                        event_id: EventId(Ulid::from_string("0003ZZZZR00000000000000000").unwrap()),
                        event: Arc::new(TestEventPerms::Set(User(
                            Ulid::from_string("00000000000000000000000000").unwrap(),
                        ))),
                        updatedness: Some(Updatedness::from_u128(1)),
                        force_lock: Lock::OBJECT.bits(),
                    },
                ]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_postgres_not_null_was_null() {
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    Op::CreatePerm {
                        object_id: ObjectId(
                            Ulid::from_string("00040G2081040G2081040G2081").unwrap(),
                        ),
                        created_at: EventId(
                            Ulid::from_string("01040G20810400C1G60R30C1G6").unwrap(),
                        ),
                        object: Arc::new(TestObjectPerms(User(
                            Ulid::from_string("060R30C1G60R30C1G60R30C1G6").unwrap(),
                        ))),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::QueryPerm {
                        user: User(Ulid::from_string("060R30C1G60R30C1G60R30C1G6").unwrap()),
                        only_updated_since: None,
                        query: Arc::new(Query::Not(Box::new(Query::Eq(
                            vec![JsonPathItem::Key("".to_string())],
                            serde_json::Value::Null,
                        )))),
                    },
                ]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_indexeddb_did_not_check_recreation_type_on_nothing_to_do() {
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    Op::CreatePerm {
                        object_id: OBJECT_ID_1,
                        created_at: EVENT_ID_1,
                        object: Arc::new(TestObjectPerms(USER_ID_1)),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::RecreateDelegator {
                        object_id: 0,
                        new_created_at: EVENT_ID_2,
                        object: Arc::new(TestObjectDelegatePerms(DbPtr::from(OBJECT_ID_2))),
                        updatedness: Some(Updatedness::from_u128(1)),
                        force_lock: Lock::OBJECT.bits(),
                    },
                ]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_indexeddb_did_not_check_recreation_type_on_stuff_to_do() {
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    Op::CreatePerm {
                        object_id: OBJECT_ID_1,
                        created_at: EVENT_ID_1,
                        object: Arc::new(TestObjectPerms(USER_ID_1)),
                        updatedness: Some(Updatedness::from_u128(1)),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::SubmitPerm {
                        object_id: 0,
                        event_id: EVENT_ID_2,
                        event: Arc::new(TestEventPerms::Set(USER_ID_2)),
                        updatedness: Some(Updatedness::from_u128(1)),
                        force_lock: Lock::OBJECT.bits(),
                    },
                    Op::RecreateDelegator {
                        object_id: 0,
                        new_created_at: EVENT_ID_3,
                        object: Arc::new(TestObjectDelegatePerms(DbPtr::from(OBJECT_ID_2))),
                        updatedness: Some(Updatedness::from_u128(1)),
                        force_lock: Lock::OBJECT.bits(),
                    },
                ]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_memdb_vacuum_updated_updatedness_even_without_any_change() {
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    Op::CreatePerm {
                        object_id: OBJECT_ID_1,
                        created_at: EVENT_ID_1,
                        object: Arc::new(TestObjectPerms(USER_ID_1)),
                        updatedness: Some(UPDATEDNESS_1),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::Vacuum {
                        recreate_at: Some((EVENT_ID_2, UPDATEDNESS_3)),
                    },
                    Op::QueryPerm {
                        user: USER_ID_1,
                        only_updated_since: Some(UPDATEDNESS_2),
                        query: Arc::new(Query::All(vec![])),
                    },
                ]),
            )
            .await;
        }

        #[fuzz_helpers::test]
        async fn regression_cache_db_returned_early_error_of_the_wrong_kind() {
            let cluster = setup();
            fuzz_impl(
                &cluster,
                Arc::new(vec![
                    Op::CreateDelegator {
                        object_id: ObjectId(
                            Ulid::from_string("33CDHP6RV3CDHP6RV3CDHP6RV3").unwrap(),
                        ),
                        created_at: EventId(
                            Ulid::from_string("33CDHPESV7CXKPESV7CXKPESV7").unwrap(),
                        ),
                        object: Arc::new(TestObjectDelegatePerms(DbPtr::from(ObjectId(
                            Ulid::from_string("37CXKPESV7CXKPESV7CXKPESV7").unwrap(),
                        )))),
                        updatedness: Some(Updatedness(
                            Ulid::from_string("37CXKPESV7CXKPESV7CXKPESV7").unwrap(),
                        )),
                        lock: Lock::OBJECT.bits(),
                    },
                    Op::SubmitDelegator {
                        object_id: 0,
                        event_id: EventId(Ulid::from_string("37CXKPESV7CXKPESV7CXKPESV7").unwrap()),
                        event: Arc::new(TestEventDelegatePerms::Set(DbPtr::from(ObjectId(
                            Ulid::from_string("37CXKPESV7CXKPESV7CXKPESV7").unwrap(),
                        )))),
                        updatedness: Some(Updatedness(
                            Ulid::from_string("37CXKPESV7CXKPESV7CXKPESV7").unwrap(),
                        )),
                        force_lock: Lock::OBJECT.bits(),
                    },
                    Op::Vacuum {
                        recreate_at: Some((
                            EventId(Ulid::from_string("7ZZZZZZZZZZZZZZZR10M3G0000").unwrap()),
                            Updatedness(Ulid::from_string("2VDNPPTVBDDME6ESV7CXKPESV7").unwrap()),
                        )),
                    },
                    Op::CreateDelegator {
                        object_id: ObjectId(
                            Ulid::from_string("3QD1QNYRV1DSFQ4SB1CHKPESV7").unwrap(),
                        ),
                        created_at: EventId(
                            Ulid::from_string("37CXKPESV7CXKPESV7CXKPEWB3").unwrap(),
                        ),
                        object: Arc::new(TestObjectDelegatePerms(DbPtr::from(ObjectId(
                            Ulid::from_string("33CDHP6RV3CDHP6RV3CDHP6RV3").unwrap(),
                        )))),
                        updatedness: Some(Updatedness(
                            Ulid::from_string("33CDHP6RV3CDHP6RWXKJE9JRV3").unwrap(),
                        )),
                        lock: Lock::OBJECT.bits(),
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
