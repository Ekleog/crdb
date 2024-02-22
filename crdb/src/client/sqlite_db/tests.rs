#![allow(unused_variables, dead_code, unused_imports)] // TODO(sqlite-high): remove

use super::SqliteDb;
use crate::{
    db_trait::{Db, Lock},
    test_utils::{
        self, TestEventSimple, TestObjectSimple, EVENT_ID_1, EVENT_ID_2, EVENT_ID_3, EVENT_ID_4,
        OBJECT_ID_1, OBJECT_ID_2,
    },
    EventId, ObjectId, Updatedness,
};
use anyhow::Context;
use std::{fmt::Debug, sync::Arc};
use ulid::Ulid;

#[sqlx::test]
async fn smoke_test(db: sqlx::SqlitePool) {
    let db = SqliteDb::connect_impl(db).await.expect("connecting to db");

    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObjectSimple::stub_1()),
        Some(Updatedness::from_u128(1)),
        Lock::OBJECT,
    )
    .await
    .expect("creating test object 1 failed");
    /*
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.create(
        OBJECT_ID_1,
        EVENT_ID_2,
        Arc::new(TestObjectSimple::stub_2()),
        &db,
    )
    .await
    .expect_err("creating duplicate test object 1 spuriously worked");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObjectSimple::stub_1()),
        &db,
    )
    .await
    .expect("creating exact copy test object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.submit::<TestObjectSimple, _>(OBJECT_ID_1, EVENT_ID_3, Arc::new(TestEventSimple::Clear), &db)
        .await
        .expect("clearing object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.submit::<TestObjectSimple, _>(OBJECT_ID_1, EVENT_ID_3, Arc::new(TestEventSimple::Clear), &db)
        .await
        .expect("submitting duplicate event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.submit::<TestObjectSimple, _>(
        OBJECT_ID_1,
        EVENT_ID_3,
        Arc::new(TestEventSimple::Set(b"foo".to_vec())),
        &db,
    )
    .await
    .expect_err("submitting duplicate event with different contents worked");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    assert_eq!(
        Vec::<u8>::new(),
        db.get::<TestObjectSimple>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .expect("object 1 is supposed to exist")
            .last_snapshot::<TestObjectSimple>()
            .expect("getting last snapshot")
            .0
    );
    db.submit::<TestObjectSimple, _>(
        OBJECT_ID_1,
        EVENT_ID_2,
        Arc::new(TestEventSimple::Set(b"bar".to_vec())),
        &db,
    )
    .await
    .expect("submitting event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    assert_eq!(
        Vec::<u8>::new(),
        db.get::<TestObjectSimple>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .expect("object 1 is supposed to exist")
            .last_snapshot::<TestObjectSimple>()
            .expect("getting last snapshot")
            .0
    );
    db.submit::<TestObjectSimple, _>(
        OBJECT_ID_1,
        EVENT_ID_4,
        Arc::new(TestEventSimple::Set(b"baz".to_vec())),
        &db,
    )
    .await
    .expect("submitting event failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    assert_eq!(
        b"baz".to_vec(),
        db.get::<TestObjectSimple>(OBJECT_ID_1)
            .await
            .expect("getting object 1")
            .expect("object 1 is supposed to exist")
            .last_snapshot::<TestObjectSimple>()
            .expect("getting last snapshot")
            .0
    );*/
}
