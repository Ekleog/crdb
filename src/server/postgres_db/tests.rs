use super::PostgresDb;
use crate::{
    db_trait::Db,
    test_utils::{
        TestEvent1, TestObject1, EVENT_ID_1, EVENT_ID_2, EVENT_ID_3, EVENT_ID_4, OBJECT_ID_1,
    },
};
use std::sync::Arc;

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
    db.create(
        OBJECT_ID_1,
        EVENT_ID_2,
        Arc::new(TestObject1::stub_2()),
        &db,
    )
    .await
    .expect_err("creating duplicate test object 1 spuriously worked");
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObject1::stub_1()),
        &db,
    )
    .await
    .expect("creating exact copy test object 1 failed");
    db.submit::<TestObject1, _>(OBJECT_ID_1, EVENT_ID_3, Arc::new(TestEvent1::Clear), &db)
        .await
        .expect("clearing object 1 failed");
    db.submit::<TestObject1, _>(OBJECT_ID_1, EVENT_ID_3, Arc::new(TestEvent1::Clear), &db)
        .await
        .expect("submitting duplicate event failed");
    db.submit::<TestObject1, _>(
        OBJECT_ID_1,
        EVENT_ID_3,
        Arc::new(TestEvent1::Set(b"foo".to_vec())),
        &db,
    )
    .await
    .expect_err("submitting duplicate event with different contents worked");
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

// TODO: fuzz the db (by having postgres create its test db in a tablespace in /tmp
// to avoid burning through the disk)
