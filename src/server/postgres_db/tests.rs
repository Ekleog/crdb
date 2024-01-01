use super::PostgresDb;
use crate::{
    db_trait::Db,
    test_utils::{TestEvent1, TestObject1, EVENT_ID_1, EVENT_ID_2, OBJECT_ID_1},
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
    db.submit::<TestObject1, _>(OBJECT_ID_1, EVENT_ID_2, Arc::new(TestEvent1::Clear), &db)
        .await
        .expect("clearing object 1 failed");
}
