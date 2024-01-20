#![cfg(all(feature = "client", target_arch = "wasm32"))]

use crdb::{
    crdb_internal::{test_utils::*, Db, LocalDb},
    Query, Timestamp,
};
use futures::stream::StreamExt;
use std::sync::Arc;
use wasm_bindgen_test::{wasm_bindgen_test as test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[test]
async fn smoke_test() {
    tracing_wasm::set_as_global_default();
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let db = LocalDb::connect("smoke-test").await.unwrap();
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObjectSimple::stub_1()),
        &db,
    )
    .await
    .expect("creating test object 1 failed");
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
    db.submit::<TestObjectSimple, _>(
        OBJECT_ID_1,
        EVENT_ID_3,
        Arc::new(TestEventSimple::Clear),
        &db,
    )
    .await
    .expect("clearing object 1 failed");
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    db.submit::<TestObjectSimple, _>(
        OBJECT_ID_1,
        EVENT_ID_3,
        Arc::new(TestEventSimple::Clear),
        &db,
    )
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
            .last_snapshot::<TestObjectSimple>()
            .expect("getting last snapshot")
            .0
    );
    db.vacuum().await.unwrap();
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    let all_objects = db
        .query::<TestObjectSimple>(USER_ID_NULL, None, &Query::All(vec![]))
        .await
        .unwrap()
        .collect::<Vec<crdb::Result<_>>>()
        .await;
    let all_objects = all_objects
        .into_iter()
        .map(|o| o.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(all_objects.len(), 1);
    db.recreate::<TestObjectSimple, _>(
        Timestamp::from_ms(EVENT_ID_2.0.timestamp_ms()),
        OBJECT_ID_1,
        &db,
    )
    .await
    .unwrap();
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
    assert!(!db.remove(OBJECT_ID_1).await.unwrap()); // fail removal, object is not uploaded yet
    db.assert_invariants_generic().await;
    db.assert_invariants_for::<TestObjectSimple>().await;
}
