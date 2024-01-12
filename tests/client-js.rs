#![cfg(all(feature = "client", target_arch = "wasm32"))]

use crdb::crdb_internal::{test_utils::*, Db, LocalDb};
use std::sync::Arc;
use wasm_bindgen_test::{wasm_bindgen_test as test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[test]
#[should_panic] // TODO: remove
async fn smoke_test() {
    let db = LocalDb::connect("smoke-test").await.unwrap();
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObjectSimple::stub_1()),
        &db,
    )
    .await
    .expect("creating test object 1 failed");
}
