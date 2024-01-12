#![cfg(all(feature = "client", target_arch = "wasm32"))]

use crdb::crdb_internal::{test_utils::*, Db, LocalDb};
use std::sync::Arc;
use wasm_bindgen_test::{wasm_bindgen_test as test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[test]
async fn smoke_test() {
    tracing_wasm::set_as_global_default();
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let db = LocalDb::connect("smoke-test").await.unwrap();
    tracing::info!("initialized db");
    /*
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObjectSimple::stub_1()),
        &db,
    )
    .await
    .expect("creating test object 1 failed");
    tracing::info!("created {OBJECT_ID_1:?}");
    db.create(
        OBJECT_ID_1,
        EVENT_ID_2,
        Arc::new(TestObjectSimple::stub_2()),
        &db,
    )
    .await
    .expect_err("creating duplicate test object 1 spuriously worked");
    tracing::info!("successfully failed creating duplicate-but-changed {OBJECT_ID_1:?}");
    db.create(
        OBJECT_ID_1,
        EVENT_ID_1,
        Arc::new(TestObjectSimple::stub_1()),
        &db,
    )
    .await
    .expect("creating exact copy test object 1 failed");
    tracing::info!("successfully created duplicate {OBJECT_ID_1:?}");
    */
}
