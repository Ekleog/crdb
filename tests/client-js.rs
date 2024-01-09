#![cfg(all(feature = "client", target_arch = "wasm32"))]

use crdb::crdb_internal::LocalDb;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn smoke_test() {
    let _db = LocalDb::connect("smoke-test").await.unwrap();
    //panic!("is_persistent: {:?}", db.is_persistent());
}
