#![cfg(all(feature = "client", target_arch = "wasm32"))]

use crdb::crdb_internal::LocalDb;
use wasm_bindgen_test::{wasm_bindgen_test as test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[test]
async fn smoke_test() {
    tracing_wasm::set_as_global_default();
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let db = LocalDb::connect("smoke-test").await.unwrap();
    crdb::smoke_test!(
        db: db,
        vacuum: db.vacuum(),
        test_remove: true,
    );
}

mod fuzz_helpers {
    use bolero::{generator::bolero_generator, Driver};
    use crdb::{
        crdb_internal::{test_utils::MemDb, LocalDb},
        Timestamp,
    };
    use rand::{rngs::StdRng, SeedableRng};
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
    };

    pub use crdb;
    pub use wasm_bindgen_test::wasm_bindgen_test as test;

    pub type Database = LocalDb;
    pub type SetupState = ();

    pub fn setup() -> () {}

    pub static COUNTER: AtomicUsize = AtomicUsize::new(0);

    pub async fn make_db(_cluster: &()) -> Database {
        LocalDb::connect(&format!("db{}", COUNTER.fetch_add(1, Ordering::Relaxed)))
            .await
            .unwrap()
    }

    pub async fn run_with_seed<Fun, Arg, RetFut>(
        seed: u64,
        show: bool,
        fuzz_impl: Fun,
    ) -> Option<String>
    where
        Fun: FnOnce(&'static (), Arg) -> RetFut,
        Arg: 'static + serde::Serialize + bolero::TypeGenerator,
        RetFut: Future<Output = ()>,
    {
        // Generate the input
        let rng = StdRng::seed_from_u64(seed);
        let mut bolero_gen = bolero_generator::driver::Rng::new(rng, &Default::default());
        let Some(input) = bolero_gen.gen() else {
            web_sys::console::log_1(&format!(" -> invalid input").into());
            return None;
        };

        // Show it
        if show {
            web_sys::console::log_1(
                &format!(
                    "running with input:\n{}",
                    serde_json::to_string(&input).unwrap()
                )
                .into(),
            );
        }

        // Run it
        let next_db = format!("db{}", COUNTER.load(Ordering::Relaxed));
        fuzz_impl(&(), input).await;
        web_sys::console::log_1(&format!(" -> cleaning up").into());

        Some(next_db)
    }

    macro_rules! make_fuzzer {
        ($name:ident, $fuzz_impl:ident) => {
            #[wasm_bindgen_test::wasm_bindgen_test]
            #[ignore]
            async fn $name() {
                use rand::Rng;

                loop {
                    // Get a seed
                    let seed: u64 = rand::thread_rng().gen();
                    web_sys::console::log_1(&format!("Fuzzing with seed {seed}").into());

                    // Run the input
                    let Some(used_db) = fuzz_helpers::run_with_seed(seed, false, $fuzz_impl).await
                    else {
                        continue;
                    };

                    // Cleanup
                    indexed_db::Factory::<()>::get()
                        .expect("failed retrieving factory")
                        .delete_database(&used_db)
                        .await
                        .expect("failed cleaning up test database");
                }
            }
        };
    }

    pub(crate) use make_fuzzer;

    pub async fn run_vacuum(
        db: &Database,
        _mem_db: &MemDb,
        recreate_at: Option<Timestamp>,
    ) -> anyhow::Result<()> {
        match recreate_at {
            None => {
                db.vacuum().await.unwrap();
            }
            Some(_recreate_at) => {
                // TODO: will have some actual stuff to do once (un)lock & co are implemented in MemDb
                db.vacuum().await.unwrap();
            }
        }
        Ok(())
    }
}

mod fuzz_simple {
    include!("../src/test_utils/fuzz_simple.rs");
}
