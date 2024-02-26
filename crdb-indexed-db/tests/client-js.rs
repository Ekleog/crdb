#![cfg(all(feature = "_tests", target_arch = "wasm32"))]

use crdb_indexed_db::IndexedDb;
use wasm_bindgen_test::{wasm_bindgen_test as test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[test]
async fn smoke_test() {
    tracing_wasm::set_as_global_default();
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let db = IndexedDb::connect("smoke-test").await.unwrap();
    crdb_test_utils::smoke_test!(
        db: db,
        query_all: db
            .query(*TestObjectSimple::type_ulid(), Arc::new(Query::All(vec![])))
            .await
            .unwrap(),
        db_type: client,
    );
}

mod fuzz_helpers {
    use bolero::{generator::bolero_generator, ValueGenerator};
    use crdb_core::{Object, Query, ResultExt, Updatedness, User};
    use crdb_indexed_db::IndexedDb;
    use crdb_test_utils::*;
    use rand::{rngs::StdRng, SeedableRng};
    use std::{
        collections::HashSet,
        future::Future,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    pub use wasm_bindgen_test::wasm_bindgen_test as test;

    pub type Database = IndexedDb;
    pub type KeepAlive = ();
    pub type SetupState = ();

    pub fn setup() -> ((), bool) {
        ((), false)
    }

    pub static COUNTER: AtomicUsize = AtomicUsize::new(0);

    pub async fn make_db(_cluster: &()) -> (Database, KeepAlive) {
        (
            IndexedDb::connect(&format!("db{}", COUNTER.fetch_add(1, Ordering::Relaxed)))
                .await
                .unwrap(),
            (),
        )
    }

    pub async fn run_with_seed<Fun, Arg, RetFut>(
        seed: u64,
        show: bool,
        fuzz_impl: Fun,
    ) -> Option<String>
    where
        Fun: FnOnce(&'static ((), bool), Arg) -> RetFut,
        Arg: 'static + serde::Serialize + for<'a> arbitrary::Arbitrary<'a>,
        RetFut: Future<Output = Database>,
    {
        // Generate the input
        let rng = StdRng::seed_from_u64(seed);
        let mut bolero_gen = bolero_generator::driver::Rng::new(rng, &Default::default());
        let Some(input) = bolero::gen_arbitrary().generate(&mut bolero_gen) else {
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
        let db = fuzz_impl(&((), false), input).await;
        db.close();
        web_sys::console::log_1(&format!(" -> cleaning up").into());

        Some(next_db)
    }

    macro_rules! make_fuzzer {
        ($fuzzer_name: expr, $function_name:ident, $fuzz_impl:ident) => {
            #[wasm_bindgen_test::wasm_bindgen_test]
            #[ignore]
            async fn $function_name() {
                use rand::Rng;

                loop {
                    // Get a seed
                    let seed: u64 = rand::thread_rng().gen();
                    web_sys::console::log_1(
                        &format!("Fuzzing {} with seed {seed}", $fuzzer_name).into(),
                    );

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

    pub async fn run_query<T: Object>(
        db: &Database,
        mem_db: &MemDb,
        _user: User,
        _only_updated_since: Option<Updatedness>,
        query: &Arc<Query>,
    ) -> anyhow::Result<()> {
        let db = db
            .query(*T::type_ulid(), query.clone())
            .await
            .wrap_context("querying postgres")
            .map(|r| r.into_iter().collect::<HashSet<_>>());
        let mem = mem_db
            .query::<T>(USER_ID_NULL, None, &query)
            .await
            .wrap_context("querying mem")
            .map(|r| r.into_iter().collect::<HashSet<_>>());
        cmp(db, mem)
    }
}

mod fuzz_simple {
    crdb_test_utils::fuzz_simple!(client);

    #[fuzz_helpers::test]
    #[cfg(disabled)]
    async fn seed_reproducer() {
        fuzz_helpers::run_with_seed(5710355506847336567, true, fuzz_impl).await;
    }
}

mod fuzz_remote_perms {
    crdb_test_utils::fuzz_remote_perms!(client);

    #[fuzz_helpers::test]
    #[cfg(disabled)]
    async fn seed_reproducer() {
        fuzz_helpers::run_with_seed(8002174428813084636, true, fuzz_impl).await;
    }
}

mod fuzz_object_full {
    crdb_test_utils::fuzz_object_full!(client);

    #[fuzz_helpers::test]
    #[cfg(disabled)]
    async fn seed_reproducer() {
        fuzz_helpers::run_with_seed(8002174428813084636, true, fuzz_impl).await;
    }
}
