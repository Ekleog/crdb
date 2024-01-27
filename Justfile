# export RUST_BACKTRACE := "short"

all *ARGS: fmt (test ARGS) (clippy ARGS)

fmt:
    cargo fmt
    cd examples/basic && cargo fmt
    rustfmt --edition 2021 src/test_utils/fuzz_*.rs

test *ARGS: (test-crate ARGS) (test-example-basic ARGS)

clippy *ARGS:
    cargo clippy {{ARGS}} -- -D warnings

doc:
    cargo doc --all-features --examples
    cargo doc --features client --examples --target wasm32-unknown-unknown

test-standalone *ARGS: (test-crate-standalone ARGS) (test-example-basic ARGS)

make-test-db:
    dropdb crdb-test || true
    createdb crdb-test
    sqlx migrate run --source src/server/migrations/ --database-url "postgres:///crdb-test?host=/run/postgresql"

rebuild-offline-queries: make-test-db
    cargo sqlx prepare --database-url "postgres:///crdb-test?host=/run/postgresql" -- --all-features --tests

list-todo-types:
    rg 'TODO\(' | grep -v Justfile | sed 's/^.*TODO(//;s/).*$//' | sort -u || true
    rg 'TODO[^(]' | grep -v Justfile || true

clean:
    pkill postgres || true
    rm -rf /tmp/crdb-test-pg-* || true
    rm -rf /tmp/.org.chromium.Chromium.* || true

test-crate *ARGS: (test-crate-api ARGS) (test-crate-client-native ARGS) (test-crate-client-js ARGS) (test-crate-server ARGS)
test-crate-standalone *ARGS: (test-crate-api ARGS) (test-crate-client-native ARGS)

test-crate-api *ARGS:
    SQLX_OFFLINE="true" cargo nextest run --features _tests {{ARGS}}

test-crate-client-native *ARGS:
    SQLX_OFFLINE="true" cargo nextest run --features client,_tests {{ARGS}}

test-crate-client-js *ARGS:
    cargo test --features client,_tests --target wasm32-unknown-unknown {{ARGS}}

test-crate-server *ARGS:
    SQLX_OFFLINE="true" cargo nextest run --features server,_tests {{ARGS}}

test-example-basic *ARGS: build-example-basic-client (test-example-basic-host ARGS)

build-example-basic-client:
    cd examples/basic && CARGO_TARGET_DIR="target/wasm" RUSTFLAGS="-Zmacro-backtrace" cargo build --target wasm32-unknown-unknown -p client-js

test-example-basic-host *ARGS:
    cd examples/basic && CARGO_TARGET_DIR="target/host" RUSTFLAGS="-Zmacro-backtrace" cargo nextest run -p api -p server -p client-native {{ARGS}}

fuzz-pg-simple ARGS='':
    cargo bolero test --all-features \
        server::postgres_db::tests::fuzz_simple::fuzz \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_simple__db_keeps_invariants/corpus.nounit \
        {{ARGS}}

fuzz-idb-simple ARGS='':
    # TODO(blocked): remove path override, when https://github.com/rustwasm/wasm-bindgen/pull/3800 lands?
    PATH="../wasm-bindgen/target/debug:$PATH" \
    WASM_BINDGEN_TEST_TIMEOUT=86400 \
    cargo test --features client,_tests --target wasm32-unknown-unknown \
        client_js::fuzz_simple::fuzz \
        --profile fuzz \
        {{ARGS}} \
        -- --include-ignored

fuzz-pg-perms ARGS='':
    cargo bolero test --all-features \
        server::postgres_db::tests::fuzz_remote_perms::fuzz \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_remote_perms__fuzz/corpus.nounit \
        {{ARGS}}

fuzz-idb-perms ARGS='':
    # TODO(blocked): remove path override, when https://github.com/rustwasm/wasm-bindgen/pull/3800 lands?
    PATH="../wasm-bindgen/target/debug:$PATH" \
    WASM_BINDGEN_TEST_TIMEOUT=86400 \
    cargo test --features client,_tests --target wasm32-unknown-unknown \
        client_js::fuzz_remote_perms::fuzz \
        --profile fuzz \
        {{ARGS}} \
        -- --include-ignored

fuzz-pg-threads ARGS='':
    cargo bolero test --all-features \
        server::postgres_db::tests::fuzz_two_threads::fuzz_no_lock_check \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_two_threads__fuzz_no_lock_check/corpus.nounit \
        {{ARGS}}

fuzz-pg-locks ARGS='':
    cargo bolero test --all-features \
        server::postgres_db::tests::fuzz_two_threads::fuzz_checking_locks \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_two_threads__fuzz_checking_locks/corpus.nounit \
        {{ARGS}}

fuzz-pg-full ARGS='':
    cargo bolero test --all-features \
        server::postgres_db::tests::fuzz_object_full::fuzz \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_object_full__fuzz/corpus.nounit \
        {{ARGS}}

fuzz-idb-full ARGS='':
    # TODO(blocked): remove path override, when https://github.com/rustwasm/wasm-bindgen/pull/3800 lands?
    PATH="../wasm-bindgen/target/debug:$PATH" \
    WASM_BINDGEN_TEST_TIMEOUT=86400 \
    cargo test --features client,_tests --target wasm32-unknown-unknown \
        client_js::fuzz_object_full::fuzz \
        --profile fuzz \
        {{ARGS}} \
        -- --include-ignored

fuzz-pg-threads-royale ARGS='':
    cargo bolero test --all-features \
        server::postgres_db::tests::fuzz_battle_royale::fuzz_no_lock_check \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_battle_royale__fuzz_no_lock_check/corpus.nounit \
        {{ARGS}}

fuzz-pg-locks-royale ARGS='':
    cargo bolero test --all-features \
        server::postgres_db::tests::fuzz_battle_royale::fuzz_checking_locks \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_battle_royale__fuzz_checking_locks/corpus.nounit \
        {{ARGS}}

fuzz-pg-sessions ARGS='':
    cargo bolero test --all-features \
        server::postgres_db::tests::fuzz_sessions::fuzz \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_sessions__fuzz/corpus.nounit \
        {{ARGS}}

fuzz-fts-normalizer ARGS='':
    cargo bolero test --all-features \
        fts::tests::fuzz_normalizer \
        --corpus-dir src/__fuzz__/fts__tests__fuzz_normalizer/corpus.nounit \
        {{ARGS}}
