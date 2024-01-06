export RUST_BACKTRACE := "short"

fmt:
    cargo fmt
    cd examples/basic && cargo fmt

test NAME='': (test-crate NAME) (test-example-basic NAME)

doc:
    # this somehow breaks regular doc?
    # RUSTDOCFLAGS="-Z unstable-options --document-hidden-items"
    cargo doc --all-features --document-private-items

test-no-pg NAME='': (test-crate-no-pg NAME) (test-example-basic NAME)

make-test-db:
    dropdb crdb-test || true
    createdb crdb-test
    sqlx migrate run --source src/server/migrations/ --database-url "postgres:///crdb-test?host=/run/postgresql"

rebuild-offline-queries: make-test-db
    cargo sqlx prepare --database-url "postgres:///crdb-test?host=/run/postgresql" -- --all-features --tests
    dropdb crdb-test

test-crate NAME='':
    SQLX_OFFLINE="true" cargo nextest run --all-features {{NAME}}

test-crate-no-pg NAME='':
    SQLX_OFFLINE="true" cargo nextest run --all-features -E 'all() - test(server::postgres_db::)' {{NAME}}

test-example-basic NAME='': build-example-basic-client (test-example-basic-host NAME)

build-example-basic-client:
    cd examples/basic && CARGO_TARGET_DIR="target/wasm" RUSTFLAGS="-Zmacro-backtrace" cargo build --target wasm32-unknown-unknown -p client

test-example-basic-host NAME='':
    cd examples/basic && CARGO_TARGET_DIR="target/host" RUSTFLAGS="-Zmacro-backtrace" cargo nextest run -p api -p server {{NAME}}

fuzz-object-cache:
    cargo bolero test --all-features \
        -j 8 \
        cache::object_cache::tests::cache_state_stays_valid \
        --corpus-dir src/cache/object_cache/__fuzz__/cache__object_cache__tests__cache_state_stays_valid/corpus.nounit

fuzz-pg-basic:
    cargo bolero test --all-features \
        -j 8 \
        server::postgres_db::tests::fuzz_simple::db_keeps_invariants \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_simple__db_keeps_invariants/corpus.nounit

fuzz-pg-perms:
    cargo bolero test --all-features \
        -j 8 \
        server::postgres_db::tests::fuzz_remote_perms::fuzz \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_remote_perms__fuzz/corpus.nounit

fuzz-pg-threads:
    cargo bolero test --all-features \
        -j 8 \
        server::postgres_db::tests::fuzz_two_threads::fuzz_no_lock_check \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_two_threads__fuzz_no_lock_check/corpus.nounit

fuzz-pg-locks:
    cargo bolero test --all-features \
        -j 8 \
        server::postgres_db::tests::fuzz_two_threads::fuzz_checking_locks \
        --corpus-dir src/server/postgres_db/tests/__fuzz__/server__postgres_db__tests__fuzz_two_threads__fuzz_checking_locks/corpus.nounit
