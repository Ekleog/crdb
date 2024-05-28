# export RUST_BACKTRACE := "short"

macro_backtrace := RUSTC_BOOTSTRAP=1 RUSTFLAGS="-Zmacro-backtrace"

all *ARGS: fmt (test ARGS) clippy udeps doc

run-example-basic-server *ARGS:
    createdb basic-crdb || true
    sqlx migrate run --source crdb-postgres/migrations --database-url "postgres:///basic-crdb"
    cd examples/basic && CARGO_TARGET_DIR="target/host" $macro_backtrace RUST_LOG="trace,tokio_tungstenite=debug,tungstenite=debug" cargo run -p basic-server -- {{ARGS}}

serve-example-basic-client-js *ARGS:
    cd examples/basic/client-js && CARGO_TARGET_DIR="../target/wasm" $macro_backtrace trunk serve

fmt:
    cargo fmt
    cd examples/basic && cargo fmt

test *ARGS: (test-crate ARGS) (test-example-basic ARGS)

test-standalone *ARGS: (test-crate-native "--exclude" "crdb-postgres" ARGS) (test-example-basic ARGS)

clippy:
    CARGO_TARGET_DIR="target/clippy" SQLX_OFFLINE="true" cargo clippy --all-features -- -D warnings

udeps:
    RUSTC_BOOTSTRAP=1 CARGO_TARGET_DIR="target/udeps" SQLX_OFFLINE="true" cargo udeps --workspace --all-features
    RUSTC_BOOTSTRAP=1 CARGO_TARGET_DIR="target/udeps" SQLX_OFFLINE="true" cargo udeps --workspace --exclude crdb-postgres --exclude crdb-server --exclude crdb-sqlite --target wasm32-unknown-unknown

udeps-full:
    RUSTC_BOOTSTRAP=1 CARGO_TARGET_DIR="target/udeps" SQLX_OFFLINE="true" cargo hack udeps --each-feature
    RUSTC_BOOTSTRAP=1 CARGO_TARGET_DIR="target/udeps" SQLX_OFFLINE="true" cargo hack udeps --tests --each-feature
    RUSTC_BOOTSTRAP=1 CARGO_TARGET_DIR="target/udeps" SQLX_OFFLINE="true" cargo udeps --workspace --exclude crdb-postgres --exclude crdb-server --exclude crdb-sqlite --target wasm32-unknown-unknown

doc:
    CARGO_TARGET_DIR="target/doc" SQLX_OFFLINE="true" cargo doc --all-features --workspace

make-test-db:
    dropdb crdb-test || true
    createdb crdb-test
    sqlx migrate run --source crdb-postgres/migrations --database-url "postgres:///crdb-test"

rebuild-offline-queries: make-test-db
    cargo sqlx prepare --database-url "postgres:///crdb-test" -- --all-features --tests

list-todo-types:
    rg 'TODO\(' | grep -v Justfile | sed 's/^.*TODO(//;s/).*$//' | sort | uniq -c || true
    rg 'TODO[^(]' | grep -v Justfile || true

clean:
    pkill postgres || true
    rm -rf /tmp/crdb-test-pg-* || true
    rm -rf /tmp/.org.chromium.Chromium.* || true

test-crate *ARGS: (test-crate-native ARGS) (test-crate-wasm32 ARGS)

test-crate-native *ARGS:
    SQLX_OFFLINE="true" cargo nextest run --workspace --all-features {{ARGS}}

test-crate-wasm32 *ARGS:
    cargo test -p crdb-indexed-db --features _tests --target wasm32-unknown-unknown {{ARGS}}

test-example-basic *ARGS: build-example-basic-client (test-example-basic-host ARGS)

build-example-basic-client:
    cd examples/basic && CARGO_TARGET_DIR="target/wasm" $macro_backtrace cargo build --target wasm32-unknown-unknown -p basic-client-js

test-example-basic-host *ARGS:
    cd examples/basic && CARGO_TARGET_DIR="target/host" $macro_backtrace cargo nextest run -p basic-api -p basic-server -p basic-client-native {{ARGS}}

fuzz-pg-simple ARGS='':
    cargo bolero test \
        --all-features \
        -p crdb-postgres \
        tests::fuzz_simple::fuzz \
        {{ARGS}}

fuzz-idb-simple ARGS='':
    # TODO(blocked): remove path override, when https://github.com/rustwasm/wasm-bindgen/pull/3800 lands?
    PATH="../wasm-bindgen/target/debug:$PATH" \
    WASM_BINDGEN_TEST_TIMEOUT=86400 \
    cargo test \
        -p crdb-indexed-db \
        --target wasm32-unknown-unknown \
        --features _tests \
        client_js::fuzz_simple::fuzz \
        --profile fuzz \
        {{ARGS}} \
        -- --include-ignored

fuzz-pg-perms ARGS='':
    cargo bolero test \
        --all-features \
        -p crdb-postgres \
        tests::fuzz_remote_perms::fuzz \
        {{ARGS}}

fuzz-idb-perms ARGS='':
    # TODO(blocked): remove path override, when https://github.com/rustwasm/wasm-bindgen/pull/3800 lands?
    PATH="../wasm-bindgen/target/debug:$PATH" \
    WASM_BINDGEN_TEST_TIMEOUT=86400 \
    cargo test \
        -p crdb-indexed-db \
        --target wasm32-unknown-unknown \
        --features _tests \
        client_js::fuzz_remote_perms::fuzz \
        --profile fuzz \
        {{ARGS}} \
        -- --include-ignored

fuzz-pg-threads ARGS='':
    cargo bolero test \
        --all-features \
        -p crdb-postgres \
        tests::fuzz_two_threads::fuzz_no_lock_check \
        {{ARGS}}

fuzz-pg-locks ARGS='':
    cargo bolero test \
        --all-features \
        -p crdb-postgres \
        tests::fuzz_two_threads::fuzz_checking_locks \
        {{ARGS}}

fuzz-pg-full ARGS='':
    cargo bolero test \
        --all-features \
        -p crdb-postgres \
        tests::fuzz_object_full::fuzz \
        {{ARGS}}

fuzz-idb-full ARGS='':
    # TODO(blocked): remove path override, when https://github.com/rustwasm/wasm-bindgen/pull/3800 lands?
    PATH="../wasm-bindgen/target/debug:$PATH" \
    WASM_BINDGEN_TEST_TIMEOUT=86400 \
    cargo test \
        -p crdb-indexed-db \
        --target wasm32-unknown-unknown \
        --features _tests \
        client_js::fuzz_object_full::fuzz \
        --profile fuzz \
        {{ARGS}} \
        -- --include-ignored

fuzz-pg-threads-royale ARGS='':
    cargo bolero test \
        --all-features \
        -p crdb-postgres \
        tests::fuzz_battle_royale::fuzz_no_lock_check \
        {{ARGS}}

fuzz-pg-locks-royale ARGS='':
    cargo bolero test \
        --all-features \
        -p crdb-postgres \
        tests::fuzz_battle_royale::fuzz_checking_locks \
        {{ARGS}}

fuzz-pg-sessions ARGS='':
    cargo bolero test \
        --all-features \
        -p crdb-postgres \
        tests::fuzz_sessions::fuzz \
        {{ARGS}}

fuzz-fts-normalizer ARGS='':
    cargo bolero test \
        --all-features \
        -p crdb-core \
        fts::tests::fuzz_normalizer \
        {{ARGS}}
