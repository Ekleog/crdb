# export RUST_BACKTRACE := "short"

export RUSTC_BOOTSTRAP := "1"
export RUSTFLAGS := "-Z macro-backtrace"
export SQLX_OFFLINE := "true"

flags_for_js := "RUSTFLAGS='-Z macro-backtrace --cfg getrandom_backend=\"wasm_js\"'"

all *ARGS: fmt (test ARGS) clippy udeps doc

run-example-basic-server *ARGS:
    createdb basic-crdb || true
    sqlx migrate run \
        --source crdb-postgres/migrations \
        --database-url "postgres:///basic-crdb"
    CARGO_TARGET_DIR="target/host" \
    watchexec -r -e rs,toml \
        -E RUST_LOG="trace,tokio_tungstenite=debug,tungstenite=debug" \
        --workdir examples/basic \
        cargo run -p basic-server -- {{ARGS}}

serve-example-basic-client-js *ARGS:
    CARGO_TARGET_DIR="../target/wasm" \
    watchexec -r -e rs,toml,html \
        --workdir examples/basic/client-js \
        trunk serve

fmt:
    cargo fmt
    cd examples/basic && cargo fmt

# TODO(test-highest): re-introduce test-example-basic test, temporarily put aside for API iteration
test *ARGS: (test-crate ARGS)

test-standalone *ARGS: (test-crate-native "--exclude" "crdb-postgres" ARGS) (test-example-basic ARGS)

clippy:
    CARGO_TARGET_DIR="target/clippy" \
    cargo clippy --all-features -- -D warnings

udeps:
    CARGO_TARGET_DIR="target/udeps" \
    cargo udeps --workspace --all-features
    CARGO_TARGET_DIR="target/udeps" \
    {{flags_for_js}} \
    cargo udeps --workspace \
        --exclude crdb-postgres \
        --exclude crdb-server \
        --exclude crdb-sqlite \
        --target wasm32-unknown-unknown

udeps-full:
    CARGO_TARGET_DIR="target/udeps" \
    cargo hack udeps --each-feature

    CARGO_TARGET_DIR="target/udeps" \
    cargo hack udeps --tests --each-feature

    CARGO_TARGET_DIR="target/udeps" \
    {{flags_for_js}} \
    cargo hack udeps --each-feature \
        --workspace \
        --exclude crdb-postgres \
        --exclude crdb-server \
        --exclude crdb-sqlite \
        --exclude-features server,sqlx-postgres,sqlx-sqlite,_tests \
        --target wasm32-unknown-unknown

    CARGO_TARGET_DIR="target/udeps" \
    {{flags_for_js}} \
    cargo hack udeps --tests --each-feature \
        --workspace \
        --exclude crdb-postgres \
        --exclude crdb-server \
        --exclude crdb-sqlite \
        --exclude-features server,sqlx-postgres,sqlx-sqlite \
        --target wasm32-unknown-unknown

doc:
    CARGO_TARGET_DIR="target/doc" \
    cargo doc --all-features --workspace

make-test-db:
    dropdb crdb-test || true
    createdb crdb-test
    sqlx migrate run \
        --source crates/crdb-postgres/migrations \
        --database-url "postgres:///crdb-test"

rebuild-offline-queries: make-test-db
    cd crates/crdb-postgres && \
        cargo sqlx prepare \
            --database-url "postgres:///crdb-test" -- \
            --all-features --tests

list-todo-types:
    rg 'TODO\(' | grep -v Justfile | sed 's/^.*TODO(//;s/).*$//' | sort | uniq -c || true
    rg 'TODO[^(]' | grep -v Justfile || true

clean:
    pkill postgres || true
    rm -rf /tmp/crdb-test-pg-* || true
    rm -rf /tmp/.org.chromium.Chromium.* || true

test-crate *ARGS: (test-crate-native ARGS) (test-crate-wasm32 ARGS)

test-crate-native *ARGS:
    CARGO_TARGET_DIR="target/test-native" \
    cargo nextest run --workspace --all-features {{ARGS}}

test-crate-wasm32 *ARGS:
    {{flags_for_js}} \
    CARGO_TARGET_DIR="target/test-wasm" \
    cargo test -p crdb-indexed-db \
        --features _tests \
        --target wasm32-unknown-unknown \
        {{ARGS}}

test-example-basic *ARGS: build-example-basic-client (test-example-basic-host ARGS)

build-example-basic-client:
    cd examples/basic && \
        CARGO_TARGET_DIR="target/wasm" \
        {{flags_for_js}} \
        cargo build \
            -p basic-client-js \
            --target wasm32-unknown-unknown

test-example-basic-host *ARGS:
    cd examples/basic && \
        CARGO_TARGET_DIR="target/host" \
        cargo nextest run \
            --workspace \
            --exclude basic-client-js \
            --no-tests=pass \
            {{ARGS}}

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
    {{flags_for_js}} \
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
    {{flags_for_js}} \
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
    {{flags_for_js}} \
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
