test: test-crate test-example-basic

test-crate:
    cargo test --all-features

test-example-basic: build-example-basic-client test-example-basic-host

build-example-basic-client:
    cd examples/basic && CARGO_TARGET_DIR="target/wasm" RUSTFLAGS="-Zmacro-backtrace" cargo build --target wasm32-unknown-unknown -p client

test-example-basic-host:
    cd examples/basic && CARGO_TARGET_DIR="target/host" RUSTFLAGS="-Zmacro-backtrace" cargo test -p api -p server

fuzz-object-cache:
    cargo bolero test --all-features \
        -j 8 \
        cache::object_cache::tests::cache_state_stays_valid \
        --corpus-dir src/cache/object_cache/__fuzz__/cache__object_cache__tests__cache_state_stays_valid/corpus.nounit
