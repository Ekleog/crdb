#!/bin/bash
set -euxo pipefail

source "$HOME/.cargo/env"
cargo bolero build-clusterfuzz --all-features
tar -xf target/fuzz/clusterfuzz.tar --directory "$OUT"
