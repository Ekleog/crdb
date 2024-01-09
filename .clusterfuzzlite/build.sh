#!/bin/bash
set -euxo pipefail

cd crdb
cargo bolero build-clusterfuzz --all-features
cd $OUT
tar xf $SRC/crdb/target/fuzz/clusterfuzz.tar
