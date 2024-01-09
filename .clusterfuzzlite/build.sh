#!/bin/bash
set -euxo pipefail

cargo bolero build-clusterfuzz --all-features
tar -xf target/fuzz/clusterfuzz.tar --directory "$OUT"
