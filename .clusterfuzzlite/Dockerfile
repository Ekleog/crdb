FROM gcr.io/oss-fuzz-base/base-builder:v1
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN source "$HOME/.cargo/env" && cargo install cargo-bolero
COPY . $SRC/crdb
WORKDIR $SRC/crdb
COPY ./.clusterfuzzlite/build.sh $SRC/
