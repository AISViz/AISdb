FROM rust:1.68-alpine

RUN apk update && apk upgrade --available
RUN apk add build-base


COPY dispatcher/ dispatcher/
COPY aisdb/aisdb_sql/ aisdb/aisdb_sql/
COPY aisdb_lib/ aisdb_lib/
COPY receiver/Cargo.toml receiver/Cargo.lock receiver/
RUN mkdir -p receiver/src && echo 'fn main(){}' > receiver/src/receiver.rs && cp receiver/src/receiver.rs receiver/src/lib.rs
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo install --path receiver
COPY receiver/ receiver/
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo install --path receiver

