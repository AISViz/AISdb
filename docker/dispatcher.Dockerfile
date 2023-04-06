FROM rust:slim
RUN apt-get update -y && apt-get upgrade -y

ARG CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
COPY dispatcher/ dispatcher/
RUN cargo install --path dispatcher/client
RUN cargo install --path dispatcher/server
RUN cargo install --path dispatcher/proxy
RUN cargo install --path dispatcher/reverse_proxy
