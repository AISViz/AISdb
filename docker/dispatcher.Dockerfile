FROM rust:slim
RUN apt-get update -y && apt-get upgrade -y

ARG CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
RUN cargo install mproxy-client
RUN cargo install mproxy-server
RUN cargo install mproxy-forward
RUN cargo install mproxy-reverse
