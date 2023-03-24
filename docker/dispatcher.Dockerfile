FROM rust:1.68-alpine

RUN apk add build-base

COPY dispatcher/ dispatcher/
RUN cargo install --path dispatcher/client
RUN cargo install --path dispatcher/server
RUN cargo install --path dispatcher/proxy
RUN cargo install --path dispatcher/reverse_proxy
