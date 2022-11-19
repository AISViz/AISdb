#FROM rust:alpine3.16
FROM rust:latest

WORKDIR /usr/src/aisdb/receiver

COPY ./Cargo.toml ./build.rs /usr/src/aisdb/
COPY ./src /usr/src/aisdb/src
COPY ./aisdb/aisdb_sql /usr/src/aisdb/aisdb/aisdb_sql
COPY ./dispatcher /usr/src/aisdb/dispatcher
COPY ./aisdb_lib /usr/src/aisdb/aisdb_lib

# pre-cache build deps and crates.io index
COPY ./receiver/Cargo.toml Cargo.toml
RUN mkdir -p src/bin \
  && echo 'fn main(){}' > src/receiver.rs \
  && cargo install --path .

COPY ./receiver .
RUN cargo install --path .
RUN cargo install --path /usr/src/aisdb/dispatcher/proxy
RUN cargo install --path /usr/src/aisdb/dispatcher/reverse_proxy

#RUN touch -a ais_rx.db
#run chmod 777 ais_rx.db

CMD ["aisdb-receiver"]
