FROM rust:slim
RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install -y openssl 

RUN rustup default nightly

# build application dependencies
COPY aisdb/aisdb_sql/ aisdb/aisdb_sql/
COPY aisdb_lib/ aisdb_lib/
COPY database_server/Cargo.toml database_server/Cargo.lock database_server/
RUN mkdir -p database_server/src \
  && echo 'fn main(){}' > database_server/src/main.rs \
  && echo 'fn main(){}' > database_server/src/lib.rs
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo install --path database_server

# build application
COPY database_server/ database_server/
RUN CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo install --path database_server

CMD aisdb-db-server
