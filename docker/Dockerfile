# base dependencies
FROM archlinux:base AS aisdb
RUN pacman -Syyuu --noconfirm \
  && pacman -S --noconfirm \
    binaryen \
    firefox \
    gcc \
    git \
    glibc \
    nodejs \
    npm \
    python \
    release-cli \
    rustup \
    wasm-pack \
    zip
RUN useradd -m "ais_env"
USER "ais_env"
WORKDIR "/home/ais_env"

ENV PATH=./env/bin:/home/ais_env/.cargo/bin:$PATH
ENV VIRTUAL_ENV="/home/ais_env/env"
ENV CARGO_UNSTABLE_SPARSE_REGISTRY=true
RUN rustup install nightly && rustup default nightly 
RUN python -m venv env
RUN pip install --upgrade pip wheel maturin setuptools

# cache build deps
COPY --chown=ais_env Cargo.toml pyproject.toml readme.rst .coveragerc ./
COPY --chown=ais_env aisdb_lib/Cargo.toml aisdb_lib/Cargo.toml
COPY --chown=ais_env aisdb_lib/build.rs aisdb_lib/build.rs
COPY --chown=ais_env receiver/Cargo.toml receiver/Cargo.toml
COPY --chown=ais_env dispatcher/ dispatcher/
RUN mkdir -p src aisdb aisdb_lib/src receiver/src \
  && echo 'fn main(){}' | tee src/lib.rs receiver/src/lib.rs aisdb_lib/src/lib.rs > /dev/null
RUN cargo fetch
RUN echo '' > aisdb/__init__.py && python -m pip install --upgrade pip wheel maturin setuptools .[test,docs]

# copy src
#RUN rm -rf aisdb src receiver dispatcher
#COPY --chown=ais_env aisdb/aisdb_sql aisdb/aisdb_sql
#COPY --chown=ais_env dispatcher/ dispatcher/
COPY --chown=ais_env aisdb_lib/ aisdb_lib/
COPY --chown=ais_env receiver/ receiver/
COPY --chown=ais_env examples/ examples/
COPY --chown=ais_env aisdb/ aisdb/
COPY --chown=ais_env docs/ docs/
COPY --chown=ais_env src/ src/

RUN cargo install --path dispatcher/proxy --debug
RUN cargo install --path dispatcher/reverse_proxy --debug
RUN cargo install --path receiver
RUN maturin develop --release --extras=test,docs
RUN cd docs && npm install
RUN /bin/bash ./docs/build_docs.sh

# build sphinx docs including build arguments, start nodejs webapp
FROM aisdb AS webserv
ARG AISDBHOST
ARG AISDBPORT
ARG VITE_BINGMAPTILES
ARG VITE_TILESERVER
COPY --chown=ais_env aisdb_web/ aisdb_web/
RUN cd aisdb_web && npm install
RUN /bin/bash ./aisdb_web/build_website.sh
CMD npm --prefix aisdb_web start
