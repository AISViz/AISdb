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
WORKDIR "/home/ais_env"
USER "ais_env"
RUN rustup install stable && rustup default stable
RUN python -m venv env
ENV PATH=./env/bin:$PATH
RUN python -m pip install maturin
COPY --chown=ais_env aisdb_web/package.json aisdb_web/package.json
RUN mkdir -p aisdb \
  && cd aisdb_web \
  && npm install
COPY --chown="ais_env" .coveragerc .coveragerc
COPY --chown=ais_env aisdb/aisdb_sql aisdb/aisdb_sql
COPY --chown=ais_env Cargo.toml pyproject.toml build.rs readme.rst ./
# pre-cache build dependencies before compiling source
RUN mkdir src \
  && touch aisdb/__init__.py \
  && echo 'fn main(){}' > src/bin.rs \
  && echo 'fn main(){}' > src/lib.rs \
  && cargo check --release
COPY --chown=ais_env src/ src/
RUN source ./env/bin/activate && maturin develop --release --extras=test,docs
COPY --chown=ais_env examples/ examples/
COPY --chown=ais_env aisdb_web/ aisdb_web/
COPY --chown=ais_env aisdb/ aisdb/

# build sphinx docs and nodejs webapp
FROM aisdb AS webserv
ARG AISDBHOST
ARG AISDBPORT
ARG VITE_BINGMAPTILES
ARG VITE_TILESERVER
RUN /bin/bash ./aisdb_web/build_website.sh

FROM aisdb AS docserv
COPY --chown=ais_env aisdb_web/build_docs.sh aisdb_web/build_docs.sh
COPY --chown=ais_env aisdb_web/server_module.js aisdb_web/server_module.js
COPY --chown=ais_env aisdb_web/docserver.js aisdb_web/docserver.js
COPY --chown=ais_env docs/ docs/
COPY --chown=ais_env aisdb_web/dist_coverage aisdb_web/dist_coverage
#COPY --chown=ais_env aisdb_web/package.json aisdb_web/package.json
RUN /bin/bash ./aisdb_web/build_docs.sh
