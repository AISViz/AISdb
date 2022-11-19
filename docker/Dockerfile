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
#RUN chown -R ais_env /home/ais_env
USER "ais_env"
WORKDIR "/home/ais_env"
RUN rustup install stable && rustup default stable
RUN python -m venv env
ENV PATH=./env/bin:$PATH VIRTUAL_ENV="/home/ais_env/env"
#COPY --chown="ais_env" .coveragerc .coveragerc
COPY --chown=ais_env aisdb/aisdb_sql aisdb/aisdb_sql
COPY --chown=ais_env Cargo.toml pyproject.toml build.rs readme.rst ./
# pre-cache build dependencies before building
  #&& chown ais_env aisdb \
  #&& touch aisdb/__init__.py \
RUN mkdir src \
  && echo 'fn main(){}' > src/bin.rs \
  && echo 'fn main(){}' > src/lib.rs \
  && cargo check --release
RUN echo '' > aisdb/__init__.py && python -m pip install --upgrade pip wheel maturin setuptools .[test,docs]

COPY --chown=ais_env src/ src/
COPY --chown=ais_env examples/ examples/
COPY --chown=ais_env aisdb/ aisdb/
RUN maturin develop --release

# build sphinx docs and nodejs webapp
FROM aisdb AS webserv
ARG AISDBHOST
ARG AISDBPORT
ARG VITE_BINGMAPTILES
ARG VITE_TILESERVER
COPY --chown=ais_env aisdb_web/ aisdb_web/
RUN cd aisdb_web && npm install
RUN /bin/bash ./aisdb_web/build_website.sh

FROM aisdb AS docserv
COPY --chown=ais_env docs/ docs/
RUN cd docs && npm install
RUN /bin/bash ./docs/build_docs.sh
