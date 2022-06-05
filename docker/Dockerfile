# base dependencies
FROM archlinux:base AS aisdb
RUN pacman -Syyuu --noconfirm \
  && pacman -S --noconfirm \
    binaryen \
    gcc \
    firefox \
    nodejs \
    npm \
    python \
    python-sphinx \
    python-sphinx_rtd_theme \
    rustup \
    wasm-pack
RUN useradd -m "ais_env"
WORKDIR "/home/ais_env"
USER "ais_env"
RUN rustup install stable && rustup default stable
RUN python -m ensurepip && python -m pip install --upgrade pip numpy wheel
COPY --chown="ais_env" docs/requirements.txt docs/requirements.txt
RUN python -m pip install -r ./docs/requirements.txt && python -m pip install \
  coverage \
  pytest \
  pytest-asyncio \
  pytest-profiling
COPY --chown=ais_env aisdb_web/package.json aisdb_web/package.json
RUN cd aisdb_web && npm install
COPY --chown="ais_env" ./target/wheels/aisdb-*cp310*.whl ./
RUN python -m pip install `ls -r1 aisdb-*cp310*.whl | head -n1`

# build sphinx docs and nodejs webapp
FROM aisdb AS webserv
ARG BINGMAPSKEY
ARG AISDBHOST
ARG AISDBPORT
COPY --chown=ais_env Cargo.toml pyproject.toml build.rs readme.rst ./
COPY --chown=ais_env aisdb/ aisdb/
COPY --chown=ais_env src/ src/
COPY --chown=ais_env docs/ docs/
COPY --chown=ais_env aisdb_wasm/ aisdb_wasm/
COPY --chown=ais_env aisdb_web/ aisdb_web/
RUN /bin/bash ./aisdb_web/build_website.sh
