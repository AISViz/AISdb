# base dependencies
FROM archlinux:base AS aisdb
RUN pacman -Syyuu --noconfirm \
  && pacman -S --noconfirm \
    nodejs \
    npm \
    python \
    python-sphinx \
    python-sphinx_rtd_theme \
    rust \
    wasm-pack
RUN useradd -m "ais_env"
WORKDIR "/home/ais_env"
USER "ais_env"
RUN mkdir -p aisdb aisdb_rust/src aisdb_web \
  && echo 'fn main(){}' > aisdb_rust/src/db.rs \
  && echo 'fn main(){}' > aisdb_rust/src/decode.rs \
  && echo 'fn main(){}' > aisdb_rust/src/util.rs \
  && echo 'fn main(){}' > aisdb_rust/src/bin.rs \
  && echo '__version__ = "0.0.0"' > aisdb/version.py
RUN python -m ensurepip \
  && python -m pip install --no-warn-script-location --upgrade \
    numpy \
    pip \
    pytest \
    wheel
COPY --chown="ais_env" aisdb_rust/Cargo.toml aisdb_rust/Cargo.toml
COPY --chown="ais_env" ./setup.py "/home/ais_env/setup.py"
COPY --chown="ais_env" ./MANIFEST.in /home/ais_env/MANIFEST.in
RUN cargo build --manifest-path=/home/ais_env/aisdb_rust/Cargo.toml \
  && cargo build --manifest-path=/home/ais_env/aisdb_rust/Cargo.toml --release
RUN mkdir -p aisdb aisdb/webdata aisdb/database aisdb_rust aisdb_sql \
  && python -m pip install --no-warn-script-location --verbose .  
COPY --chown=ais_env aisdb_web/package.json aisdb_web/package.json
RUN cd aisdb_web && npm install


# build rust target
FROM aisdb as build_rust
COPY --chown="ais_env" aisdb_sql/ aisdb_sql/
COPY --chown="ais_env" aisdb_rust/ aisdb_rust/
RUN cargo build --manifest-path=/home/"ais_env"/aisdb_rust/Cargo.toml 


# build python target
FROM build_rust as build_python
COPY --chown="ais_env" tests/ tests/
COPY --chown="ais_env" aisdb/ aisdb/ 
RUN python -m pip install --no-warn-script-location --verbose .


# build sphinx docs and nodejs webapp
FROM build_python AS webserv
ARG BINGMAPSKEY
ARG AISDBHOST
ARG AISDBPORT
COPY --chown=ais_env readme.rst ./readme.rst
COPY --chown=ais_env docs/ docs/
COPY --chown=ais_env changelog.rst ./changelog.rst
COPY --chown=ais_env sphinxbuild.sh ./sphinxbuild.sh
RUN /bin/bash ./sphinxbuild.sh
COPY --chown=ais_env aisdb_web/ aisdb_web/
RUN cd aisdb_web \
  && VITE_BINGMAPSKEY=$BINGMAPSKEY \
     VITE_AISDBHOST=$AISDBHOST \
     VITE_AISDBPORT=$AISDBPORT \
     npx vite build --outDir ../dist
