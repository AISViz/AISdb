# base dependencies
FROM archlinux:base AS aisdb
RUN pacman -Syyuu --noconfirm \
  && pacman -S --noconfirm \
    nodejs \
    npm \
    python \
    python-sphinx \
    python-sphinx_rtd_theme \
    rust 
RUN useradd -m "ais_env"
WORKDIR "/home/ais_env"
USER "ais_env"
RUN mkdir -p aisdb aisdb_rust/src nodejs \
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
RUN cargo build --manifest-path=/home/ais_env/aisdb_rust/Cargo.toml \
  && cargo build --manifest-path=/home/ais_env/aisdb_rust/Cargo.toml --release
RUN mkdir -p aisdb aisdb/webdata aisdb/database aisdb_rust aisdb_sql \
  && python -m pip install --no-warn-script-location --verbose .  
COPY --chown=ais_env nodejs/package.json nodejs/package.json
RUN cd nodejs && npm install


# build rust target and run tests
FROM aisdb as test_rust
COPY --chown="ais_env" aisdb_sql/ aisdb_sql/
COPY --chown="ais_env" aisdb_rust/ aisdb_rust/
RUN cargo build --manifest-path=/home/"ais_env"/aisdb_rust/Cargo.toml 
CMD ["cargo", "test", "--manifest-path=aisdb_rust/Cargo.toml", "--color=always"]


# build python target
FROM aisdb as build_python
COPY --chown="ais_env" aisdb_sql/ aisdb_sql/
COPY --chown="ais_env" aisdb_rust/ aisdb_rust/
COPY --chown="ais_env" aisdb/ aisdb/ 
RUN python -m pip install --no-warn-script-location --verbose .


# python tests
FROM build_python as test_python
COPY --chown="ais_env" tests/ tests/
CMD ["python", "-m", "pytest", "--color=yes", "--tb=native", "-k", "not merge", "tests/" ]


# build webpages
FROM build_python AS webserv
ARG BINGMAPSKEY
COPY --chown=ais_env readme.rst ./readme.rst
COPY --chown=ais_env docs/ docs/
COPY --chown=ais_env changelog.rst ./changelog.rst
RUN /bin/bash ./docs/sphinxbuild.sh
COPY --chown=ais_env nodejs/ nodejs/
RUN cd nodejs \
  && VITE_BINGMAPSKEY=$BINGMAPSKEY npx vite build --outDir ../dist
  #&& npm install \
CMD ["npm", "--prefix", "nodejs/", "start"]

