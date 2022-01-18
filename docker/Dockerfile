# base dependencies
FROM archlinux:base AS aisdb
RUN pacman -Syyuu --noconfirm \
  && pacman -S --noconfirm \
    nodejs \
    npm \
    openssh \
    python \
    python-sphinx \
    python-sphinx_rtd_theme \
    qgis \
    rust \
    xorg-xauth
RUN useradd -m "ais_env" --shell /bin/python
WORKDIR "/home/ais_env"
USER "ais_env"
RUN npm install express
RUN mkdir -p aisdb_rust/src \
  && echo 'fn main(){}' > aisdb_rust/src/db.rs \
  && echo 'fn main(){}' > aisdb_rust/src/decode.rs \
  && echo 'fn main(){}' > aisdb_rust/src/util.rs \
  && echo 'fn main(){}' > aisdb_rust/src/bin.rs
RUN python -m ensurepip \
  && python -m pip install --no-warn-script-location --upgrade \
    numpy \
    pip \
    pytest \
    wheel
COPY --chown="ais_env" aisdb_rust/Cargo.toml aisdb_rust/Cargo.toml
COPY --chown="ais_env" ./setup.py "/home/ais_env/setup.py"
RUN cargo build --manifest-path=/home/"ais_env"/aisdb_rust/Cargo.toml
RUN mkdir -p aisdb aisdb/webdata aisdb/database aisdb_rust \
  && python -m pip install --no-warn-script-location .  


# build rust target
FROM aisdb as build_rust
COPY --chown="ais_env" aisdb_rust/ aisdb_rust/
RUN cargo build --manifest-path=/home/"ais_env"/aisdb_rust/Cargo.toml 

# rust tests
FROM build_rust AS test_rust
CMD ["cargo", "test", "--manifest-path=aisdb_rust/Cargo.toml", "--color=always"]


# build python target
FROM aisdb as build_python
COPY --chown="ais_env" aisdb_rust/ aisdb_rust/
COPY --chown="ais_env" aisdb/ aisdb/ 
RUN python -m pip install --no-warn-script-location .  


# python tests
FROM build_python as test_python
COPY --chown="ais_env" tests/ tests/
# disable tests that require X11 or web data sources
CMD ["python", "-m", "pytest", "--color=yes", "--tb=native", "-k", "not qgis and not merge", "tests/" ]


# sphinx docs + rust cargo docs
FROM build_python AS webserv
COPY --chown=ais_env nodejs/ nodejs/
COPY --chown=ais_env readme.rst ./readme.rst
COPY --chown=ais_env docs/ docs/
RUN /bin/bash ./docs/sphinxbuild.sh
CMD ["npm", "--prefix", "nodejs/", "start"]


# forward X11 on sshd host
FROM build_python AS sshhost
USER root
COPY --chown="ais_env" docker/entry.py ./entry.py
ENTRYPOINT ["python", "./entry.py"]

