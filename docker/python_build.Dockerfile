FROM ghcr.io/pyo3/maturin:main AS aisdb-manylinux

# Updates
RUN yum update -y && yum upgrade -y
RUN yum install -y glibc postgresql-libs python-sphinx
RUN rustup update

WORKDIR /aisdb_src

COPY Cargo.toml Cargo.lock .coveragerc pyproject.toml readme.rst ./
COPY aisdb_lib/ aisdb_lib/
COPY dispatcher/ dispatcher/

# cache deps
COPY aisdb/aisdb_sql aisdb/aisdb_sql
COPY receiver/Cargo.toml receiver/Cargo.lock receiver/
RUN mkdir -p src receiver/src aisdb \
  && echo 'fn main(){}' > src/lib.rs \
  && echo 'fn main(){}' > receiver/src/lib.rs \
  && touch aisdb/__init__.py
RUN python3.9 -m venv /env_aisdb
RUN /env_aisdb/bin/python -m pip install .[test,docs]


COPY receiver/ receiver/
COPY src/ src/
RUN VIRTUAL_ENV=/env_aisdb/ CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse maturin build --release --strip --compatibility manylinux2014 --interpreter 3.9 3.10 3.11 3.12 --locked
COPY aisdb/ aisdb/
COPY examples/  examples/
COPY docs/ docs/

# build manylinux package wheels for distribution
RUN VIRTUAL_ENV=/env_aisdb/ CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse maturin build --release --strip --compatibility manylinux2014 --interpreter 3.9 3.10 3.11 3.12 --locked --offline
RUN RUST_BACKTRACE=1 CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse VIRTUAL_ENV=/env_aisdb/ maturin develop --release --extras=test,docs --locked --offline


# copy wheel file from aisdb-manylinux to a fresh python container and install AISDB
FROM python:3.11.2-slim AS aisdb-python
RUN apt-get update -y && apt-get upgrade -y
RUN python -m pip install --upgrade pip packaging Pillow requests selenium tqdm numpy webdriver-manager pytest coverage pytest-cov pytest-dotenv psycopg2-binary
WORKDIR /aisdb
COPY --from=aisdb-manylinux /aisdb_src/target/wheels/* wheels/
RUN python -m pip install wheels/aisdb-*-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
CMD ["python3", "-Iqu"]


# install extras required for tests and sphinx docs
FROM aisdb-manylinux AS aisdb-python-test
RUN cp readme.rst docs/changelog.rst docs/source/
COPY aisdb_web/map/public/ aisdb_web/map/public/
RUN source /env_aisdb/bin/activate && sphinx-apidoc --separate --force --implicit-namespaces --module-first --no-toc -o docs/source/api aisdb aisdb/tests/*
RUN source /env_aisdb/bin/activate && python -m sphinx -a -j auto -q -b=html docs/source docs/dist_sphinx
ENTRYPOINT []
CMD [ "/env_aisdb/bin/python" ]


# copy sphinx docs to node container
FROM node:latest AS docserver
COPY --from=aisdb-python-test /aisdb_src/docs docs
RUN cd docs && npm install
CMD ["node", "docs/docserver.js"]
