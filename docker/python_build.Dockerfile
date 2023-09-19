FROM ghcr.io/pyo3/maturin:v1.0.0 AS aisdb-manylinux

# Updates
RUN rm /var/cache/yum/*/7/timedhosts.txt
RUN ulimit -n 1024000 && yum update -y && yum upgrade -y
RUN ulimit -n 1024000 && yum install -y postgresql-libs python-sphinx perl-IPC-Cmd

RUN rustup update

WORKDIR /aisdb_src
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL="sparse"
ENV VIRTUAL_ENV="/env_aisdb"
ARG CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

COPY Cargo.toml Cargo.lock .coveragerc pyproject.toml readme.rst ./
COPY aisdb_lib/ aisdb_lib/

# cache deps
COPY aisdb/aisdb_sql aisdb/aisdb_sql
COPY receiver/Cargo.toml receiver/Cargo.lock receiver/
RUN mkdir -p src receiver/src aisdb \
  && echo 'fn main(){}' > src/lib.rs \
  && echo 'fn main(){}' > receiver/src/lib.rs \
  && touch aisdb/__init__.py
RUN python3.9 -m venv $VIRTUAL_ENV
RUN $VIRTUAL_ENV/bin/python -m pip install --upgrade --verbose --no-warn-script-location .[test,docs] pip wheel setuptools numpy


COPY receiver/ receiver/
COPY src/ src/
RUN maturin build --release --strip --compatibility manylinux2014 --interpreter 3.11 --locked
COPY aisdb/ aisdb/
COPY examples/  examples/
COPY docs/ docs/

# build manylinux package wheels for distribution
RUN VIRTUAL_ENV=/env_aisdb/ maturin build --release --strip --compatibility manylinux2014 --interpreter 3.9 3.10 3.11 3.12 --locked
#RUN VIRTUAL_ENV=/env_aisdb/ CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse maturin sdist
RUN RUST_BACKTRACE=1 VIRTUAL_ENV=/env_aisdb/ maturin develop --release --extras=test,docs --locked --offline

CMD ["build", "--release", "--strip", "--compatibility", "manylinux2014", "--interpreter", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"]


# copy wheel file from aisdb-manylinux to a fresh python container and install AISDB
FROM python:slim AS aisdb-python
RUN apt-get update -y && apt-get upgrade -y
RUN python -m pip install --upgrade pip packaging Pillow requests selenium tqdm numpy webdriver-manager pytest coverage pytest-cov pytest-dotenv psycopg[binary] orjson websockets
RUN python -m pip install gunicorn
COPY aisdb/tests/testdata/ /aisdb_src/aisdb/tests/testdata/
WORKDIR /aisdb
COPY --from=aisdb-manylinux /aisdb_src/target/wheels/* wheels/
RUN python -m pip install "`ls wheels/aisdb-*-cp311-cp311-manylinux_2_17_*.manylinux2014_*.whl`[test]"
CMD ["python3", "-Iqu"]


# install extras required for tests and sphinx docs
FROM aisdb-manylinux AS aisdb-python-docs
RUN cp readme.rst docs/changelog.rst docs/source/
COPY aisdb_web/map/public/ aisdb_web/map/public/
RUN source /env_aisdb/bin/activate && sphinx-apidoc --separate --force --implicit-namespaces --module-first --no-toc -o docs/source/api aisdb aisdb/tests/*
RUN source /env_aisdb/bin/activate && python -m sphinx -a -j auto -q -b=html docs/source docs/dist_sphinx


# copy sphinx docs to node container
FROM node:slim AS docserver
COPY --from=aisdb-python-docs /aisdb_src/docs docs
RUN cd docs && npm install
CMD ["node", "docs/docserver.js"]
