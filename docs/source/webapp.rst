.. _webapp:

Web Application Development
===========================

The easiest way to install and utilize the AISDB web application components together is using the ``docker-compose.yml`` configuration included in this repository to run them as docker services. 
See the :ref:`docker` documentation for more info on how to do this.
However, in some situations, such as developing or testing AISDB components, it may be convenient to run these services outside of a docker environment. 
This page documents how to run these services outside of docker, so that they can be tested or integrated in an existing environment.

The application has three primary components:

  - Database server (Back end web API)
  - Database storage (Postgres database)
  - Web application interface (JS front end)

And some secondary components:

  - Documentation webserver
  - AIS receiver client
  - AIS livestream proxy dispatcher


Dependencies
------------

The following software is requisite for each AISDB service:

- Database Storage

  - Postgresql Database Server
  - Postgresql Database Client Libraries
  - See the `Postgres Install Tutorial <https://www.postgresql.org/docs/current/tutorial-install.html>`__

- Database Server

  - Rustup, the Rust Compiler Toolchain `Install Rust <https://www.rust-lang.org/tools/install>`__
  - OpenSSL

- Web Application Front End

  - Rustup, the Rust Compiler Toolchain `Install Rust <https://www.rust-lang.org/tools/install>`__
  - Binaryen, the WebAssembly Compiler Toolchain `Binaryen <https://github.com/WebAssembly/binaryen>`__
  - Wasm-pack, the Rust WebAssembly Packaging Utility `Install wasm-pack <https://rustwasm.github.io/wasm-pack/installer/>`__
  - Clang, the C/C++ Compiler `Clang Download <https://releases.llvm.org/download.html>`__
  - OpenSSL Development Toolkit (e.g. ``libssl-dev`` on ubuntu/debian)
  - Pkg-config `pkg-config <https://en.wikipedia.org/wiki/Pkg-config>`__
  - NodeJS, the JavaScript Runtime Environment `Node.js download <https://nodejs.org/en>`__

- Documentation Server

  - Python `Download Python <https://www.python.org/downloads/>`__
  - Rustup, the Rust Compiler Toolchain `Install Rust <https://www.rust-lang.org/tools/install>`__
  - Sphinx Doc `Installing and Running Sphinx <https://www.sphinx-doc.org/en/master/#get-started>`__
  - Maturin Build System `Maturin User Guide <https://www.maturin.rs/>`__
  - NodeJS, the JavaScript Runtime Environment `Node.js download <https://nodejs.org/en>`__

- AIS Receiver Client

  - Rustup, the Rust Compiler Toolchain `Install Rust <https://www.rust-lang.org/tools/install>`__

- AIS Proxy Dispatcher

  - Rustup, the Rust Compiler Toolchain `Install Rust <https://www.rust-lang.org/tools/install>`__


Database Storage
----------------

Ensure that the Postgres server is running by following the `Postgres Database Server Tutorial <https://www.postgresql.org/docs/current/server-start.html>`__.
The other web services will use this server for storage and retrieval of AIS data.


Database Server
---------------

Configure the database server by setting the following environment variables for the postgres database connection:

.. code-block:: sh

  PGPASSFILE=$HOME/.pgpass
  PGUSER="postgres"
  PGHOST="[fc00::9]"
  PGPORT="5432"

Navigate to the ``database_server`` folder in the project repository, install it with cargo, and then run it.

.. code-block:: sh

  cd database_server
  cargo install --path . 
  aisdb-db-server


Web Application Front End
-------------------------

A web interface client is included in the AISDB python package. 

.. include:: ../../examples/visualize.py
   :literal:

