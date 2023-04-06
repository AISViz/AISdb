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

`Vite <https://vitejs.dev/>`__ can be used for local development and deployment bundling. 
JavaScript dependencies can be installed with the Node Package Manager (NPM) included with NodeJS. 
Navigate to the aisdb_web directory, and install the dependencies with NPM. 
Use the build script included in the repository to compile WebAssembly targets, and embed them in JavaScript.
Then start a development instance of the web application interface.
The web application can then be viewed at `<http://localhost:3000>`__.
For configuration, see the ``VITE_*`` environment variables in :ref:`environment`.

.. code-block:: sh

   cd aisdb_web
   npm install
   /bin/bash ./build_website.sh
   npx vite --port 3000 ./map 

Alternatively, configure and start the web application interface with the following Python code after building dependencies

.. include:: ../../examples/start_web_interface.py
   :literal:

