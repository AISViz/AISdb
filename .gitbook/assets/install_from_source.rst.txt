
.. install_source:

Installing from Source
----------------------

The `maturin build system <https://maturin.rs/develop.html>`__ can be used to compile dependencies and install AISDB. 
Conda users may need to `install maturin from conda-forge <https://maturin.rs/installation.html#conda>`__.

.. code-block:: sh

  # installing the rust toolchain may be required
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  # Windows users can instead download the installer:
  # https://forge.rust-lang.org/infra/other-installation-methods.html#rustup
  # https://static.rust-lang.org/rustup/dist/i686-pc-windows-gnu/rustup-init.exe

  # create a virtual python environment and install maturin
  python -m venv env_ais
  source ./env_ais/bin/activate
  python -m pip install --upgrade maturin

  # clone source and navigate to the package root
  git clone http://git-dev.cs.dal.ca/meridian/aisdb.git
  cd aisdb

  # install AISDB
  maturin develop --release --extras=test,docs


Also see ``maturin build`` for compiling package wheels instead of a local installation.


Read more about the docker services for this package in ``docker-compose.yml`` and `AISDB docker services <docker>`__.
