AISDB: Readme
=============

.. raw:: html

    <a href="https://git-dev.cs.dal.ca/meridian/aisdb/-/commits/master">
      <img alt="pipeline status" src="https://git-dev.cs.dal.ca/meridian/aisdb/badges/master/pipeline.svg" />
    </a>

    <!--
    <a href="https://aisdb.meridian.cs.dal.ca/">
      <img alt="website" src="https://img.shields.io/gitlab/pipeline-status/meridian/aisdb?branch=master&gitlab_url=https%3A%2F%2Fgit-dev.cs.dal.ca&label=build-website"/>
    </a>
    -->

    <a href="https://aisdb.meridian.cs.dal.ca/coverage/">
      <img alt="coverage" src="https://img.shields.io/gitlab/coverage/meridian/aisdb/master?gitlab_url=https%3A%2F%2Fgit-dev.cs.dal.ca&job_name=python-test"/>
    </a>

    <a href="https://git-dev.cs.dal.ca/meridian/aisdb/-/releases">
      <img alt="release" src="https://img.shields.io/gitlab/v/release/meridian/aisdb?gitlab_url=https%3A%2F%2Fgit-dev.cs.dal.ca&include_prereleases&sort=semver"/>
    </a>

.. description:

Description
-----------

Package features:
  + SQL database for storing AIS position reports and vessel metadata
  + Vessel position cleaning and trajectory modeling
  + Utilities for streaming and decoding AIS data in the NMEA binary string format (See `Base Station Deployment <AIS_base_station.html>`__)
  + Integration with external datasources including depth charts, distances from shore, vessel geometry, etc.
  + Network graph analysis, MMSI deduplication, interpolation, and other processing utilities
  + Data visualization



.. raw:: html 

   <a href='_images/scriptoutput.png'>
      <img 
        src='https://aisdb.meridian.cs.dal.ca/doc/_images/scriptoutput.png' 
        width="800"
        onerror="this.html='';"
      ></img>
   </a>


| Web Interface:
  https://aisdb.meridian.cs.dal.ca/
| Docs:
  https://aisdb.meridian.cs.dal.ca/doc/readme.html
| Source Code: 
  https://gitlab.meridian.cs.dal.ca/public_projects/aisdb


.. whatisais:

What is AIS?
------------

| Wikipedia:
  https://en.wikipedia.org/wiki/Automatic_identification_system
| Description of message types:
  https://arundaleais.github.io/docs/ais/ais_message_types.html

.. install:

Install Prerequisite
--------------------

Python version 3.8 or higher (tested on version 3.10).
Requires SQLite version 3.8.2 or higher.


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


Read more about the docker services for this package in ``docker-compose.yml`` and :ref:`AISDB docker services <docker>`


Code examples
-------------

1. `Parsing raw format messages into a
   database <./api/aisdb.database.decoder.html#aisdb.database.decoder.decode_msgs>`__

2. `Automatically generate SQL database
   queries <./api/aisdb.database.dbqry.html#aisdb.database.dbqry.DBQuery>`__

3. `Compute trajectories from database rows <./api/aisdb.track_gen.html#aisdb.track_gen.TrackGen>`__

4. `Vessel trajectory cleaning and MMSI deduplication <./api/aisdb.track_gen.html#aisdb.track_gen.encode_greatcircledistance>`__

5. `Compute network graph of vessel movements between
   polygons <./api/aisdb.network_graph.html#aisdb.network_graph.graph>`__

6. Integrating data from web sources, such as depth charts, shore distance, etc.

Collecting AIS Data
-------------------

1. `Setting up an AIS radio station, and exchanging data with other
   networks <docs/AIS_base_station.md>`__
