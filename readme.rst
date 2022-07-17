AISDB: Readme
=============

.. raw:: html

    <a href="https://gitlab.meridian.cs.dal.ca/public_projects/aisdb/-/commits/master"><img alt="pipeline status" src="https://gitlab.meridian.cs.dal.ca/public_projects/aisdb/badges/master/pipeline.svg" /></a>


.. description:

Description
-----------

Package features:
  + SQL database for storing AIS position reports and vessel metadata
  + Vessel position cleaning and trajectory modeling
  + Utilities for streaming and decoding AIS data in the NMEA binary string format (See `Base Station Deployment <AIS_base_station.html>`__)
  + Integration with public datasources including depth charts, distances from shore, vessel geometry, etc.
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



Installing from PyPI
--------------------

TODO: upload package wheels to PyPI


Installing from Source
----------------------

Clone the repository and create a virtual python environment to install the package. Navigate to the project root folder

.. code-block:: sh

  python -m venv env_ais
  source ./env_ais/bin/activate
  git clone https://gitlab.meridian.cs.dal.ca/public_projects/aisdb.git
  cd aisdb


Python wheel files can be built using the included docker environment. By default, wheels will be built using the manylinux2014_x86_64 target. Results will be output to ``./target/wheels/``. Package wheels can then be installed using pip

.. code-block:: sh

  python -m pip install --upgrade docker-compose maturin
  docker-compose up --build pkgbuild  # may require sudo 
  PYTHON3VERSION="`maturin list-python | tail -n +2 | sort -g | head -n1 | egrep -o 'python3.*' | cut -d'.' -f2`"
  WHEELFILE="`ls ./target/wheels/aisdb-*cp3$PYTHON3VERSION*.whl -r1 | head -n1`"
  python -m pip install ${WHEELFILE}


Alternatively, for an editable installation, rust targets can be compiled to shared object (.so) format using `maturin build system<https://maturin.rs/develop.html>`_. Conda users may need to `install maturin from conda-forge<https://maturin.rs/installation.html#conda>`

.. code-block:: sh

  python -m pip install maturin
  maturin develop --release


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
