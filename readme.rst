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
        src='_images/scriptoutput.png' 
        width="800"
        onerror="this.html=''; this.width=0px;"
      ></img>
   </a>


| Web Interface:
  https://aisdb.meridian.cs.dal.ca/
| Python Documentation:
  https://aisdb.meridian.cs.dal.ca/doc/readme.html
| Rust Documentation:
  https://aisdb.meridian.cs.dal.ca/rust/doc/aisdb/index.html
| JavaScript Documentation:
  https://aisdb.meridian.cs.dal.ca/js/
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

Installing from PyPI
----------------------

TODO: upload package wheels to PyPI


Installing from Source
----------------------

Build wheel files using the included docker environment. By default, wheels will be built for python versions 3.7, 3.8, 3.9, and 3.10 using the manylinux2014_x86_64 target. Resulting wheel files will be output to ./target/wheels/

.. code-block:: sh

  docker-compose up --build pkgbuild


Package wheels can be installed using pip:

.. code-block:: sh

  python -m pip install aisdb-1.2.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl


Read more about the docker services for this package in :ref:`docker-compose.yml <https://gitlab.meridian.cs.dal.ca/public_projects/aisdb/-/blob/master/docker-compose.yml>` and :ref:`AISDB docker services <docker>`


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
