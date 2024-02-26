Readme
======

.. image:: https://github.com/AISViz/AISdb/actions/workflows/CI.yml/badge.svg
   :target: https://github.com/AISViz/AISdb/actions/workflows/CI.yml
   :alt: CI status
.. image:: https://github.com/AISViz/AISdb/actions/workflows/github-code-scanning/codeql/badge.svg
   :target: https://github.com/AISViz/AISdb/actions/workflows/github-code-scanning/codeql
   :alt: CodeQL status
.. image:: https://github.com/AISViz/AISdb/actions/workflows/test-installation.yml/badge.svg
   :target: https://github.com/AISViz/AISdb/actions/workflows/test-installation.yml
   :alt: Test installation status

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


.. image:: https://aisdb.meridian.cs.dal.ca/readme_example.png


| Web Interface:
  https://aisdb.meridian.cs.dal.ca/
| Docs:
  https://aisdb.meridian.cs.dal.ca/doc/readme.html
| Source Code: 
  https://git-dev.cs.dal.ca/meridian/aisdb

.. whatisais:

What is AIS?
------------

| Wikipedia:
  https://en.wikipedia.org/wiki/Automatic_identification_system
| Description of message types:
  https://arundaleais.github.io/docs/ais/ais_message_types.html



Install
-------

.. _install-pip:
  
Requires Python version 3.8 or newer.
Optionally requires SQLite (included in Python) or PostgresQL server (installed separately).
The AISDB Python package can be installed using pip.
It is recommended to install the package in a virtual Python environment such as ``venv``.

.. code-block:: sh

   python -m venv env_ais 
   source ./env_ais/*/activate
   pip install aisdb

.. _install-src:

For information on installing AISDB from source code, see `Installing from Source <https://aisdb.meridian.cs.dal.ca/doc/install_from_source.html>`__

.. _readme-docs:


Documentation
-------------

An introduction to AISDB can be found here: `Introduction <https://aisdb.meridian.cs.dal.ca/doc/intro.html>`__.

Additional API documentation: `API Docs <https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.html>`__.

.. _readme-examples:

Code examples
-------------

1. `Parsing raw format messages into a
   database <https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.database.decoder.html#aisdb.database.decoder.decode_msgs>`__

2. `Automatically generate SQL database
   queries <https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.database.dbqry.html#aisdb.database.dbqry.DBQuery>`__

3. `Compute trajectories from database rows <https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.track_gen.html#aisdb.track_gen.TrackGen>`__

4. `Vessel trajectory cleaning and MMSI deduplication <https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.track_gen.html#aisdb.track_gen.encode_greatcircledistance>`__

5. `Compute network graph of vessel movements between
   polygons <https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.network_graph.html#aisdb.network_graph.graph>`__

6. Integrating data from web sources, such as depth charts, shore distance, etc.

