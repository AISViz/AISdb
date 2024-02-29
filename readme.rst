.. image:: https://github.com/AISViz/AISdb/actions/workflows/CI.yml/badge.svg
   :target: https://github.com/AISViz/AISdb/actions/workflows/CI.yml
   :alt: CI status
.. image:: https://github.com/AISViz/AISdb/actions/workflows/github-code-scanning/codeql/badge.svg
   :target: https://github.com/AISViz/AISdb/actions/workflows/github-code-scanning/codeql
   :alt: CodeQL status
.. image:: https://github.com/AISViz/AISdb/actions/workflows/Install.yml/badge.svg
   :target: https://github.com/AISViz/AISdb/actions/workflows/Install.yml
   :alt: Test installation status
.. image:: https://img.shields.io/github/license/aisviz/aisdb
    :target: https://img.shields.io/github/license/aisviz/aisdb
    :alt: License Status
.. image:: https://img.shields.io/github/commit-activity/t/aisviz/aisdb
    :target: https://img.shields.io/github/commit-activity/t/aisviz/aisdb
    :alt: Commits in the Repository


.. description:

📍 Description
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

📢 What is AIS Data❓
------------

The Automatic Identification System (AIS) is a tracking system used by ships and vessel traffic services (VTS) to identify and locate vessels by exchanging data with other nearby ships, VTS stations, and satellites. The primary goal of AIS is to enhance maritime safety, navigation, and security by providing real-time information about the vessels' position, course, and other relevant data. The AIS system uses different message types to communicate information between vessels and tracking stations. These messages can include vessel identification, position, course, speed, navigational status, and other safety-related information. The widespread adoption of AIS has significantly improved the ability of ships to avoid collisions and navigate more safely, especially in busy shipping lanes and ports.

For those interested in a more in-depth understanding of AIS and its message types, start with these resources:

- `Wikipedia Article <https://en.wikipedia.org/wiki/Automatic_identification_system>`_: An overview of the Automatic Identification System, including its history, functionality, and applications.

- `AIS Message Types <https://arundaleais.github.io/docs/ais/ais_message_types.html>`_: A guide to the various AIS message types used for communication between ships and tracking systems.

📦 Install
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


📓 Documentation
-------------

An introduction to AISDB can be found here: `Introduction <https://aisdb.meridian.cs.dal.ca/doc/intro.html>`__.

Additional API documentation: `API Docs <https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.html>`__.

.. _readme-examples:

🔮 Code examples
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

