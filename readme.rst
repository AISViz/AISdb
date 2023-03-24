Readme
======

.. image:: https://git-dev.cs.dal.ca/meridian/aisdb/badges/master/pipeline.svg

.. image:: https://img.shields.io/gitlab/coverage/meridian/aisdb/master?gitlab_url=https%3A%2F%2Fgit-dev.cs.dal.ca&job_name=python-test

.. image:: https://img.shields.io/gitlab/v/release/meridian/aisdb?gitlab_url=https%3A%2F%2Fgit-dev.cs.dal.ca&include_prereleases&sort=semver

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


.. install:

Install
-------
Requires Python version 3.8, 3.9, or 3.10.
Requires SQLite (included by default in most versions of Python).

.. code-block:: sh

   python -m venv env_ais && source ./env_ais/*/activate
   pip install aisdb


Installing from Source
----------------------


See `Installing from Source <install_from_source>`__


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

Collecting AIS Data
-------------------

1. `Decode incoming AIS data and log to SQLite database <https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.receiver.html>`__

.. code-block:: python

   from aisdb.receiver import start_receiver

   start_receiver(connect_addr='aisdb.meridian.cs.dal.ca:9920', dbpath='AIS.sqlitedb', stdout=True)

