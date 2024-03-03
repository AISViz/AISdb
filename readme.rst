.. |space| unicode:: 0xA0

.. general-information:
**General Information**

.. image:: https://img.shields.io/pypi/pyversions/aisdb
   :target: https://pypi.org/project/aisdb/
.. image:: https://img.shields.io/github/v/release/aisviz/aisdb
   :target: https://github.com/AISViz/AISdb/releases
.. image:: https://img.shields.io/github/commits-since/aisviz/aisdb/latest
   :target: https://github.com/AISViz/AISdb
.. image:: https://img.shields.io/github/commit-activity/t/aisviz/aisdb
   :target: https://github.com/AISViz/AISdb
   :alt: Commits in the Repository
.. image:: https://img.shields.io/github/languages/top/aisviz/aisdb
   :target: https://github.com/AISViz/AISdb
.. image:: https://img.shields.io/github/repo-size/aisviz/aisdb
   :target: https://github.com/AISViz/AISdb

.. licensing-integration:
**Licensing and Itegration**

.. image:: https://img.shields.io/github/license/aisviz/aisdb
   :target: https://github.com/AISViz/AISdb
.. image:: https://github.com/AISViz/AISdb/actions/workflows/CI.yml/badge.svg
   :target: https://github.com/AISViz/AISdb/actions/workflows/CI.yml
   :alt: CI status
.. image:: https://github.com/AISViz/AISdb/actions/workflows/github-code-scanning/codeql/badge.svg
   :target: https://github.com/AISViz/AISdb/actions/workflows/github-code-scanning/codeql
   :alt: CodeQL status
.. image:: https://github.com/AISViz/AISdb/actions/workflows/Install.yml/badge.svg
   :target: https://github.com/AISViz/AISdb/actions/workflows/Install.yml
   :alt: Test installation status

.. quick-links:
**Documentation and Tutorials**

.. |aisviz| image:: https://img.shields.io/website?url=https%3A%2F%2Faisviz.github.io
   :target: https://img.shields.io/website?url=https%3A%2F%2Faisviz.github.io
.. |aisdb_doc| image:: https://img.shields.io/website?url=https%3A%2F%2Faisviz.gitbook.io/documentation/
   :target: https://img.shields.io/website?url=https%3A%2F%2Faisviz.gitbook.io/documentation/
.. |aisdb_tut| image:: https://img.shields.io/website?url=https%3A%2F%2Faisviz.gitbook.io/tutorials/
   :target: https://img.shields.io/website?url=https%3A%2F%2Faisviz.gitbook.io/tutorials/
.. |aisdb_rtd| image:: https://img.shields.io/website?url=https%3A%2F%2Faisdb.meridian.cs.dal.ca/doc/readme.html
   :target: https://img.shields.io/website?url=https%3A%2F%2Faisdb.meridian.cs.dal.ca/doc/readme.html

- |aisviz| |space| `AISViz Website <https://aisviz.github.io>`_
- |aisdb_rtd| |space| `AISdb ReadTheDocs <https://aisdb.meridian.cs.dal.ca/doc/readme.html>`_ *(outdated)*
- |aisdb_tut| |space| `AISdb GitBook Tutorials <https://aisviz.gitbook.io/tutorials>`_
- |aisdb_doc| |space| `AISdb GitBook Documentation <https://aisviz.gitbook.io/documentation>`_
****

.. whatisais:
📢 What is AIS Data?
------------------------

The Automatic Identification System (AIS) is a tracking system used by ships and vessel traffic services to identify and locate vessels by exchanging data with other nearby ships, vessel traffic services stations, and satellites. The primary goal of AIS is to enhance maritime safety, navigation, and security by providing real-time information about the vessels' position, course, and other relevant data. The AIS system uses different message types to communicate information between vessels and tracking stations. These messages can include vessel identification, position, course, speed, navigational status, and other safety-related information. The widespread adoption of AIS has significantly improved the ability of ships to avoid collisions and navigate more safely, especially in busy shipping lanes and ports.

For those interested in a more in-depth understanding of AIS and its message types, start with these resources:

- `Wikipedia Article <https://en.wikipedia.org/wiki/Automatic_identification_system>`_: An overview of the Automatic Identification System, including its history, functionality, and applications.

- `AIS Message Types <https://arundaleais.github.io/docs/ais/ais_message_types.html>`_: A guide to the various AIS message types used for communication between ships and tracking systems.

📍 Description
-----------

Package features:
  + SQL database for storing AIS position reports and vessel metadata
  + Vessel position cleaning and trajectory modeling
  + Utilities for streaming and decoding AIS data in the NMEA binary string format (See `Base Station Deployment <AIS_base_station.html>`__)
  + Integration with external datasources including depth charts, distances from shore, vessel geometry, etc.
  + Network graph analysis, MMSI deduplication, interpolation, and other processing utilities
  + Data visualization

# .. image:: https://aisdb.meridian.cs.dal.ca/readme_example.png

| Web Interface:
  https://aisdb.meridian.cs.dal.ca/
| Docs:
  https://aisdb.meridian.cs.dal.ca/doc/readme.html
| Source Code: 
  https://git-dev.cs.dal.ca/meridian/aisdb

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

