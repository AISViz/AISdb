# Readme

AISDB![https://git-dev.cs.dal.ca/meridian/aisdb/badges/master/pipeline.svg](https://git-dev.cs.dal.ca/meridian/aisdb/badges/master/pipeline.svg)![https://img.shields.io/gitlab/coverage/meridian/aisdb/master?gitlab\_url=https%3A%2F%2Fgit-dev.cs.dal.ca\&job\_name=python-test](https://img.shields.io/gitlab/coverage/meridian/aisdb/master?gitlab\_url=https%3A%2F%2Fgit-dev.cs.dal.ca\&job\_name=python-test) ![https://img.shields.io/gitlab/v/release/meridian/aisdb?gitlab\_url=https%3A%2F%2Fgit-dev.cs.dal.ca\&include\_prereleases\&sort=semver](https://img.shields.io/gitlab/v/release/meridian/aisdb?gitlab\_url=https%3A%2F%2Fgit-dev.cs.dal.ca\&include\_prereleases\&sort=semver)

### Description

Package features:

* SQL database for storing AIS position reports and vessel metadata
* Vessel position cleaning and trajectory modeling
* Utilities for streaming and decoding AIS data in the NMEA binary string format (See Base Station Deployment)
* Integration with external datasources including depth charts, distances from shore, vessel geometry, etc.
* Network graph analysis, MMSI deduplication, interpolation, and other processing utilities
* Data visualization

![https://aisdb.meridian.cs.dal.ca/readme\_example.png](https://aisdb.meridian.cs.dal.ca/readme\_example.png)

### What is AIS?

### Install

Requires Python version 3.8 or newer. Optionally requires SQLite (included in Python) or PostgresQL server (installed separately). The AISDB Python package can be installed using pip. It is recommended to install the package in a virtual Python environment such as `venv`.

```
python -m venv env_ais
source ./env_ais/*/activate
pip install aisdb
```

For information on installing AISDB from source code, see [Installing from Source](https://aisdb.meridian.cs.dal.ca/doc/install\_from\_source.html)

### Documentation

An introduction to AISDB can be found here: [Introduction](https://aisdb.meridian.cs.dal.ca/doc/intro.html).

Additional API documentation: [API Docs](https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.html).

### Code examples

1. [Parsing raw format messages into a database](https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.database.decoder.html#aisdb.database.decoder.decode\_msgs)
2. [Automatically generate SQL database queries](https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.database.dbqry.html#aisdb.database.dbqry.DBQuery)
3. [Compute trajectories from database rows](https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.track\_gen.html#aisdb.track\_gen.TrackGen)
4. [Vessel trajectory cleaning and MMSI deduplication](https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.track\_gen.html#aisdb.track\_gen.encode\_greatcircledistance)
5. [Compute network graph of vessel movements between polygons](https://aisdb.meridian.cs.dal.ca/doc/api/aisdb.network\_graph.html#aisdb.network\_graph.graph)
6. Integrating data from web sources, such as depth charts, shore distance, etc.
