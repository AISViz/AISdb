AISDB
=====

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

   <a href='docs/source/scriptoutput.png'>
      <img 
        src='docs/source/scriptoutput.png' 
        width="800"
        onerror="this.src='_images/scriptoutput.png'"
      ></img>
   </a>


| Source Code: 
  https://gitlab.meridian.cs.dal.ca/public_projects/aisdb
| Documentation: 
  https://docs.meridian.cs.dal.ca/aisdb/

.. whatisais:

What is AIS?
------------

| Wikipedia:
  https://en.wikipedia.org/wiki/Automatic_identification_system
| Description of message types:
  https://arundaleais.github.io/docs/ais/ais_message_types.html

.. install:

Installing
----------

Database creation with Rust can be enabled by installing the Rust compiler
(Optional). 
If installed, a rust executable will be compiled during pip install

.. code-block:: sh

   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

The package can be installed using pip:

.. code-block:: sh

  python3 -m venv env_aisdb
  source env_aisdb/bin/activate
  python3 -m pip install --verbose 'git+https://gitlab.meridian.cs.dal.ca/public_projects/aisdb#egg=aisdb'

Although the graphical interface is still a work in progress, it can be
enabled by `installing QGIS <https://qgis.org/en/site/forusers/download.html>`__. Note that
when creating an environment using venv, the ``--system-site-packages``
option must be used to share QGIS application data with the environment.

Alternatively, the package can be :ref:`installed with docker <docker>`

.. _Configuring: 

Configuring
-----------

| A config file can be used to specify storage location for the database
  as well as directory paths for where to look for additional data. The
  package will look for configs in ``$HOME/.config/ais.cfg``,
  where $HOME is the user’s home directory. The following defaults will be 
  used for missing values

.. code-block:: sh

  dbpath = $HOME/ais/ais.db
  data_dir = $HOME/ais/
  zones_dir = $HOME/ais/zones/
  tmp_dir = $HOME/ais/tmp_parsing/
  rawdata_dir = $HOME/ais/rawdata/
  output_dir = $HOME/ais/scriptoutput/

  host_addr = localhost
  host_port = 9999

Code examples
-------------

1. `Parsing raw format messages into a
   database <./api/aisdb.database.decoder.html#aisdb.database.decoder.decode_msgs>`__

2. `Automatically generate SQL database
   queries <./api/aisdb.database.dbqry.html#aisdb.database.dbqry.DBQuery>`__

3. `Compute trajectories from database rows <./api/aisdb.track_gen.html#aisdb.track_gen.TrackGen>`__

4. `Vessel trajectory cleaning and MMSI deduplication <./api/aisdb.track_gen.html#aisdb.track_gen.segment_tracks_encode_greatcircledistance>`__

5. `Compute network graph of vessel movements between
   polygons <./api/aisdb.network_graph.html#aisdb.network_graph.graph>`__

6. | Integrating data from web sources, such as depth charts, shore distance, etc.
   | Planned for v1.1 

7. | Plot with QGIS
   | Upcoming in a future version

Collecting AIS Data
-------------------

1. `Setting up an AIS radio station, and exchanging data with other
   networks <docs/AIS_base_station.md>`__
