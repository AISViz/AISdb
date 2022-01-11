Getting Started
===============

.. description:

Description
-----------

Package features:
  + SQL database for storing AIS position reports and vessel metadata
  + Vessel position cleaning and trajectory modeling
  + Utilities for streaming and decoding AIS data in the NMEA binary string format (See `Base Station Deployment <AIS_base_station.html>`__)
  + Integration with public datasources including depth charts, distances from shore, vessel geometry, etc.
  + Plotting with QGIS (work in progress)
  + Network graph analysis, MMSI deduplication, interpolation, and other processing utilities


.. raw:: html 

   <a href='docs/source/scriptoutput.png'>
      <img 
        src='docs/source/scriptoutput.png' 
        width="800"
        onerror="this.src='_images/scriptoutput.png'"
      ></img>
   </a>


| Source Code: 
  https://gitlab.meridian.cs.dal.ca/matt_s/aisdb
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

The package can be installed using pip:

.. code-block:: sh

  python3 -m venv env_aisdb --upgrade
  source env_aisdb/bin/activate
  python3 -m pip install 'git+https://gitlab.meridian.cs.dal.ca/matt_s/aisdb#egg=aisdb'


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
  package will look for configs in the file ``$HOME/.config/ais.cfg``,
  where $HOME is the user’s home directory. If no config file is found,
  the following defaults will be used

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
   database <examples/example01_create_db_from_rawmsgs.py>`__

2. `Automatically generate SQL database
   queries <examples/example02_query_the_database.py>`__

3. | Compute vessel trajectories 
   | TODO: add documentation

4. | Integrating data from public data sources
   | TODO: add documentation

5. `Compute network graph of vessel movements between
   polygons <examples/example04_network_graph.py>`__

6. | Plot with QGIS
   | TODO: add documentation

Collecting AIS Data
-------------------

1. `Setting up an AIS radio station, and exchanging data with other
   networks <docs/AIS_base_station.md>`__
