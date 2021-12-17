Getting Started
===============

.. description:

Description
-----------

Package features:
  + SQL database for storing AIS position reports and vessel metadata
  + Utilities for streaming and decoding AIS data in the NMEA binary string format (See `Base Station Deployment <AIS_base_station.html>`__)
  + Integration with public datasources including depth charts, distances from shore, vessel geometry, etc.
  + Plotting with QGIS (work in progress)
  + Network graph analysis, MMSI deduplication, interpolation, and other processing utilities


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

  python3 -m venv env_aisdb --upgrade-deps
  source env_aisdb/bin/activate
  python3 -m pip install 'git+https://gitlab.meridian.cs.dal.ca/matt_s/ais_public#egg=aisdb'


Although the graphical interface is still a work in progress, it can be
enabled by `installing QGIS <https://qgis.org/en/site/forusers/download.html>`__. Note that
when creating an environment using venv, the ``--system-site-packages``
option must be used to share QGIS application data with the environment.

.. docker:

Docker Install
--------------

Build the Dockerfile with docker-compose, and connect to the container
using SSH. Use the ``-X`` option on connect to enable X11 forwarding
over SSH, allowing the QGIS application window to be displayed. You will
need a public/private key to connect, by default the docker-compose file
will look for ``~/.ssh/id_aisdb`` and ``~/.ssh/id_aisdb.pub``. Set the
environment variable ``DATA_DIR`` to the desired storage location, this
path will be mounted as a volume within the container. The default
configuration paths will be used inside this directory 
(see :ref:`Configuring <Configuring>` below)

.. code-block:: sh

  echo "DATA_DIR=/home/$USER/ais/" > .env  
  ssh-keygen -f ~/.ssh/id_aisdb  
  docker-compose up --detach  
  AISDB_IP=`docker inspect aisdb | grep 'IPAddr' | grep '[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*' | cut -d'"' -f4`  
  ssh ais_env@$AISDB_IP -i ~/.ssh/id_aisdb -X  


.. _Configuring: 

Configuring
-----------

| A config file can be used to specify storage location for the database
  as well as directory paths for where to look for additional data. The
  package will look for configs in the file ``$HOME/.config/ais.cfg``,
  where $HOME is the user’s home directory. If no config file is found,
  the following defaults will be used

.. code-block:: sh

  dbpath = $HOME/ais.db
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

3. Compute vessel trajectories TODO: add documentation

4. | Merging data from additional sources
   | TODO: add documentation

5. | Scraping the web for vessel metadata
   | TODO: add documentation

6. `Compute network graph of vessel movements between
   polygons <examples/example04_network_graph.py>`__

7. | Render visualizations
   | TODO: add documentation

Collecting AIS Data
-------------------

1. `Setting up an AIS radio station, and exchanging data with other
   networks <docs/AIS_base_station.md>`__
