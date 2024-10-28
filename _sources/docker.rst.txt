.. _docker:

Docker
======

Python Docker Quick Start
-------------------------

.. _docker-quickstart:

For most users, the recommended way to use the AISDB Python package is by installing it with pip.
The main purpose of the Python docker images in this repository is to provide a build environment for the Python wheel files that can be used for pip installation, as well as providing a reference and testing environment.
The ``meridiancfi/aisdb:latest`` image is based on ``python:slim`` with the AISDB package wheel installed.
In the ``meridiancfi/aisdb-manylinux:latest`` image, wheel binary files are compiled from Rust and Python source code using a ``manylinux`` base docker image.
The `Manylinux project <https://github.com/pypa/manylinux>`__ aims to provide a convenient way to distribute binary Python extensions as wheels on Linux.
To start the container, ensure that ``docker`` and ``docker-compose`` are installed, and enter into the command line:

.. code-block:: sh

  docker pull meridiancfi/aisdb
  docker run --interactive --tty --volume ./:/aisdb/ meridiancfi/aisdb

.. _docker-compose:

The current working directory will be mounted inside the container as a volume.
The same can be achieved with ``docker-compose`` in the context of the repository compose file:

.. code-block:: sh

  docker-compose --file ./docker-compose.yml run --rm --volume "`pwd`:/aisdb" aisdb-python

Compose Services
----------------

In addition to the Python AISDB docker image, AISDB provides a complete suite of cloud services for storing, accessing, viewing, and networking AIS data.
These services are defined in the ``docker-compose.yml`` file in the project repository. 
Cloud services are implemented in a microservices architecture, using a software-defined network defined in the compose file.
Services include:


* docserver

  - NodeJS webserver to display Sphinx documentation (such as this webpage)

* webserver

  - NodeJS web application front-end interface for AISDB. 

* postgresdb

  - AISDB Postgres database storage.

* db-

  - Web application back-end. Serves vectorized AIS tracks from the Postgres database in response to JSON-formatted queries.

* receiver

  - Listens for IP-enabled AIS transmitters over TLS/TCP or UDP channels, and stores incoming AIS messages in the postgres database. Optionally forwards filtered messages to a downstream UDP channel in raw or parsed format.

* upstream-ais

  - Live streaming AIS server. Listens for AIS messages forwarded by the ``receiver`` service, and reverse-proxy messages to downstream TCP clients or UDP channels. This service provides live-streaming data to the front end ``webserver``.


To start all AISDB cloud services, navigate to the repository root directory and enter into the command line (root permissions may be required):

.. code-block:: sh

  docker-compose up --build nginx db-server webserver docserver receiver upstream-ais




Development and Deployment
--------------------------

The following services are used for the development and deployment of AISDB

* build-wheels

  - Build manylinux-compatible wheels for Python package distribution. The resulting wheel file is installed in aisdb-python.

* python-test

  - Run Python package integration tests. Based on the aisdb-python docker image..

* nginx

  - NGINX gateway router. Routes incoming traffic to the appropriate cloud service. The following ports are exposed by the gateway:

    + ``80``: redirects to port 443
    + ``443``: serves web application endpoints over HTTPS
    + ``9920``: Proxy for the ``upstream-ais`` service stream output (raw format).
    + ``9922``: Proxy for the ``receiver`` service stream output (JSON format).

    The following endpoints are available over HTTPS:

    + ``/``: Proxy for the ``webserver`` service.
    + ``/doc``: Proxy for the ``docserver`` service.
    + ``/ws``: Proxy for the ``db-server`` service.
    + ``/stream``: Alias of port ``9922``. 
    + ``/stream-raw``: Alias of port ``9920``.
    + ``/coverage``: Alias of ``/docs/coverage``.

* certbot

  - TLS/SSL certificate renewal service. Renews certificates used by the ``nginx`` service. ``privkey.pem`` and ``fullchain.pem`` certificates are mounted in the ``nginx`` container inside directory ``/etc/letsencrypt/live/$FQDN/``,  where ``$FQDN`` is the domain name, e.g. ``127.0.0.1``.


.. _environment:

Environment
-----------

Services running with docker compose will read environment variables from a ``.env`` file in the project root directory.
An example ``.env`` file is included here:

.. code-block:: sh

  # Front end config (bundled with Vite for NodeJS)

  # AISDB database server and livestream server hostname
  VITE_AISDBHOST='127.0.0.1'

  # Bing maps token
  # Get your token here: https://www.bingmapsportal.com/
  #VITE_BINGMAPSKEY='<my-token-here>'

  # Disable SSL/TLS for incoming livestream data.
  # When using this option, the front end will connect to the livestream
  # server at ws://$VITE_AISDBHOST:9922
  # Otherwise, the front end will connect to wss://$VITE_AISDBHOST/stream
  VITE_DISABLE_SSL_STREAM=1

  # Disable SSL for the database server connection
  VITE_DISABLE_SSL_DB=1

  # Port used for database server connection.
  # This setting is only active when VITE_DISABLE_SSL_DB is enabled, 
  # otherwise, an SSL connection will be made to https://VITE_AISDBHOST/ws
  VITE_AISDBPORT=9924

  # Allow users to query an unlimited amount of data at one time
  #VITE_NO_DB_LIMIT=1

  # if enabled, Bing Maps will be used for WMTS instead of OpenStreetMaps
  VITE_BINGMAPTILES=1

  # Default WMTS server
  #VITE_TILESERVER="dev.virtualearth.net"
  VITE_TILESERVER="aisdb.meridian.cs.dal.ca"


  # Back end config

  # Hostname
  AISDBHOST='127.0.0.1'
  #AISDBHOST='aisdb.meridian.cs.dal.ca'

  # Database server port
  AISDBPORT=9924

  # Python database path
  AISDBPATH='./AIS.sqlitedb'

  # Postgres database client config
  PGPASSFILE=$HOME/.pgpass
  PGUSER="postgres"
  PGHOST="[fc00::9]"
  PGPORT="5432"

  # Postgres database server config
  # More info here: https://hub.docker.com/_/postgres/
  POSTGRES_PASSWORD="example"

  # This volume will be mounted for the postgres data directory
  POSTGRES_VOLUME_DIR='./postgres_data'

  # NGINX CSP header endpoints
  NGINX_CSP_FRAME_ANCESTORS=""
  #NGINX_CSP_FRAME_ANCESTORS="https://aisdb.meridian.cs.dal.ca/"


  # Tests config

  # Mounted AISDB metadata directory.
  # Will be used during testing
  AISDBDATADIR='/RAID0/ais/'
  AISDBMARINETRAFFIC='/RAID0/ais/marinetraffic_V2.db'


Interacting with Postgres Database
----------------------------------

In some cases, Postgres may preferred over SQLite. 
Postgres offers improved concurrency and scalability over SQLite, at the cost of requiring more disk space and compute resources.
The easiest and recommended way to use AISDB with the Postgresql database is via docker (to manually install dependencies, see :ref:`webapp`). 
To get started, navigate to the repository root directory, and ensure docker and docker-compose are installed. 
Start the AIS receiver, database server, and postgres database docker images. 
Sudo permissions may be required for Docker and docker-compose.

Python API
++++++++++

The receiver will fetch live data streaming from the MERIDIAN AIS network, and store it in the postgres database.
Start the AIS receiver and Postgres database services from the command line with docker-compose:

.. code-block:: sh

  export POSTGRES_PASSWORD="example"
  docker-compose up --build receiver postgresdb


The Postgres database may be interfaced using Python in the same manner as the default SQLite database by using :class:`aisdb.database.dbconn.PostgresDBConn` as a drop-in replacement for the default :class:`aisdb.database.dbconn.DBConn` that uses SQLite.

.. code-block:: python

    import os
    from aisdb.database.dbconn import PostgresDBConn

    # keyword arguments
    dbconn = PostgresDBConn(
        hostaddr='127.0.0.1',
        user='postgres',
        port=5432,
        password=os.environ.get('POSTGRES_PASSWORD'),
    )

    # Alternatively, connect using a connection string:
    dbconn = PostgresDBConn('Postgresql://localhost:5433')

The resulting dbconn may then be used similar to how ``DBConn`` is used in the :ref:`Intro Doc <intro>`

Web API
+++++++

Start the AIS receiver, Postgres database, and database webserver services from the command line using the following command.
See :ref:`docker` for more info on docker services.
Alternatively, the services can be run in a local environment instead of a docker environment as described in :ref:`webapp`.

.. code-block:: sh

  docker-compose up --build receiver postgresdb db-server

The receiver service will listen for new data from MERIDIAN's AIS receiver, and store it in the postgres database.
The db-server service provides a web API for the AIS data stored in the postgres database.
This listens for WebSocket connections on port 9924, and returns JSON-formatted vessel tracks in response to queries.
The following Python code provides an example of how to asynchronously query AIS data from db-server.
This code can either run in a local Python environment or in the aisdb-python docker image.
While this example uses Python, the web API can be accessed using any language or package using the `Websocket Protocol <https://www.rfc-editor.org/rfc/rfc6455>`__, such as JavaScript, as long as requests are formatted as utf8-encoded JSON.

.. include:: ../../examples/query_db_API.py
   :literal:

Interacting with the Map
------------------------


