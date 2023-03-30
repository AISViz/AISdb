.. _docker:

Docker
======

Python Docker Quick Start
-------------------------

.. _docker-quickstart:

Alternative to installing with pip, a docker image is provided containing a Python environment with AISDB installed.
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

* db-server

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

* python-test

  - Run Python package integration tests

* pkgbuild

  - Build manylinux-compatible wheels for Python package distribution

* certbot

  - TLS/SSL certificate renewal service. Renews certificates used by the ``nginx`` service. ``privkey.pem`` and ``fullchain.pem`` certificates are mounted in the ``nginx`` container inside directory ``/etc/letsencrypt/live/$FQDN/``,  where ``$FQDN`` is the domain name, e.g. ``127.0.0.1``.

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
  #AISDBPATH='./AIS.sqlitedb'

  # Postgres database config
  # For more info on postgres configs, see:
  # https://github.com/docker-library/docs/blob/master/postgres/README.md#environment-variables
  POSTGRES_PASSWORD='example'

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



