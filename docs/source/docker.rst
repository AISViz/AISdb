.. _docker:

Docker
======

Package tests, documentation, and webapp are included as services in ``docker-compose.yml``.


Environment
-----------

To enable Bing Maps aerial overlay when hosting the application, obtain a WMTS
token from `Bing Maps <https://www.bingmapsportal.com/>`_ and place this in 
``.env`` within the project root.
The ``AISDBHOST`` and ``AISDBPORT`` configurations set the webserver listen address and port, which will default to ``127.0.0.1`` and ``9924`` if unset. 
By default, the webserver will listen to requests from all clients (``*``), but this can be restricted by setting ``AISDBHOSTALLOW``.


``.env``

.. code-block:: sh

  BINGMAPSKEY="<your token here>"
  AISDBHOST="<your FQDN here>"
  AISDBPORT=9924
  AISDBHOSTALLOW="*"


The default command for starting the websocket server will look for the database file, zone polygons folder, and marinetraffic database file at the following volume locations.
These can be mounted inside the docker container using a ``docker-compose.override.yml`` in the root project directory, for example:

.. code-block:: yml

   services:
     websocket:
       volumes:
         - /home/arch/ais/ais_2022.db:/home/ais_env/ais/ais.db
         - /home/arch/ais/:/home/ais_env/ais/
         - /home/arch/ais/marinetraffic.db:/home/ais_env/ais/marinetraffic.db

   
Instead of using the default command, consider writing a script similar to examples/start_websocket.py with the filepath locations replaced, and volume paths adjusted accordingly


Compose Services
----------------

Run tests, build documentation, and start the webapp with ``docker-compose up --build pkgbuild && docker-compose up --build nginx websocket webserver``. 
Services can also be run individually: ``pkgbuild``, ``python-test``, ``rust-test``, ``webserver``, ``websocket``, ``nginx``, and ``certbot``.
Note that the ``pkgbuild`` service must be run before running any dependant services. 
For SSL configuration with nginx and certbot, mount certificates to ``/etc/letsencrypt/live/$HOSTNAME/fullchain.pem`` and ``/etc/letsencrypt/live/$HOSTNAME/privkey.pem``

.. code-block:: sh

  $ docker-compose up --build pkgbuild  # must be run first
  $ docker-compose up --build python-test rust-test
  $ docker-compose up --build webserver websocket nginx


Self-Signed SSL for local development
-------------------------------------

TODO: document self-signed certificates


Website SSL
-----------

Not required for local development.
The following tutorial describes how to configure SSL certification using lets encrypt with docker.

| https://pentacent.medium.com/nginx-and-lets-encrypt-with-docker-in-less-than-5-minutes-b4b8a60d3a71

