.. _docker:

Docker
======

Package tests, documentation, and webapp are included as services in ``docker-compose.yml``.


Environment
-----------

To enable Bing Maps aerial overlay when hosting the application, obtain a WMTS
token from `Bing Maps <https://www.bingmapsportal.com/>`_ and place this in 
``.env`` within the project root.


``.env``

.. code-block:: sh

  DATA_DIR=/home/$USER/ais/
  BINGMAPSKEY="<your token here>"


To mount a local database inside the webapp, mount it as a volume in the docker
container, and create a corresponding entry for the ``AISDBPATH`` env variable 
in your ``.env`` file. A similar approach can be used for configuring zone
polygon filepaths with ``AISDBZONES``, and MarineTraffic metadata database using
``AISDBMARINETRAFFIC``. See ``docker-compose.yml`` for more info.


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

