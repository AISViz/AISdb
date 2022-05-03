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


To mount a local directory inside the webapp, specify the ``DATA_DIR`` directory 
in ``.env``, and create a corresponding service entry in ``docker-compose.override.yml``.
This may be desireable when using a pre-built database file for the web interface.


``docker-compose.override.yml``

.. code-block:: yaml

   services:
    websocket:
      volumes:
        - ${DATA_DIR}:/home/ais_env/ais

The default paths will be used inside this directory 
(see :ref:`Configuring <Configuring>`)


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

