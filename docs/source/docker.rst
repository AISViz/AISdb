.. _docker:

Docker
======

Package tests, documentation, and webapp are included as services in ``docker-compose.yml``.


Environment
-----------

To mount a local directory inside the webapp, specify the directory inside 
``.env``, and create a corresponding service entry in 
``docker-compose.override.yml``.
To enable Bing Maps aerial overlay when hosting the application, obtain a WMTS
token from `Bing Maps <https://www.bingmapsportal.com/>`_ and place this in ``.env`` also


``.env``

.. code-block:: sh

  DATA_DIR=/home/$USER/ais/
  BINGMAPSKEY="<your token here>"


``docker-compose.override.yml``

.. code-block:: yaml

   services:
    aisdb_web:
      volumes:
        - ${DATA_DIR}:/home/ais_env/ais

The default paths will be used inside this directory 
(see :ref:`Configuring <Configuring>`)


Compose Services
----------------

Run tests, build documentation, and start the webapp with ``docker-compose up``. 
Services can also be run individually: ``aisdb_test``, ``aisdb_rust``, ``aisdb_web``, ``nginx``,
for python tests, rust tests, web services, and web routing, respectively.

.. code-block:: sh

  $ docker-compose up --build aisdb_rust aisdb_test aisdb_web nginx

