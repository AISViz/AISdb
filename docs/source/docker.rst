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

.. code-block:: 

.. code-block:: yaml

   services:
     websocket:
       volumes:
         - /home/$USER/ais/ais_2022.db:/home/ais_env/ais/ais.db
         - /home/$USER/ais/:/home/ais_env/ais/
         - /home/$USER/ais/marinetraffic.db:/home/ais_env/ais/marinetraffic.db

   
Instead of using the default command, consider writing a script similar to examples/start_websocket.py with the filepath locations replaced, and volume paths adjusted accordingly


Compose Services
----------------

Run tests, build documentation, and start the webapp with ``docker-compose up --build python-test && docker-compose up --build nginx websocket webserver docserver``. 
Services can also be run individually: ``pkgbuild``, ``python-test``, ``rust-test``, ``webserver``, ``docserver``, ``websocket``, ``nginx``, and ``certbot``.
Note that the ``python-test`` service must be run before starting ``docserver`` for test coverage results (will be output to ``aisdb_web/dist_coverage`` from the project root). 
For SSL configuration with nginx and certbot, mount certificates to ``/etc/letsencrypt/live/$HOSTNAME/fullchain.pem`` and ``/etc/letsencrypt/live/$HOSTNAME/privkey.pem``

.. code-block:: sh

  $ docker-compose up --build pkgbuild  # must be run first
  $ docker-compose up --build python-test rust-test
  $ docker-compose up --build webserver websocket nginx


Website SSL
-----------

Not required for local development.

In ``docker-compose.override.yml``, mount local directories intended for storing certificates from letsencrypt.
Replace ``/home/$USER/cert/`` with a new local directory for this purpose.

.. code-block:: yaml

  services:
    nginx:
      volumes:
        - /home/$USER/cert/conf:/etc/letsencrypt
        - /home/$USER/cert/www:/var/www/certbot
    certbot:
      volumes:
        - /home/$USER/cert/conf:/etc/letsencrypt
        - /home/$USER/cert/www:/var/www/certbot

Disable SSL configuration in nginx temporarily to serve the authentication challenge.
Make the following modification to ``docker/nginx.conf``, commenting lines for SSL:

.. code-block:: cfg

   #listen 443 ssl http2;
   #listen [::]:443 ssl http2;
   #ssl_certificate /etc/letsencrypt/live/${AISDBHOST}/fullchain.pem;
   #ssl_certificate_key /etc/letsencrypt/live/${AISDBHOST}/privkey.pem;
   listen 443;
   listen [::]:443;

Manually request a new certbot authentication challenge from the certbot docker service, replacing $DOMAIN with your fully-qualified domain name.
Sudo permissions may be required.
Follow the prompt and create the files in the mounted cert directory, replacing the directory path with the one used in ``docker-compose.override.yml``.
Restart the router to apply the changes, and then verify that the router is serving the acme challenge with cURL. 

.. code-block:: sh

   docker exec -it certbot certbot certonly --manual -d $DOMAIN

   # in another terminal window:
   mkdir -p /home/$USER/cert/www/.well-known/acme-challenge/
   echo "<challenge token from certbot prompt goes here>" > /home/$USER/cert/www/.well-known/acme-challenge/<challenge filename>
   docker-compose restart nginx

   # verify with curl
   curl $DOMAIN/.well-known/acme-challenge/<challenge filename>


If cURL returns the challenge token provided by certbot, proceed with the prompt by pressing 'Enter'.
Revert ``docker/nginx.conf`` to use SSL and restart the service

.. code-block:: cfg

   listen 443 ssl http2;
   listen [::]:443 ssl http2;
   ssl_certificate /etc/letsencrypt/live/${AISDBHOST}/fullchain.pem;
   ssl_certificate_key /etc/letsencrypt/live/${AISDBHOST}/privkey.pem;
   #listen 443;
   #listen [::]:443;


.. code-block:: sh

   docker-compose restart nginx certbot


See the following tutorial for more info

| https://pentacent.medium.com/nginx-and-lets-encrypt-with-docker-in-less-than-5-minutes-b4b8a60d3a71

