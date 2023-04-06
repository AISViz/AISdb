.. _webserver:

Running the Webserver
=====================

The webserver has two primary components:

  - websocket data server (back end)
  - client front end (front end)


Starting the back end server
----------------------------

See the example provided in ``examples/start_websocket.py``:

.. include:: ../../examples/start_websocket.py
   :literal:


Start the front end webserver (local development)
-------------------------------------------------

`Vite <https://vitejs.dev/>`__ can be used for local development and deployment bundling. 
Vite can be installed with npm

.. code-block:: sh

   cd aisdb_web
   npm install
   npx vite aisdb_web/map


Disable SSL for local development
---------------------------------

Set the environment variable ``VITE_DISABLE_SSL`` to a non-null value before starting the webserver.
This will change the websocket client connection URL to ``ws://${hostname}:9924`` instead of ``wss://${hostname}/ws``, where ``${hostname}`` is e.g. ``localhost``.

.. code-block:: sh

   export VITE_DISABLE_SSL=1


Start the webserver (deployment)
--------------------------------

Consider using a :ref:`docker configuration <docker>` to deploy the back end, front end, routing, and manage SSL certification.

