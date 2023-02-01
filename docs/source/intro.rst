AISDB
===== 

New User Guide
--------------

AISDB was created to provide a complete set of tools to aid in the collection and processing of AIS data. 
AISDB can be used with either livestreaming AIS or historical raw AIS data files.
The core data model behind AISDB is an SQLite database, and AISDB provides a Python interface to interact with the database; from database creation, querying, data processing, visualization, and exportation of data in CSV format. 
AISDB also provides tools for integrating AIS with environmental data in raster file formats.
For example, AISDB provides convenience functions to download ocean bathymetric chart grids which may be used to query seafloor depth beneath each surface vessel position, although any raster data using longitude/latitude coordinate grid cells may be appended.




Index
-----

  1. `Database Creation <database_create>`__
      * From MERIDIAN's data livestream
      * From historical AIS data files
  2. `Querying the database <query>`__
      * Connect to the database
      * Get vessel trajectories from the database
      * Query a bounding box encapsulating a collection of zone polygons
  3. Processing AIS messages
      * Data cleaning and MMSI deduplication
      * Interpolate vessel trajectories to uniform intervals
      * Geofencing and filtering vessel positions within a collection of zone polygons
      * Filtering messages based on vessel speed
  4. Integration with external data sources
      * Retrieve additional vessel metadata from marinetraffic.com
      * Bathymetric charts
  5. Data collection and sharing
      * Setting up an AIS receiving antenna
      * Sharing data to external networks


.. database_create

Database Creation
-----------------


Creating a database from live streaming data
++++++++++++++++++++++++++++++++++++++++++++

A typical workflow for using AISDB requires a database of recorded AIS messages.
To create a new database from MERIDIAN's crowd-sourced AIS data stream, see the following code snippet.

.. code-block:: python
    
  from aisdb.receiver import start_receiver

  start_receiver(connect_addr='aisdb.meridian.cs.dal.ca:9920', dbpath='AIS.sqlitedb', stdout=True)


AIS station operators are encouraged to share incoming AIS data from their receivers with the MERIDIAN data sharing network.
  
.. code-block:: python
    
  # listen for incoming raw AIS messages on port 9921 and share with MERIDIAN network
  start_receiver(udp_listen_addr='0.0.0.0:9921', multicast_rebroadcast_addr='aisdb.meridian.cs.dal.ca:9921')


For further info on how to set up a Raspberry Pi for receiving AIS, see <receiver link>


Creating a database from historical data files
++++++++++++++++++++++++++++++++++++++++++++++

