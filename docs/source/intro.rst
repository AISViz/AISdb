Introduction
============

AISDB was created to provide a complete set of tools to aid in the collection and processing of AIS data. 
AISDB can be used with either livestreaming AIS or historical raw AIS data files.
The core data model behind AISDB is an SQLite database, and AISDB provides a Python interface to interact with the database; from database creation, querying, data processing, visualization, and exportation of data in CSV format. 
AISDB also provides tools for integrating AIS with environmental data in raster file formats.
For example, AISDB provides convenience functions to download ocean bathymetric chart grids which may be used to query seafloor depth beneath each surface vessel position, although any raster data using longitude/latitude coordinate grid cells may be appended.


.. raw:: html

   <iframe src="https://aisdb.meridian.cs.dal.ca/?24h" title="AISDB" style="width:100%;height:60vh;"></iframe>



.. _index:

Index
-----

  1. :ref:`Database Creation <db-create>`
      * From MERIDIAN's data livestream
      * From historical AIS data files
  2. :ref:`Querying the database <query>`
      * Connect to the database
      * Get vessel trajectories from the database
      * Query a bounding box encapsulating a collection of zone polygons
  3. :ref:`Processing AIS messages <processing>`
      * Data cleaning and MMSI deduplication
      * Interpolate vessel trajectories to uniform intervals
      * Geofencing and filtering messages
  4. :ref:`Integration with external metadata <external-data>`
      * Retrieve detailed vessel metadata from marinetraffic.com
      * Bathymetric charts
      * Rasters
  5. :ref:`Visualization <visualization>`
      * 
  6. :ref:`Data collection and sharing <data-sharing>`
      * Setting up an AIS receiving antenna
      * Sharing data to external networks


.. _db-create:

1. Database Creation
--------------------


Creating a database from live streaming data
++++++++++++++++++++++++++++++++++++++++++++

A typical workflow for using AISDB requires a database of recorded AIS messages.
The following code snippet demonstrates how to create a new database from MERIDIAN's AIS data stream.
with the argument ``stdout=True``, the raw message input will be copied to stdout before it is decoded and added to the database.

.. code-block:: python
    
  from aisdb.receiver import start_receiver

  start_receiver(connect_addr='aisdb.meridian.cs.dal.ca:9920', dbpath='AIS.sqlitedb', stdout=True)


Creating a database from historical data files
++++++++++++++++++++++++++++++++++++++++++++++

An SQLite database file will be created at the specified database path ``dbpath``.
The ``source`` string will be stored in the database along with the decoded message data, making it easier to integrate data from multiple sources into the same database.
A checksum of the first 1000 bytes from the input file will be stored to prevent processing the same data file twice.
Checksum validation can be disabled by adding the argument ``skip_checksum=True``.
Decoding speed can be improved by placing the raw data files on a seperate hard drive from the database.

.. code-block:: python

   import aisdb

   aisdb.decode_msgs(
     filepaths=['aisdb/tests/test_data_20210701.csv', 'aisdb/tests/test_data_20211101.nm4'],
     dbpath='AIS.sqlitedb',
     dbconn=aisdb.DBConn(),
     source='TESTING',
   )


The decoder accepts raw AIS data in the ``.nm4`` format, as long as a timestamp is included in the message header.
For example:

.. code-block:: text

   \s:41925,c:1635731889,t:1635731965*66\!AIVDM,1,1,,,19NSRM@01v;inKaVqpGVUmN:00Rh,0*7C
   \s:41925,c:1635731889,t:1635731965*66\!AIVDM,1,1,,,15Benl0000<P7Te`HQFVrU<804;`,0*39
   \s:41925,c:1635731889,t:1635731965*66\!AIVDM,1,1,,,17`BO@7P@9;sbjwUDa7uSH:@00RQ,0*35


CSV formatted data files can also be used to create a database.
When using CSV, the following header is expected:

.. code-block:: text

  MMSI,Message_ID,Repeat_indicator,Time,Millisecond,Region,Country,Base_station,Online_data,Group_code,Sequence_ID,Channel,Data_length,Vessel_Name,Call_sign,IMO,Ship_Type,Dimension_to_Bow,Dimension_to_stern,Dimension_to_port,Dimension_to_starboard,Draught,Destination,AIS_version,Navigational_status,ROT,SOG,Accuracy,Longitude,Latitude,COG,Heading,Regional,Maneuver,RAIM_flag,Communication_flag,Communication_state,UTC_year,UTC_month,UTC_day,UTC_hour,UTC_minute,UTC_second,Fixing_device,Transmission_control,ETA_month,ETA_day,ETA_hour,ETA_minute,Sequence,Destination_ID,Retransmit_flag,Country_code,Functional_ID,Data,Destination_ID_1,Sequence_1,Destination_ID_2,Sequence_2,Destination_ID_3,Sequence_3,Destination_ID_4,Sequence_4,Altitude,Altitude_sensor,Data_terminal,Mode,Safety_text,Non-standard_bits,Name_extension,Name_extension_padding,Message_ID_1_1,Offset_1_1,Message_ID_1_2,Offset_1_2,Message_ID_2_1,Offset_2_1,Destination_ID_A,Offset_A,Increment_A,Destination_ID_B,offsetB,incrementB,data_msg_type,station_ID,Z_count,num_data_words,health,unit_flag,display,DSC,band,msg22,offset1,num_slots1,timeout1,Increment_1,Offset_2,Number_slots_2,Timeout_2,Increment_2,Offset_3,Number_slots_3,Timeout_3,Increment_3,Offset_4,Number_slots_4,Timeout_4,Increment_4,ATON_type,ATON_name,off_position,ATON_status,Virtual_ATON,Channel_A,Channel_B,Tx_Rx_mode,Power,Message_indicator,Channel_A_bandwidth,Channel_B_bandwidth,Transzone_size,Longitude_1,Latitude_1,Longitude_2,Latitude_2,Station_Type,Report_Interval,Quiet_Time,Part_Number,Vendor_ID,Mother_ship_MMSI,Destination_indicator,Binary_flag,GNSS_status,spare,spare2,spare3,spare4


The ``decode_msgs()`` function also accepts compressed ``.zip`` and ``.gz`` file formats as long as they can be decoded into either nm4 or CSV.

.. _query:

2. Querying the Database
------------------------


.. _processing:

3. Processing
-------------


.. _external-data:

4. Integration with external metadata
-------------------------------------

Detailed metadata from marinetraffic.com
++++++++++++++++++++++++++++++++++++++++

Bathymetric charts
++++++++++++++++++

Rasters
+++++++


.. _visualization:

5. Visualization
----------------


.. _data-sharing:

6. Data collection and sharing
------------------------------



AIS station operators are encouraged to share incoming AIS data from their receivers with the MERIDIAN data sharing network.
  
.. code-block:: python
    
  # listen for incoming raw AIS messages on port 9921 and share with MERIDIAN network
  start_receiver(udp_listen_addr='0.0.0.0:9921', multicast_rebroadcast_addr='aisdb.meridian.cs.dal.ca:9921')


For further info on how to set up a Raspberry Pi for receiving AIS, see <receiver link>


