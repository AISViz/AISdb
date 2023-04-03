.. _intro:

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

  0. :ref:`Python Environment <python-env>`
      * Install with Pip
      * Install with Docker
  1. :ref:`Database Creation <db-create>`
      * From MERIDIAN's data livestream
      * From historical AIS data files
  2. :ref:`Querying the database <query>`
      * Connect to the database
      * Get vessel trajectories from the database
      * Query a bounding box encapsulating a collection of zone polygons
  3. :ref:`Processing AIS messages <processing>`
      * Voyage modelling
      * Data cleaning and MMSI deduplication
      * Interpolating, Geofencing, and filtering
      * Exporting as CSV
  4. :ref:`Integration with external metadata <external-data>`
      * Retrieve detailed vessel metadata from marinetraffic.com
      * Bathymetric charts
      * Rasters
  5. :ref:`Visualization <visualization>`
      * Display AIS data in the web interface
  6. :ref:`Data collection and sharing <data-sharing>`
      * Setting up an AIS receiving antenna
      * Sharing data to external networks


.. _python-env:

0. Python Environment
---------------------

.. include:: readme.rst
   :start-after: _install-pip:
   :end-before: _readme-docs:

.. include:: docker.rst
   :start-after: _docker-quickstart:
   :end-before: _docker-compose:

AISDB cloud services are documented in the :ref:`AISDB Docker docs <docker>`.

The Python code in the rest of this document can then be run in the new Python environment.
When using an interpreter such as `Jupyter <https://jupyter.org/>`__, ensure that jupyter is installed in the same environment as AISDB.

.. _db-create:

1. Database Creation
--------------------


Creating a database from live streaming data
++++++++++++++++++++++++++++++++++++++++++++

A typical workflow for using AISDB requires a database of recorded AIS messages.
The following code snippet demonstrates how to create a new database from MERIDIAN's AIS data stream.
with the argument ``stdout=True``, the raw message input will be copied to stdout before it is decoded and added to the database.
Also see the receiver api docs: :func:`aisdb.receiver.start_receiver`. 

.. code-block:: python
    
  from aisdb.receiver import start_receiver

  start_receiver(connect_addr='aisdb.meridian.cs.dal.ca:9920', sqlite_dbpath='AIS.sqlitedb', stdout=True)


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


The :func:`aisdb.database.decoder.decode_msgs` function also accepts compressed ``.zip`` and ``.gz`` file formats as long as they can be decoded into either nm4 or CSV.

.. _query:

2. Querying the Database
------------------------

Parameters for the database query can be defined using :class:`aisdb.database.dbqry.DBQuery`. 
Iterate over rows returned from the database for each vessel with :func:`aisdb.database.dbqry.DBQuery.gen_qry`.
Convert the results into generator yielding dictionaries with numpy arrays describing position vectors e.g. lon, lat, and time using :func:`aisdb.track_gen.TrackGen`.


The following query will return vessel positions from the past 48 hours:

.. code-block:: python

    import aisdb
    from datetime import datetime, timedelta

    with aisdb.DBConn() as dbconn:
      qry = aisdb.DBQuery(
        dbconn=dbconn,
        dbpath='AIS.sqlitedb',
        callback=aisdb.database.sql_query_strings.in_timerange,
        start=datetime.utcnow() - timedelta(hours=48),
        end=datetime.utcnow(),
      )

      for vessel in aisdb.TrackGen(qry.gen_qry()):
          print(vessel)


A specific region can be queried for AIS data using :class:`aisdb.gis.Domain` or one of its subclasses to define a collection of ``shapely`` polygon features.
For this example, the domain contains a single bounding box polygon derived from a longitude/latitude coordinate pair and radial distance specified in meters.
If multiple features are included in the domain object, the domain boundaries will encompass the convex hull of all features contained within. 

.. code-block:: python

    with DBConn() as dbconn:
        domain = aisdb.DomainFromPoints(points=[(-63.6, 44.6),], radial_distances=[5000,])
        qry = aisdb.DBQuery(
            dbconn=dbconn,
            dbpath='AIS.sqlitedb',
            callback=aisdb.database.sqlfcn_callbacks.in_bbox_time_validmmsi,
            start=datetime.utcnow() - timedelta(hours=48),
            end=datetime.utcnow(),
            xmin=domain.boundary['xmin'],
            xmax=domain.boundary['xmax'],
            ymin=domain.boundary['ymin'],
            ymax=domain.boundary['ymax'],
        )

      for vessel in aisdb.TrackGen(qry.gen_qry()):
          print(vessel)


Additional query callbacks for filtering by region, timeframe, identifier, etc. can be found in :mod:`aisdb.database.sql_query_strings` and :mod:`aisdb.database.sqlfcn_callbacks`

.. _processing:

3. Processing
-------------

Voyage Modelling
++++++++++++++++

The generator described above can be input into a processing function, yielding modified results.
For example, to model the activity of vessels on a per-voyage or per-transit basis, each voyage is defined as a continuous vector of vessel positions where the time between observed timestamps never exceeds a 24-hour period.

.. code-block:: python

    import aisdb
    from datetime import datetime, timedelta

    maxdelta = timedelta(hours=24)

    with aisdb.DBConn() as dbconn:
      qry = aisdb.DBQuery(
        dbconn=dbconn,
        dbpath='AIS.sqlitedb',
        callback=aisdb.database.sql_query_strings.in_timerange,
        start=datetime.utcnow() - timedelta(hours=48),
        end=datetime.utcnow(),
      )

      tracks = aisdb.TrackGen(qry.gen_qry())
      track_segments = aisdb.split_timedelta(tracks, maxdelta)

      for segment in track_segments:
          print(segment)


Data cleaning and MMSI deduplication
++++++++++++++++++++++++++++++++++++


A common issue with AIS is that the data is noisy, and databases may contain multiple vessels broadcasting with same identifier at the same time.
The :func:`aisdb.track_gen.encode_greatcircledistance` function uses an encoder to check the approximate distance between each vessel's position, and then segments resulting vectors where a surface vessel couldn't reasonably travel there using the most direct path, e.g. above 50 knots.
A distance threshold and speed threshold are used as a hard limit on the maximum delta distance or delta time allowed between messages to be considered continuous.
A score is computed for each position delta, with sequential messages in close proximity at shorter intervals given a higher score, calculated by haversine distance divided by elapsed time.
Any deltas with a score not reaching the minimum threshold are considered as the start of a new segment.
Finally, the beginning of each new segment is compared to the end of each existing segment with a matching vessel identifier, and if the delta exceeds the minimum score, the segments are concatenated.
If multiple existing trajectories meet the minimum score threshold, the new segment will be concatenated the existing segment with the highest score.

Processing functions may be executed in sequence as a processing chain or pipeline, so after segmenting the individual voyages as shown above, results can be input into the encoder to effectively remove noise and correct for vessels with duplicate identifiers.

.. code-block:: python

    import aisdb
    from datetime import datetime, timedelta

    maxdelta = timedelta(hours=24)
    distance_threshold = 200000  # meters
    speed_threshold = 50  # knots
    minscore = 1e-6

    with aisdb.DBConn() as dbconn:
        qry = aisdb.DBQuery(
          dbconn=dbconn,
          dbpath='AIS.sqlitedb',
          callback=aisdb.database.sql_query_strings.in_timerange,
          start=datetime.utcnow() - timedelta(hours=48),
          end=datetime.utcnow(),
        )

        tracks = aisdb.TrackGen(qry.gen_qry())
        track_segments = aisdb.split_timedelta(tracks, maxdelta)
        tracks_encoded = aisdb.encode_greatcircledistance(track_segments, distance_threshold=distance_threshold, speed_threshold=speed_threshold, minscore=minscore)


Interpolating, geofencing and filtering
+++++++++++++++++++++++++++++++++++++++

Building on the above processing pipeline, the resulting cleaned trajectories can then be geofenced and filtered for results contained by atleast one domain polygon, and interpolated for uniformity.

.. code-block:: python

    # ... 
        domain = aisdb.DomainFromPoints(points=[(-63.6, 44.6),], radial_distances=[5000,])
        tracks_filtered = aisdb.track_gen.fence_tracks(tracks_encoded, domain)
        tracks_interp = aisdb.interp_time(tracks_filtered, step=timedelta(minutes=15))

        for segment in track_segments:
            print(segment)


Additional processing functions can be found in the :mod:`aisdb.track_gen` module.

Exporting as CSV
++++++++++++++++

The resulting processed voyage data can be exported in CSV format instead of being printed:

.. code-block:: python

    # ...
        aisdb.write_csv(tracks_interp, 'ais_24h_processed.csv')


.. _external-data:

4. Integration with external metadata
-------------------------------------

Bathymetric charts
++++++++++++++++++

Rasters
+++++++

Detailed metadata from marinetraffic.com
++++++++++++++++++++++++++++++++++++++++


.. _visualization:

5. Visualization
----------------

AIS data contained in the database may be viewed in a web map such as the one shown above.
Requires nodejs and npm installed.
Clone the project repo, and this script from inside the project root directory to start the web client and database server.
Configuration options such as database path may be set by environment variables.

.. literalinclude:: ../../examples/display.py
   :language: python

.. _data-sharing:

6. Data collection and sharing
------------------------------

AIS station operators are may set up an AIS base station with a Raspberry Pi receiver client, and store messages in a database with the receiver server.
For instructions on transmitting AIS from a Raspberry Pi to the receiver server, see :ref:`Setting up an AIS receiver client <receiver>`.
The receiver server can then forward incoming filtered messages to a downstream UDP multicast channel:
  
.. code-block:: python

  import aisdb
    
  # listen for incoming raw AIS messages on port 9921 and share with MERIDIAN network
  aisdb.start_receiver(
    udp_listen_addr='0.0.0.0:9921', 
    multicast_rebroadcast_addr='aisdb.meridian.cs.dal.ca:9921',
  )

