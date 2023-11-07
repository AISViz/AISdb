
.. tutorials2:

AISdb Tutorial 2
----------------------

Welcome to Tutorial 2 of AISdb, where you'll learn how to seamlessly integrate and query real, historical AIS (Automatic Identification System) data using the AISdb package.

Unlike Tutorial 1, this tutorial takes a step further by utilizing authentic AIS data, providing a more realistic experience. The AIS data used here has been gathered from the MERIDIAN project's advanced antenna network in Nova Scotia. These collected data closely resemble the real-time AIS data you'll encounter when utilizing an AISdb receiver.

By working with genuine historical AIS data, you'll gain valuable insights into the complexities and intricacies involved, surpassing the simplicity of the simulated data used in the previous tutorial.

You can run the following code in **Jupyter Notebook** or **Jupyter Lab**.


1. Install Requirements
====================================

In this section, we will download required packages via pip and import them into our notebook. Before we begin, make sure you have the pip package manager installed.

.. code-block:: python

    # install aisdb
    %pip install aisdb

    # install nest-asyncio for enabling asyncio.run() in Jupyter Notebook
    %pip install nest-asyncio

    # Some of the system may show the following error when running user interface:
    # urllib3 v2.0 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'.
    # install urllib3 v1.26.6 to avoid this error
    %pip install urllib3==1.26.6 

import required packages

.. code-block:: python
    
    import aisdb
    from datetime import datetime, timedelta
    import os
    import nest_asyncio
    nest_asyncio.apply()


2. Select a database
=======================================

We've curated a set of historical AIS data for you to dive into and analyze. This valuable dataset is conveniently stored in a SQLite database format, known for its lightweight design, user-friendly interface, and portability.

In this tutorial, we've taken care of the initial data import process on your behalf. You don't have to worry about importing the data yourself. Instead, you can simply select a database from the downloaded zip file, and you'll gain instant access to a treasure trove of AIS data.

This streamlined approach allows you to focus directly on exploring the data, running queries, and uncovering valuable insights without the need for additional setup. By eliminating the import step, we've created a hassle-free experience that maximizes your time spent working with the data.

If you're interested in learning how to import AIS data into a database, we encourage you to refer to Tutorial 1, where we provide a step-by-step demonstration of the import process.

.. code-block:: bash

    # Download the data file automatically
    wget https://www.dropbox.com/s/09blsyzcumjol52/MERIDIAN_AIS_ESRF_databases.zip

    # Or, you can download the data file manually from the following link:
    # https://drive.google.com/drive/folders/1DXT88-Fl0dbZfzeNTO7G_7__1xbGcGgu
    # and place it in the same folder as this notebook

    # unzip the data file
    unzip MERIDIAN_AIS_ESRF_databases.zip

    # list all the files in the MERIDIAN_AIS_ESRF_databases folder
    ls MERIDIAN_AIS_ESRF_databases


.. code-block:: python

    # select the database file you want to use
    # In this tutorial, we use esrf_hydrophone_01.db
    db_file = 'MERIDIAN_AIS_ESRF_databases/esrf_hydrophone_01.db'

.. code-block:: python

    # Do basic querying for checking if the database is working
    start_time = datetime.strptime("2015-08-01 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime("2015-09-01 00:00:00", '%Y-%m-%d %H:%M:%S')

    with aisdb.DBConn() as dbconn:
    qry = aisdb.DBQuery(
        dbconn=dbconn,
        dbpath=db_file,
        callback=aisdb.database.sql_query_strings.in_timerange,
        start=start_time,
        end=end_time,
    )

    for vessel in aisdb.TrackGen(qry.gen_qry(), decimate=False):
        print(vessel)

As you can see the output, it indicates that you have successfully established a connection to the database using the AISdb package. This crucial step sets the foundation for leveraging the full power of AISdb and unleashing its capabilities to work with AIS data seamlessly.

Now that you have successfully connected, you're ready to explore the myriad functionalities AISdb offers. From querying data to analyzing trends, visualizing insights, and much more, AISdb empowers you to extract meaningful information from AIS data effortlessly.

3. Visualize AIS data
=======================================

In this section, we are thrilled to introduce a unique approach that allows you to directly visualize raw AIS data without the need for extensive processing. By leveraging the capabilities of AISdb, you'll gain immediate access to visual representations of AIS data, enabling you to quickly identify patterns, trends, and key information.

The ability to visualize raw AIS data in its unprocessed form provides a valuable opportunity to gain preliminary insights without undergoing complex data transformations. This direct visualization approach can be particularly useful when time is of the essence or when you're looking for initial visual cues within the data.

.. code-block:: python

    # Do basic querying for checking if the database is working

    start_time = datetime.strptime("2015-08-01 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime("2015-09-01 00:00:00", '%Y-%m-%d %H:%M:%S')

    with aisdb.DBConn() as dbconn:
        qry = aisdb.DBQuery(
            dbconn=dbconn,
            dbpath=db_file,
            callback=aisdb.database.sql_query_strings.in_timerange,
            start=start_time,
            end=end_time,
        )

        rowgen = qry.gen_qry()
        tracks = aisdb.track_gen.TrackGen(rowgen, decimate=False)

        if __name__ == '__main__':
            aisdb.web_interface.visualize(
                tracks,
                visualearth=True,
                open_browser=True,
            )

Now, you can see the AIS data on the map in the browser. The interactive map provides a dynamic and engaging platform for exploring the details of AIS data. You'll have the freedom to zoom in and out, enabling you to focus on specific areas of interest or gain a broader perspective of the maritime landscape. This flexibility empowers you to uncover intricate patterns, vessel movements, and other noteworthy information.

The data are in the area between Nova Scotia and Newfoundland. You can zoom in and out to see the details.


4. Basic AIS data processing
=======================================

In this section we will show how to process AIS data to extract useful information.

.. code-block:: python

    start_time = datetime.strptime("2015-08-01 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime("2015-09-01 00:00:00", '%Y-%m-%d %H:%M:%S')

    with aisdb.DBConn() as dbconn:
        qry = aisdb.DBQuery(
            dbconn=dbconn,
            dbpath=db_file,
            callback=aisdb.database.sql_query_strings.in_timerange,
            start=start_time,
            end=end_time,
        )
        
        rowgen = qry.gen_qry()
        tracks = aisdb.track_gen.TrackGen(rowgen, decimate=False)

        # split trajectories by time without AIS message transmission
        tracks = aisdb.split_timedelta(tracks, timedelta(hours=24))
        # filter the tracks by distance and speed
        tracks = aisdb.encode_greatcircledistance(tracks,
                                                distance_threshold=200000,
                                                speed_threshold=50)
        # interpolate time
        tracks = aisdb.interp_time(tracks, step=timedelta(minutes=5))

        if __name__ == '__main__':
            aisdb.web_interface.visualize(
                tracks,
                visualearth=True,
                open_browser=True,
            )

5. Process AIS data with External Data Source
==============================================================================

In this section, we will demonstrate the process of integrating AIS data with external Bathymetric data to enhance our analysis.

Our objective is to identify all vessels located within circular areas with a radius of 1000m around Cape Town, South Africa, specifically on the date of 2015-08-15.

Subsequently, we will apply a filtering criterion to exclude vessels that have the shortest distance to the coast.

.. code-block:: python

    # Download bathymetry data

    # set the path to the data storage directory
    bathymetry_data_dir = "./bathymetry_data/"

    # check if the directory exists
    if not os.path.exists(bathymetry_data_dir):
        os.makedirs(bathymetry_data_dir)

    # check if the directory is empty\
    if os.listdir(bathymetry_data_dir) == []:
        # download the bathymetry data
        bathy = aisdb.webdata.bathymetry.Gebco(data_dir=bathymetry_data_dir)
        bathy.fetch_bathymetry_grid()
    else:
        print("Bathymetry data already exists.")


.. code-block:: python

    start_time = datetime.strptime("2016-08-01 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime("2016-08-02 00:00:00", '%Y-%m-%d %H:%M:%S')

    # define a function to add color to the tracks
    def add_color(tracks):
        for track in tracks:
            if abs(track['coast_distance'][0]) <= 100:
                track['color'] = "yellow"
            elif abs(track['coast_distance'][0]) <= 1000:
                track['color'] = "orange"
            elif abs(track['coast_distance'][0]) <= 20000:
                track['color'] = "pink"
            else:
                track['color'] = "red"
            yield track

    with aisdb.SQLiteDBConn() as dbconn:

        # define the region of interest
        # In this example, we use a circle with a center at the city of Sydney, Nova Sctia, Canada, and a radius of 200 km
        domain = aisdb.DomainFromPoints(
            points=[(-60.215912, 46.128103),], radial_distances=[200000,])

        qry = aisdb.DBQuery(
            dbconn=dbconn,
            dbpath=db_file,
            callback=aisdb.database.sqlfcn_callbacks.in_bbox_time_validmmsi,
            start=start_time,
            end=end_time,
            xmin=domain.boundary['xmin'],
            xmax=domain.boundary['xmax'],
            ymin=domain.boundary['ymin'],
            ymax=domain.boundary['ymax'],
        )
        rowgen = qry.gen_qry()
        tracks = aisdb.track_gen.TrackGen(rowgen, decimate=True)

        # merge the tracks with the raster data
        raster_path = "./bathymetry_data/gebco_2022_n0.0_s-90.0_w0.0_e90.0.tif"
        raster = aisdb.webdata.load_raster.RasterFile(raster_path)
        tracks_raster = raster.merge_tracks(tracks, new_track_key="coast_distance")

        # add color to the tracks
        tracks_colored = add_color(tracks_raster)

        if __name__ == '__main__':
            aisdb.web_interface.visualize(
                tracks_colored,
                visualearth=True,
                open_browser=True,
            )
        
    

The processed AIS data you obtain through AISdb isn't limited to standalone analysis. We understand the importance of collaboration and integration within your existing pipeline.

With AISdb, you have the flexibility to effortlessly integrate the processed data into your current workflow and seamlessly migrate it to other third-party tools. Whether you require powerful visualization tools, advanced analytics platforms, or any other specific requirements, AISdb empowers you to connect with the tools that best suit your needs.

This integration capability opens up a world of possibilities, allowing you to leverage the strengths of various tools and enhance the value derived from your AIS data. Seamlessly transfer the processed data and collaborate with other stakeholders, enabling informed decision-making and fostering a deeper understanding of maritime operations.

.. code-block:: python

  # install the packages for visualization with plotly 

  %pip install plotly
  %pip install pandas
  %pip install nbformat
  import pandas as pd
  import plotly.express as px
      
.. code-block:: python

    start_time = datetime.strptime("2016-08-01 00:00:00", '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime("2016-08-02 00:00:00", '%Y-%m-%d %H:%M:%S')

    with aisdb.SQLiteDBConn() as dbconn:

        # define the region of interest
        # In this example, we use a circle with a center at the city of Sydney, Nova Sctia, Canada, and a radius of 800 km
        domain = aisdb.DomainFromPoints(
            points=[(-60.215912, 46.128103),], radial_distances=[10000,])

        qry = aisdb.DBQuery(
            dbconn=dbconn,
            dbpath=db_file,
            callback=aisdb.database.sqlfcn_callbacks.in_bbox_time_validmmsi,
            start=start_time,
            end=end_time,
            xmin=domain.boundary['xmin'],
            xmax=domain.boundary['xmax'],
            ymin=domain.boundary['ymin'],
            ymax=domain.boundary['ymax'],
        )
        rowgen = qry.gen_qry()
        tracks = aisdb.track_gen.TrackGen(rowgen, decimate=True)

        # merge the tracks with the raster data
        raster_path = "./bathymetry_data/gebco_2022_n0.0_s-90.0_w0.0_e90.0.tif"
        raster = aisdb.webdata.load_raster.RasterFile(raster_path)
        tracks_raster = raster.merge_tracks(tracks, new_track_key="coast_distance")

        track_list = list(tracks_raster)
        # sort the tracks by the costal distance
        track_list.sort(key=lambda x: x['coast_distance'][0])

        print("The vessel with the longest distance to the coast is in this area:")
        print(track_list[0])

        # 
        # The following code will be used to visualize the track with plotly
        #
        
        # convert the track list to a pandas dataframe
        track_dataframe = pd.DataFrame(track_list)

        track_dataframe['lat'] = track_dataframe['lat'].apply(lambda x: x[0])
        track_dataframe['lon'] = track_dataframe['lon'].apply(lambda x: x[0])
        track_dataframe['coast_distance'] = track_dataframe['coast_distance'].apply(lambda x: x[0])

        # draw a map grapsed on the raster data use plotly
        fig = px.scatter_geo(track_dataframe, lat='lat', lon='lon', color='coast_distance', hover_data=['mmsi'])
        fig.show()
        
