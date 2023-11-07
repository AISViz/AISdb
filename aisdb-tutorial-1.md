# AISdb Tutorial 1

This tutorial will show you how to use the AISdb package to load AIS data into a database and query it.

### 1. Install Requirements

Before we start, we need to install the required packages. The following code will install the required packages for this tutorial.

```
# install aisdb
%pip install aisdb

# install nest-asyncio for enabling asyncio.run() in Jupyter Notebook
%pip install nest-asyncio

# Some of the system may show the following error when running user interface:
# urllib3 v2.0 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'.
# install urllib3 v1.26.6 to avoid this error
%pip install urllib3==1.26.6
```

import required packages

```
import aisdb
from datetime import datetime, timedelta
import os
import nest_asyncio
nest_asyncio.apply()
```

### 2. Load AIS data into a database

In this section, we will provide a comprehensive demonstration of how to efficiently load AIS data into a database.

Additionally, in real-world scenarios, you have the flexibility to actively listen for stream data and effortlessly load it into the designated database.

```
# list the test data files included in the package
print(os.listdir(os.path.join(aisdb.sqlpath, '..', 'tests', 'testdata')))

dbpath = './AIS2.db'
# use test_data_20210701.csv as the test data
filepaths = [os.path.join(aisdb.sqlpath, '..', 'tests', 'testdata', 'test_data_20210701.csv')]
with aisdb.DBConn() as dbconn:
    aisdb.decode_msgs(filepaths=filepaths, dbconn=dbconn,
                      dbpath=dbpath, source='TESTING')
```

### 3. Visualize AIS data

In this section we will directly visualize AIS data without processing it. This is useful for quickly checking the data.

```
start_time = datetime.strptime("2021-07-01 00:00:00", '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime("2021-07-02 00:00:00", '%Y-%m-%d %H:%M:%S')

with aisdb.SQLiteDBConn() as dbconn:
    qry = aisdb.DBQuery(
        dbconn=dbconn,
        dbpath='./AIS2.db',
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
```

### 4. Basic AIS data processing

In this section we will show how to process AIS data to extract useful information.

```
start_time = datetime.strptime("2021-07-01 00:00:00", '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime("2021-07-02 00:00:00", '%Y-%m-%d %H:%M:%S')

with aisdb.SQLiteDBConn() as dbconn:

    qry = aisdb.DBQuery(
        dbconn=dbconn,
        dbpath='./AIS2.db',
        callback=aisdb.database.sqlfcn_callbacks.in_timerange,
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
```

### 5. Process AIS data with External Data Source

In this section, we will demonstrate the process of integrating AIS data with external Bathymetric data to enhance our analysis.

Our objective is to identify all vessels located within circular areas with a radius of 1000m around Cape Town, South Africa, specifically on the date of 2021-07-01.

Subsequently, we will apply a filtering criterion to exclude vessels that have the shortest distance to the coast.

```
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
```

```
start_time = datetime.strptime("2021-07-01 00:00:00", '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime("2021-07-02 00:00:00", '%Y-%m-%d %H:%M:%S')

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
    # In this example, we use a circle with a center at cape town in South Africa, and a radius of 800 km
    domain = aisdb.DomainFromPoints(
        points=[(18.4157, -33.9646),], radial_distances=[800000,])

    qry = aisdb.DBQuery(
        dbconn=dbconn,
        dbpath='./AIS2.db',
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
```

Furthermore, the processed data can be effortlessly integrated into your existing pipeline, allowing for seamless migration to other third-party tools (e.g., visualization tools) that align with your specific requirements.

```
# install the packages for visualization with plotly

%pip install plotly
%pip install pandas
%pip install nbformat
import pandas as pd
import plotly.express as px
import nbformat
```

```
start_time = datetime.strptime("2021-07-01 00:00:00", '%Y-%m-%d %H:%M:%S')
end_time = datetime.strptime("2021-07-02 00:00:00", '%Y-%m-%d %H:%M:%S')

with aisdb.SQLiteDBConn() as dbconn:

    # define the region of interest
    # In this example, we use a circle with a center at cape town in South Africa, and a radius of 800 km
    domain = aisdb.DomainFromPoints(
        points=[(18.4157, -33.9646),], radial_distances=[800000,])

    qry = aisdb.DBQuery(
        dbconn=dbconn,
        dbpath='./AIS2.db',
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
```
