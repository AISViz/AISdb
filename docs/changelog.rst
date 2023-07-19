
Changelog
=========

v1.6.6
------

improved parity between SQLite and Postgres database backends


v1.6.5
------

add track color customization to web_interface


v1.6.4
------

fix issue when inserting checksum into postgres database


v1.6.3
------

support offline package builds from source


v1.6.2
------

improved mac compatability and faster docker builds #9


v1.6.1
------

update maturin version in pyproject.toml


v1.6.0
------

merge feature branch for v1.6.0 #version:minor


v1.5.9
------

python 3.12 support and fix duplicate server status message


v1.5.8
------

fix multiple bugs related to postgres database connections


v1.5.7
------

update docs


v1.5.6
------

fix bugs, update testing, add new examples


v1.5.5
------

fix timerange selection


v1.5.4
------

improved fault tolerance in dispatcher proxy


v1.5.3
------

update readme and docker docs; add python environment to introduction doc


v1.5.2
------

update cloud services documentation


v1.5.1
------

update docker build, lockfiles, and dispatcher docstrings


v1.5.0
------

version bump #version:minor


v1.4.41
-------

add scale bar to web interface


v1.4.40
-------

fix string formatting error in front end


v1.4.39
-------

add map display example, and update docs


v1.4.38
-------

docs


v1.4.37
-------

decoder improvements


v1.4.36
-------

improved error handling and logging in aisdb_lib


v1.4.35
-------

add support for decoding .zip and .gz compressed AIS data files


v1.4.34
-------

fix receiver crash on database timeout


v1.4.33
-------

merge branch fix_website


v1.4.32
-------

python code cleanup


v1.4.31
-------

update docker build, default TCP listen address, and improved logging


v1.4.30
-------

update docs for gis.py, track encoding, and receiving messages on a raspberry pi


v1.4.29
-------

improved radial distance calculation


v1.4.28
-------

improve consistency of dispatcher CLI


v1.4.27
-------

update receiver for new dispatcher API


v1.4.26
-------

update dispatcher API


v1.4.25
-------

fix column issue when writing to CSV file


v1.4.24
-------

fix bug in gis.py initializing domain when coordinate is zero


v1.4.23
-------

update readme


v1.4.22
-------

receiver TLS/TCP


v1.4.21
-------

add rust receiver to python API


v1.4.20
-------

front end updates


v1.4.19
-------

improve sqlite compatability


v1.4.18
-------

update docs and readme


v1.4.17
-------

improved database query handling


v1.4.16
-------

code cleanup


v1.4.15
-------

receiver error handling


v1.4.14
-------

receiver SSL


v1.4.13
-------

update dependency version and add Cargo.lock files to repository


v1.4.12
-------

enable rebroadcasting raw input from receiver


v1.4.11
-------

update readme


v1.4.10
-------

create docker image for receiver


v1.4.9
------

improve sqlite version compatibility


v1.4.8
------

increase default decimation precision


v1.4.7
------

rename receiver


v1.4.6
------

livestream database integration


v1.4.5
------

add buffer size const to socket_dispatch


v1.4.4
------

add option to disable default TrackGen curve decimation


v1.4.3
------

update nginx allowed script-src for localhost


v1.4.2
------

fix for database server sending wrong date range to front end


v1.4.1
------

overhaul livestream back end; add support for MacOS


v1.4.0
------

livestream backend #version:minor


v1.3.150
--------

remove unused code from src/


v1.3.149
--------

fix websocket returning error when fetching time range for empty database


v1.3.148
--------

update tileserver hostname config


v1.3.147
--------

fix port configuration error


v1.3.146
--------

improved web map tile server network routing


v1.3.145
--------

database query error handling and improvements


v1.3.144
--------

improved error handling for websocket client when setting query range


v1.3.143
--------

assure domain zones are within (-180, 180) deg longitude, otherwise, intersect and remap zone polygon at the boundary


v1.3.142
--------

update example start_websocket.py


v1.3.141
--------

add polygon text labels to web app


v1.3.140
--------

intercept tileserver requests and cache tiles with nginx. fall back to openstreetmaps if missing bing maps API token


v1.3.139
--------

refactor javascript modules


v1.3.138
--------

defer SSL to nginx


v1.3.137
--------

allow defining the network graph processing pipeline at runtime


v1.3.136
--------

prevent CI from recreating old tags


v1.3.135
--------

fix network routing issue


v1.3.134
--------

update docker docs for starting docserver container


v1.3.133
--------

refactor docs server into a new docker-service, separated from webapp server


v1.3.132
--------

update docstring for tracks cropping and cleaning with encode_greatcircledistance_async


v1.3.131
--------

websocket client requests will now time out after 3 minutes


v1.3.130
--------

webapp cursor position coordinates display


v1.3.129
--------

optionally increase logging verbosity and improve doctests


v1.3.128
--------

reduce memory leaks in websocket_server.py, and added 10s timeout before dropping client connection


v1.3.127
--------

fix CSS overflow error


v1.3.126
--------

fix bug causing websocket server to hang when making database queries


v1.3.125
--------

refactor network graph and improved test coverage


v1.3.124
--------

minor fixes and updated testing for gis.py and track_gen.py


v1.3.123
--------

remove index.py


v1.3.122
--------

fix ship_type error in asynchronous DB query


v1.3.121
--------

fix web client query from hanging unexpectedly; reorganizing files


v1.3.120
--------

update docstrings


v1.3.119
--------

refactor network_graph


v1.3.118
--------

add multiprocessing queue for parallelized network graphs. resolves #15


v1.3.117
--------

fix zone containment bug in aisdb.gis.Domain.point_in_polygon. resolves #19


v1.3.116
--------

tracks radial filtering


v1.3.115
--------

remove unused args from webscraper


v1.3.114
--------

fix bathymetry rasters from being closed too early when using .merge_tracks() repeatedly (closes #24)


v1.3.113
--------

update CSV output columns ordering


v1.3.112
--------

add variable column name when setting vesseltrack_3D_distance, closes #23


v1.3.111
--------

fix DBQuery missing aggregate table when using alternate sql query function


v1.3.110
--------

enable extrapolation when interpolating outside single time step range #22


v1.3.109
--------

fix rust Chrono deprecation warning; remove rustdoc and rust binary target


v1.3.108
--------

fix interpolation range bug #20


v1.3.107
--------

add raster data to network graph pipeline


v1.3.106
--------

add JSON track serialization and deserialization functions to track_gen module


v1.3.105
--------

improved warnings for bathymetry coordinates outside of longitude range (-180,180). bathymetry module now returns positive depth value instead of negative elevation value


v1.3.104
--------

fix bug causing additional rows to be returned when querying boundaries exceeding longitude range (-180, 180). possibly related to issue #14


v1.3.103
--------

Docstrings for binarysearch_vector() function


v1.3.102
--------

fix DBQuery truncating results from large queries (#17)


v1.3.101
--------

accelerate rasters loading using vectorized binary search from rust module


v1.3.100
--------

fast array indexing with rust: vectorized binary search implementation


v1.3.99
-------

Update websocket_server.py for changes to database connection API (#13)


v1.3.98
-------

update testing for DBConn() API  (#13)


v1.3.97
-------

clean up DBConn() API #13


v1.3.96
-------

fix bathymetry assertion error , closes #14


v1.3.95
-------

network graph domain from point radius geometry #12


v1.3.94
-------

optimized trajectory cleaning and network graph processing pipeline: rewrite trajectory encoder in rust


v1.3.93
-------

add alternative modules using rasterio in load_rasters.py and bathymetry.py


v1.3.92
-------

add more testing for rasterio, bathymetry, and network graph pipeline


v1.3.91
-------

refactor raster loading


v1.3.90
-------

remove merge_data and message_logger modules


v1.3.89
-------

add imported rust functions to sphinx docs


v1.3.88
-------

added tests and improved test coverage


v1.3.87
-------

update dotfiles


v1.3.86
-------

remove unused code and add more warnings


v1.3.85
-------

update docker builds and CI pipeline


v1.3.84
-------

update link in readme


v1.3.83
-------

test CI auto-versioning


v1.3.82
-------

update CI


v1.3.81
-------

update dockerfile


v1.3.80
-------

auto versioning for CI


v1.3.79
-------

improved test coverage for DBQuery, decoder, marinetraffic, network_graph, and trackgen modules


v1.3.78
-------

bug fix for storing cog, sog arrays in track dictionary


v1.3.77
-------

support for rasterio when loading rasters


v1.3.76
-------

refactor web scraping toolchain


v1.3.75
-------

update websocket example


v1.3.74
-------

update network_graph pipeline and bug fixes


v1.3.73
-------

gitlab CI coverage


v1.3.72
-------

update docker builds


v1.3.71
-------

error handling in interp.py


v1.3.70
-------

removed unused utils and fix bug in write_csv()


v1.3.69
-------

remove unused rust module


v1.3.68
-------

improved logging and fixed test in rust decoder


v1.3.67
-------

database cleanup and fix bug in zone bounding box for SQL query


v1.3.66
-------

refactor aisdb_web/map/


v1.3.65
-------

update docker docs and configuration


v1.3.64
-------

update install instructions in readme


v1.3.63
-------

update websocket test


v1.3.62
-------

update dbqry testing


v1.3.61
-------

update compose file


v1.3.60
-------

update websocket_server for new asynchronous database connection API


v1.3.59
-------

update example for refactored database connection API


v1.3.58
-------

code comments in aisdb_web


v1.3.57
-------

update requirements


v1.3.56
-------

update dockerfile


v1.3.55
-------

update testing for new database API


v1.3.54
-------

bug fix in write_csv() when querying only dynamic tables without left join


v1.3.53
-------

refactoring database modules to support multiple attached databases


v1.3.52
-------

add support for multiple connected databases (synchronous), and refactor asynchronous database connection into its own class


v1.3.51
-------

update CI arguments


v1.3.50
-------

update dotfiles


v1.3.49
-------

bug fix in bathymetry database


v1.3.48
-------

update code example


v1.3.47
-------

R-Tree database creation for bathymetry derived from rasters


v1.3.46
-------

add example script for unzipping raw data and creating SQL databases`


v1.3.45
-------

error handling when reading Spire/exactEarth CSV files


v1.3.44
-------

get approximate file date from CSV files


v1.3.43
-------

skip header row when checking CSV file checksums


v1.3.42
-------

improvements to CSV output from track vectors


v1.3.41
-------

add example script for starting websocket server


v1.3.40
-------

add callback SQL function for in_time_bbox_inmmsi


v1.3.39
-------

improved compatability with python versions before 3.10


v1.3.38
-------

heatmap prototyping


v1.3.37
-------

update server routing


v1.3.36
-------

ignore marinetraffic tests in CI


v1.3.35
-------

add profiling to CI


v1.3.34
-------

add webdriver to system path


v1.3.33
-------

update Dockerfile


v1.3.32
-------

automatically create missing tables for DB query


v1.3.31
-------

add heatmaps experimental feature to webserver backend


v1.3.30
-------

create aggregated data results if they dont exist upon DB Query


v1.3.29
-------

improvements to marinetraffic data integration and testing


v1.3.28
-------

update docs for submerged surface area


v1.3.27
-------

update nodejs server routing


v1.3.26
-------

fixed decoded messages counting issue in rust decoder and updated testing


v1.3.25
-------

update wetted surface area computation


v1.3.24
-------

add asynchronous track generators, min speed filter, and update testing


v1.3.23
-------

improved checksums logic for raw data file duplicate checking


v1.3.22
-------

add code coverage to CI


v1.3.21
-------

error handling in web scraping


v1.3.20
-------

asynchronous linear interpolation


v1.3.19
-------

improved database query logic; update static messages aggregation and tests


v1.3.18
-------

update testing


v1.3.17
-------

improved error handling when decoding timestamps


v1.3.16
-------

update documentation


v1.3.15
-------

fix webscraping schema insert conflict


v1.3.14
-------

fix function name in broken test


v1.3.13
-------

bug fixes and improvements to web scraping database


v1.3.12
-------

minor docs cleanup


v1.3.11
-------

update parameter selection and docs in  network graph module


v1.3.10
-------

prevent panic when decoding malformed NMEA messages


v1.3.9
------

update webscraping for zero-config changes


v1.3.8
------

minor changes to docs and docker build


v1.3.7
------

client side bug fixes


v1.3.6
------

bug fixes


v1.3.5
------

refactor encoder


v1.3.4
------

add more integration testing


v1.3.3
------

replace GPL license with MIT license


v1.3.2
------

update websocket server and docker-compose for zero-config


v1.3.1
------

remove configuration requirement


v1.3.0
------

updated database model (version:minor)


v1.2.2
------

fix commit script and remove version.py


v1.2.1
------

minor fixes in dockerfile to install latest package wheel


v1.2.0
------

Package build overhaul using native rust modules #version:minor


v1.1.10
-------

fix bugs when viewing from firefox browser


v1.1.9
------

bug fix


v1.1.8
------

front end overhaul


v1.1.7
------

fully asynchronous server backend


v1.1.6
------

update website build


v1.1.5
------

web client: enable filtering tracks by vessel type, and add ecoregions polygon display via GET request


v1.1.4
------

update nginx routing


v1.1.3
------

update server to vectorize zone geometry when sending to client


v1.1.2
------

docker build: optimize generated webassembly


v1.1.1
------

numerous bug fixes in webapp


v1.1.0
------

update readme #version:minor


v1.0.106
--------

improved error handling for database query edge cases;


v1.0.105
--------

bug fixes and improvements


v1.0.104
--------

update socketserver and map


v1.0.103
--------

more support for different message headers in decoder


v1.0.102
--------

fix graph in network graph CSV file writing


v1.0.101
--------

merge wasm-test feature branch


v1.0.100
--------

minor front end fixes


v1.0.99
-------

bug fixes in web scraping DB


v1.0.98
-------

web client updates


v1.0.97
-------

resolve trajectory linking issue


v1.0.96
-------

update webapp


v1.0.95
-------

refactor polygon geometry handling


v1.0.94
-------

bug fixes and improvements to processing pipeline


v1.0.93
-------

bug fixes in web scraping


v1.0.92
-------

update database client


v1.0.91
-------

database query improvements


v1.0.90
-------

tuning network graph processing


v1.0.89
-------

websocket server for web application database


v1.0.88
-------

update python package build and docker build


v1.0.87
-------

update sphinx documentation


v1.0.86
-------

web application prototyping: Merge branch 'webmap' into master


v1.0.85
-------

bug fix in trajectory processing pipeline


v1.0.84
-------

improvements and bug fixes in metadata collection


v1.0.83
-------

collect vessel metadata when building indexes


v1.0.82
-------

improved contextualization of multi-part messages in rust decoder and bump rust dependency versions


v1.0.81
-------

rewrite web scraper


v1.0.80
-------

fix filepath error when creating database tables


v1.0.79
-------

update track generation from web data sources


v1.0.78
-------

trim whitespace in SQL select query


v1.0.77
-------

refactoring web data sources


v1.0.76
-------

minor fixes and code cleanup


v1.0.75
-------

update CSV functions for new DB format


v1.0.74
-------

refactor track interpolation


v1.0.73
-------

updates to network graph pipeline


v1.0.72
-------

prevent files from being decoded twice and update vessel type descriptions


v1.0.71
-------

compute vessel distance to submerged location


v1.0.70
-------

fix bug in rust decoder


v1.0.69
-------

update testing


v1.0.68
-------

vessel positions polygon masking, update function names, and minor changes


v1.0.67
-------

update readme install text and proc_util


v1.0.66
-------

update web scraping


v1.0.65
-------

update message logging; fix bugs in rust decoder


v1.0.64
-------

update readme


v1.0.63
-------

update gitlab CI


v1.0.62
-------

removing unnecessary code


v1.0.61
-------

improved cross-platform support in rust executable


v1.0.60
-------

update CI


v1.0.59
-------

filter malformed payloads in rust decoder


v1.0.58
-------

include sqlite3 binaries in package preventing issues with outdated software on ubuntu


v1.0.57
-------

prevent rust executable from crashing due to malformed message payload


v1.0.56
-------

update minimum required SQLite version


v1.0.55
-------

comments in marinetraffic module; committing before merge


v1.0.54
-------

fix bug in SQL query generation when querying multiple months at a time


v1.0.53
-------

file checksums performance tuning


v1.0.52
-------

prevent rust executable from crashing when trying to decode empty data files


v1.0.51
-------

store a checksum for every decoded data file; skip decoding if the checksum exists


v1.0.50
-------

docstrings and formatting in index.py


v1.0.49
-------

minor SQL updates


v1.0.48
-------

fix bug in DBQuery.run_qry() and improved bathymetry raster memory management


v1.0.47
-------

update testing for database creation


v1.0.46
-------

fix path resolution errors when creating database from raw data


v1.0.45
-------

update setup.py and sphinxbuild, rename csvreader.rs


v1.0.44
-------

update SQL documentation


v1.0.43
-------

add docstrings and reformatting SQL code


v1.0.42
-------

update project URL


v1.0.41
-------

support for reading exactearth CSV format


v1.0.40
-------

move SQL code to aisdb_sql/


v1.0.39
-------

update gebco bathymetry rasters to latest dataset


v1.0.38
-------

update rust package for CSV decoder dependency


v1.0.37
-------

rust tests for reading from csv


v1.0.36
-------

comment some lines of code not being used right now


v1.0.35
-------

rename variable for clarity


v1.0.34
-------

add time segmenting to network graph processing


v1.0.33
-------

qgis plotting: add line/marker size customization, docstrings, and application window button placeholders


v1.0.32
-------

fix binarysearch to return an index even if search is out of range


v1.0.31
-------

fix divide by zero error when computing vessel speed


v1.0.30
-------

fix SQL error during database creation


v1.0.29
-------

update readme


v1.0.28
-------

docstrings, testing, and formatting


v1.0.27
-------

improvement to loading raster data


v1.0.26
-------

update loading data from marinetraffic.com API


v1.0.25
-------

add port distance


v1.0.24
-------

refactor network graph CSV columns


v1.0.23
-------

include ship type as string in database query by default


v1.0.22
-------

add ship_type when generating track vectors and update docstrings


v1.0.21
-------

improved status messages when decoding


v1.0.20
-------

fix bug with decoding ship_type properly


v1.0.19
-------

prevent network_graph from failing if tmp_dir doesnt exist


v1.0.18
-------

data generation for testing, update network graph test, bathymetry and shore distance now passing tests


v1.0.17
-------

bump dependency version requirement


v1.0.16
-------

bug fix, error handling when modeling vessel trajectories, and updated testing for additional data sources


v1.0.15
-------

add changelog to sphinx docs


v1.0.14
-------

update post-commit hook


v1.0.13
-------

add post-commit hook to repo


v1.0.12
-------

automated version incrementing and changelog updates


v1.0.11
-------

add changelog


