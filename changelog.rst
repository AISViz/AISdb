
Changelog
=========

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


