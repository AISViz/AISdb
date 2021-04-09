Tools and utilities for processing AIS data

Requirements:
  * Python 3.8 (may require compile option --enable-loadable-sqlite-extensions)
  * NumPy
  * Shapely
  * pyais
  * qgis

Optional for web data:
  * requests
  * selenium
  * firefox


To test track segmentation, some sample data is provided along with some test code in test/test_segments.py


![ais tracks - one month in the canadian atlantic](https://gitlab.meridian.cs.dal.ca/matt_s/ais_public/-/raw/master/output/scriptoutput.png)


Acknowledgement:
  * Database schema has been adapted from the postgres data model developed by Casey Hilliard

