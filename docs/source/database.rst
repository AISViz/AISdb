SQL Database Info
=================


Table Naming
------------

When loading data into the database, messages will be sorted into SQL
tables determined by the message type and month. The names of these
tables follow the following format, where ``{MONTH}`` indicates the
table month in the format YYYYMM

.. code:: sql

   ais_MONTH_static
   ais_MONTH_dynamic

Some additional tables containing computed data may be created
depending on the indexes used. For example, an aggregate of vessel static data
by month, or a virtual table used as a covering index. 
For more info, see
`SQLite R\* Tree Indexes <https://sqlite.org/rtree.html>`__

.. code:: sql 

   static_MONTH_aggregate
   rtree_MONTH_dynamic


Additional tables are also included for storing data not directly
derived from AIS message reports.

.. code:: sql

   rtree_polygons 
   coarsetype_ref
   hashmap


Custom SQL Queries
------------------

Instead of using the included run_qry and gen_qry functions for querying
the database, SQL code can be written manually. As messages are
separated into tables by message type and month, queries spanning
multiple message types or multiple months should use UNIONs and JOINs to
combine results as is appropriate.

For AIS position reports, the R\* tree virtual tables should be queried
instead of the default tables. Query performance can be significantly
improved using the R\* tree index when restricting output to a narrow
range of MMSIs, timestamps, longitudes, and latitudes, although querying
a wide range will not yield much benefit. If custom indexes are required
for specific manual queries, these should be defined on message tables
1_2_3, 5, 18, and 24 directly instead of upon the virtual tables.

Timestamps are stored as epoch-minutes in the database. For convenience,
import the ``dt_2_epoch`` and ``epoch_2_dt`` functions for conversion
between datetime format when querying the database manually.


Schema
------

See ``aisdb_sql/`` to see the SQL code used to create database tables and
associated queries.

