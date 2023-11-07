# aisdb.database.dbqry module

class to convert a dictionary of input parameters into SQL code, and generate queries

_class_ aisdb.database.dbqry.DBQuery(_\*_, _dbconn_, _dbpath=None_, _dbpaths=\[]_, _\*\*kwargs_)[\[source\]](about:blank/\_modules/aisdb/database/dbqry.html#DBQuery)

Bases: `UserDict`

A database abstraction allowing the creation of SQL code via arguments passed to \_\_init\_\_(). Args are stored as a dictionary (UserDict).

Parameters:

* **dbconn** ([`aisdb.database.dbconn.ConnectionType`](about:blank/aisdb.database.dbconn.html#aisdb.database.dbconn.ConnectionType)) – database connection object
*   **callback** (_function_) –

    anonymous function yielding SQL code specifying “WHERE” clauses. common queries are included in [`aisdb.database.sqlfcn_callbacks`](about:blank/aisdb.database.sqlfcn\_callbacks.html#module-aisdb.database.sqlfcn\_callbacks), e.g. >>> from aisdb.database.sqlfcn\_callbacks import in\_timerange\_validmmsi >>> callback = in\_timerange\_validmmsi

    this generates SQL code to apply filtering on columns (mmsi, time), and requires (start, end) as arguments in datetime format.
* **limit** (_int_) – Optionally limit the database query to a finite number of rows
* **\*\*kwargs** (_dict_) – more arguments that will be supplied to the query function and callback function

Custom SQL queries are supported by modifying the fcn supplied to .gen\_qry(), or by supplying a callback function. Alternatively, the database can also be queried directly, see dbconn.py for more info

complete example:

```
>>> import os
>>> from datetime import datetime
>>> from aisdb import SQLiteDBConn, DBQuery, decode_msgs
>>> from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi
```

```
>>> dbpath = './testdata/test.db'
>>> start, end = datetime(2021, 7, 1), datetime(2021, 7, 7)
>>> filepaths = ['aisdb/tests/testdata/test_data_20210701.csv', 'aisdb/tests/testdata/test_data_20211101.nm4']
>>> with SQLiteDBConn(dbpath) as dbconn:
...     decode_msgs(filepaths=filepaths, dbconn=dbconn, source='TESTING', verbose=False)
...     q = DBQuery(dbconn=dbconn, callback=in_timerange_validmmsi, start=start, end=end)
...     for rows in q.gen_qry():
...         assert dict(rows[0]) == {'mmsi': 204242000, 'time': 1625176725,
...                                  'longitude': -8.93166666667, 'latitude': 41.45,
...                                  'sog': 4.0, 'cog': 176.0}
...         break
```

check\_marinetraffic(_trafficDBpath_, _boundary_, _retry\_404=False_)[\[source\]](about:blank/\_modules/aisdb/database/dbqry.html#DBQuery.check\_marinetraffic)

scrape metadata for vessels in domain from marinetraffic

Parameters:

* **trafficDBpath** (_string_) – marinetraffic database path
* **boundary** (_dict_) – uses keys xmin, xmax, ymin, and ymax to denote the region of vessels that should be checked. if using [`aisdb.gis.Domain`](about:blank/aisdb.gis.html#aisdb.gis.Domain), the _Domain.boundary_ attribute can be supplied here

create\_qry\_params()[\[source\]](about:blank/\_modules/aisdb/database/dbqry.html#DBQuery.create\_qry\_params)gen\_qry(_fcn=\<function crawl\_dynamic>_, _reaggregate\_static=False_, _verbose=False_)[\[source\]](about:blank/\_modules/aisdb/database/dbqry.html#DBQuery.gen\_qry)

queries the database using the supplied SQL function.

Parameters:

* **self** (_UserDict_) – Dictionary containing keyword arguments
* **fcn** (_function_) – Callback function that will generate SQL code using the args stored in self
* **reaggregate\_static** (_bool_) – If True, the metadata aggregate tables will be regenerated from
* **verbose** (_bool_) – Log info to stdout

Yields:

numpy array of rows for each unique MMSI arrays are sorted by MMSI rows are sorted by time
