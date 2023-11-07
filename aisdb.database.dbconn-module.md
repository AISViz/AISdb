# aisdb.database.dbconn module

AISDB

SQLite Database connection

Also see: [https://docs.python.org/3/library/sqlite3.html#connection-objects](https://docs.python.org/3/library/sqlite3.html#connection-objects)

_class_ aisdb.database.dbconn.ConnectionType(_value_)[\[source\]](about:blank/\_modules/aisdb/database/dbconn.html#ConnectionType)

Bases: `Enum`

database connection types enum. used for static type hints

POSTGRES _= \<class 'aisdb.database.dbconn.PostgresDBConn'>_SQLITE _= \<class 'aisdb.database.dbconn.SQLiteDBConn'>_aisdb.database.dbconn.DBConn

alias of `SQLiteDBConn`

_class_ aisdb.database.dbconn.PostgresDBConn(_libpq\_connstring=None_, _\*\*kwargs_)[\[source\]](about:blank/\_modules/aisdb/database/dbconn.html#PostgresDBConn)

Bases: `_DBConn`, `Connection`

This feature requires optional dependency psycopg for interfacing Postgres databases.

The following keyword arguments are accepted by Postgres: | [https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS)

Alternatively, a connection string may be used. Information on connection strings and postgres URI format can be found here: | [https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)

Example:

```
import os
from aisdb.database.dbconn import PostgresDBConn

# keyword arguments
dbconn = PostgresDBConn(
    hostaddr='127.0.0.1',
    user='postgres',
    port=5432,
    password=os.environ.get('POSTGRES_PASSWORD'),
    dbname='postgres',
)

# Alternatively, connect using a connection string:
dbconn = PostgresDBConn('Postgresql://localhost:5433')
```

aggregate\_static\_msgs(_months\_str: list_, _verbose: bool = True_)[\[source\]](about:blank/\_modules/aisdb/database/dbconn.html#PostgresDBConn.aggregate\_static\_msgs)

collect an aggregate of static vessel reports for each unique MMSI identifier. The most frequently repeated values for each MMSI will be kept when multiple different reports appear for the same MMSI

this function should be called every time data is added to the database

Parameters:

* **months\_str** (_list_) – list of strings with format: YYYYmm
* **verbose** (_bool_) – logs messages to stdout

cursor\_factory_: Type\[Cursor\[Row]]_deduplicate\_dynamic\_msgs(_month: str_, _verbose=True_)[\[source\]](about:blank/\_modules/aisdb/database/dbconn.html#PostgresDBConn.deduplicate\_dynamic\_msgs)execute(_sql_, _args=\[]_)[\[source\]](about:blank/\_modules/aisdb/database/dbconn.html#PostgresDBConn.execute)

Execute a query and return a cursor to read its results.

rebuild\_indexes(_month_, _verbose=True_)[\[source\]](about:blank/\_modules/aisdb/database/dbconn.html#PostgresDBConn.rebuild\_indexes)row\_factory_: RowFactory\[Row]_server\_cursor\_factory_: Type\[ServerCursor\[Row]]__class_ aisdb.database.dbconn.SQLiteDBConn(_dbpath_)[\[source\]](about:blank/\_modules/aisdb/database/dbconn.html#SQLiteDBConn)

Bases: `_DBConn`, `Connection`

SQLite3 database connection object

dbpath

database filepath

Type:

str

db\_daterange

temporal range of monthly database tables. keys are DB file names

Type:

dict

aggregate\_static\_msgs(_months\_str: list_, _verbose: bool = True_)[\[source\]](about:blank/\_modules/aisdb/database/dbconn.html#SQLiteDBConn.aggregate\_static\_msgs)

collect an aggregate of static vessel reports for each unique MMSI identifier. The most frequently repeated values for each MMSI will be kept when multiple different reports appear for the same MMSI

this function should be called every time data is added to the database

Parameters:

* **dbconn** (`aisdb.database.dbconn.SQLiteDBConn`) – database connection object
* **months\_str** (_list_) – list of strings with format: YYYYmm
* **verbose** (_bool_) – logs messages to stdout
