# aisdb.database.sql\_query\_strings module

AISDBaisdb.database.sql\_query\_strings.has\_mmsi(_\*_, _alias_, _mmsi_, _\*\*\__)[\[source\]](about:blank/\_modules/aisdb/database/sql\_query\_strings.html#has\_mmsi)

SQL callback selecting a single vessel identifier

Parameters:

* **alias** (_string_) – the ‘alias’ in a ‘WITH tablename AS alias …’ SQL statement
* **mmsi** (_int_) – vessel identifier

Returns:

SQL code (string)

aisdb.database.sql\_query\_strings.in\_bbox(_\*_, _alias_, _xmin_, _xmax_, _ymin_, _ymax_, _\*\*\__)[\[source\]](about:blank/\_modules/aisdb/database/sql\_query\_strings.html#in\_bbox)

SQL callback restricting vessels in bounding box region

Parameters:

* **alias** (_string_) – the ‘alias’ in a ‘WITH tablename AS alias …’ SQL statement
* **xmin** (_float_) – minimum longitude
* **xmax** (_float_) – maximum longitude
* **ymin** (_float_) – minimum latitude
* **ymax** (_float_) – maximum latitude

Returns:

SQL code (string)

aisdb.database.sql\_query\_strings.in\_mmsi(_\*_, _alias_, _mmsis_, _\*\*\__)[\[source\]](about:blank/\_modules/aisdb/database/sql\_query\_strings.html#in\_mmsi)

SQL callback selecting multiple vessel identifiers

Parameters:

* **alias** (_string_) – the ‘alias’ in a ‘WITH tablename AS alias …’ SQL statement
* **mmsis** (_tuple_) – tuple of vessel identifiers (int)

Returns:

SQL code (string)

aisdb.database.sql\_query\_strings.in\_timerange(_\*_, _alias_, _start_, _end_, _\*\*\__)[\[source\]](about:blank/\_modules/aisdb/database/sql\_query\_strings.html#in\_timerange)

SQL callback restricting vessels in temporal range.

Parameters:

* **alias** (_string_) – the ‘alias’ in a ‘WITH tablename AS alias …’ SQL statement
* **start** (_datetime_) –
* **end** (_datetime_) –

Returns:

SQL code (string)

aisdb.database.sql\_query\_strings.valid\_mmsi(_\*_, _alias='m123'_, _\*\*\__)[\[source\]](about:blank/\_modules/aisdb/database/sql\_query\_strings.html#valid\_mmsi)

SQL callback selecting all vessel identifiers within the valid vessel mmsi range, e.g. (201000000, 776000000)

Parameters:

**alias** (_string_) – the ‘alias’ in a ‘WITH tablename AS alias …’ SQL statement

Returns:

SQL code (string)
