# aisdb.database.decoder module

AISDB

Parsing NMEA messages to create an SQL database. See function decode\_msgs() for usage

_class_ aisdb.database.decoder.FileChecksums(_\*_, _dbconn_)[\[source\]](about:blank/\_modules/aisdb/database/decoder.html#FileChecksums)

Bases: `object`

checksum\_exists(_checksum_)[\[source\]](about:blank/\_modules/aisdb/database/decoder.html#FileChecksums.checksum\_exists)checksums\_table()[\[source\]](about:blank/\_modules/aisdb/database/decoder.html#FileChecksums.checksums\_table)

instantiates new database connection and creates a checksums hashmap table if it doesn’t exist yet.

creates a temporary directory and saves path to `self.tmp_dir`

creates SQLite connection attribute `self.dbconn`, which should be closed after use

e.g.

self.dbconn.close()

get\_md5(_path_, _f_)[\[source\]](about:blank/\_modules/aisdb/database/decoder.html#FileChecksums.get\_md5)

get md5 hash from the first kilobyte of data

insert\_checksum(_checksum_)[\[source\]](about:blank/\_modules/aisdb/database/decoder.html#FileChecksums.insert\_checksum)aisdb.database.decoder.decode\_msgs(_filepaths_, _dbconn_, _source_, _vacuum=False_, _skip\_checksum=False_, _verbose=True_)[\[source\]](about:blank/\_modules/aisdb/database/decoder.html#decode\_msgs)

Decode NMEA format AIS messages and store in an SQLite database. To speed up decoding, create the database on a different hard drive from where the raw data is stored. A checksum of the first kilobyte of every file will be stored to prevent loading the same file twice.

If the filepath has a .gz or .zip extension, the file will be decompressed into a temporary directory before database insert.

Parameters:

* **filepaths** (_list_) – absolute filepath locations for AIS message files to be ingested into the database
* **dbconn** ([`aisdb.database.dbconn.DBConn`](about:blank/aisdb.database.dbconn.html#aisdb.database.dbconn.DBConn)) – database connection object
* **source** (_string_) – data source name or description. will be used as a primary key column, so duplicate messages from different sources will not be ignored as duplicates upon insert
* **vacuum** (_boolean, str_) – if True, the database will be vacuumed after completion. if string, the database will be vacuumed into the filepath given. Consider vacuuming to second hard disk to speed this up

Returns:

None

example:

```
>>> import os
>>> from aisdb import decode_msgs, DBConn
>>> filepaths = ['aisdb/tests/testdata/test_data_20210701.csv',
...              'aisdb/tests/testdata/test_data_20211101.nm4']
>>> with SQLiteDBConn('test_decode_msgs.db') as dbconn:
...     decode_msgs(filepaths=filepaths, dbconn=dbconn,
...                 source='TESTING', verbose=False)
```

aisdb.database.decoder.decoder(_dbpath_, _psql\_conn\_string_, _files_, _source_, _verbose_)

Parse NMEA-formatted strings, and create databases from raw AIS transmissions

Parameters:

* **dbpath** (_str_) – Output SQLite database path. Set this to an empty string to only use Postgres
* **psql\_conn\_string** (_str_) – Postgres database connection string. Set this to an empty string to only use SQLite
* **files** (_array of str_) – array of .nm4 raw data filepath strings
* **source** (_str_) – data source text. Will be used as a primary key index in database
* **verbose** (_bool_) – enables logging

Returns:

None

aisdb.database.decoder.fast\_unzip(_zipfilenames_, _dirname_, _processes=12_)[\[source\]](about:blank/\_modules/aisdb/database/decoder.html#fast\_unzip)

unzip many files in parallel any existing unzipped files in the target directory will be skipped
