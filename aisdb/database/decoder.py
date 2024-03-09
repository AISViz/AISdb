import gzip
import os
import pickle
import tempfile
import zipfile
from copy import deepcopy
from datetime import timedelta
from functools import partial
from hashlib import md5

import psycopg
from aisdb.aisdb import decoder
from dateutil.rrule import rrule, MONTHLY

from aisdb import sqlpath
from aisdb.database.dbconn import PostgresDBConn
from aisdb.database.dbconn import SQLiteDBConn
from aisdb.proc_util import getfiledate


class FileChecksums:
    """
    Initializes a FileChecksums object with a specified database connection.

    :param dbconn: A required parameter of type PostgresDBConn or SQLiteDBConn that represents the database connection.
    :return: None
    """

    def __init__(self, *, dbconn):
        """
        :param dbconn: A required parameter of type PostgresDBConn or SQLiteDBConn that represents the database connection.
        :return: None
        """
        assert isinstance(dbconn, (PostgresDBConn, SQLiteDBConn))
        self.dbconn = dbconn
        self.checksums_table()
        self.tmp_dir = tempfile.mkdtemp()
        if not os.path.isdir(self.tmp_dir):
            os.mkdir(self.tmp_dir)

    def checksums_table(self):
        """
        Creates a checksums table in the database if it doesn't exist.

        This method creates a table named 'hashmap' in the database, if it doesn't already exist.
        The table contains two columns: 'hash' of type TEXT and 'bytes' of type BLOB for SQLiteDBConn *, or BYTEA for PostgresDBConn.

         :param self: an instance of the current object.
         :return: None
        """
        cur = self.dbconn.cursor()
        if isinstance(self.dbconn, SQLiteDBConn):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS
                hashmap(
                    hash TEXT PRIMARY KEY,
                    bytes BLOB
                )
                """)
        elif isinstance(self.dbconn, PostgresDBConn):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS
                hashmap(
                    hash TEXT PRIMARY KEY,
                    bytes BYTEA
                );""")

        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_map on hashmap(hash)")
        self.dbconn.commit()

    def insert_checksum(self, checksum):
        """
        Inserts a checksum and corresponding pickled None value into the hashmap table.

        :param checksum: The checksum to be inserted into the hashmap table.
        :returns: None
        """
        if isinstance(self.dbconn, SQLiteDBConn):
            self.dbconn.execute("INSERT OR IGNORE INTO hashmap VALUES (?,?)",
                                [checksum, pickle.dumps(None)])
            self.dbconn.commit()
        elif isinstance(self.dbconn, PostgresDBConn):
            self.dbconn.execute(
                "INSERT INTO hashmap VALUES ($1,$2) ON CONFLICT DO NOTHING",
                [checksum, pickle.dumps(None)])
            self.dbconn.commit()

    def checksum_exists(self, checksum):
        """
        Check if a given checksum exists in the database.

        :param checksum: The checksum value to check.
        :return: True if the checksum exists in the database, False otherwise.
        """
        cur = self.dbconn.cursor()
        if isinstance(self.dbconn, SQLiteDBConn):
            cur.execute("SELECT * FROM hashmap WHERE hash = ?", [checksum])
        elif isinstance(self.dbconn, PostgresDBConn):
            cur.execute("SELECT * FROM hashmap WHERE hash = %s", [checksum])
        res = cur.fetchone()

        if res is None or res is False:
            return False

        return True

    def get_md5(self, path, f):
        """
        Calculates the MD5 hash digest of a file.

        :param path: The path of the file.
        :param f: The file object to calculate the digest for.
        :return: The MD5 hash digest of the file content.
        """
        if path[-4:].lower() == ".csv":
            _ = f.read(1600)  # skip the header (~1.6kb)
        digest = md5(f.read(1000)).hexdigest()
        return digest


def _fast_unzip(zipf, dirname):
    """
    This function unzips a compressed file archive quickly; it handles both .zip and .gz file formats.

    :param zipf: The path to the compressed file archive.
    :param dirname: The directory to extract the contents of the archive.
    """
    if zipf.lower()[-4:] == ".zip":
        exists = set(sorted(os.listdir(dirname)))
        with zipfile.ZipFile(zipf, "r") as zip_ref:
            contents = set(zip_ref.namelist())
            members = list(contents - exists)
            zip_ref.extractall(path=dirname, members=members)
    elif zipf.lower()[-3:] == ".gz":
        unzip_file = os.path.join(dirname, zipf.rsplit(os.path.sep, 1)[-1][:-3])
        with gzip.open(zipf, "rb") as f1, open(unzip_file, "wb") as f2:
            f2.write(f1.read())
    else:
        raise ValueError("unknown zip file type")


def fast_unzip(zip_filenames, dirname):
    """
    Unzips multiple zip files to a specified directory.

    :param zip_filenames: List of zip file names to be extracted.
    :param dirname: Directory path where the files should be extracted to.
    """
    print(f"unzipping files to {dirname} ... ")
    fcn = partial(_fast_unzip, dirname=dirname)
    for file in zip_filenames:
        fcn(file)


def decode_msgs(filepaths, dbconn, source, vacuum=False, skip_checksum=False, type_preference="all", raw_insertion=False, verbose=True):
    """
    Decode messages from filepaths and insert them into a database.

    :param filepaths: list of file paths to decode
    :param dbconn: database connection to use for insertion
    :param source: source identifier for the decoded messages
    :param vacuum: whether to vacuum the database after insertion (default is False)
    :param skip_checksum: whether to skip checksum validation (default is False)
    :param type_preference: preferred file type to be used (default is "all")
    :param raw_insertion: whether to insert messages without indexing them (default is False)
    :param verbose: whether to print verbose output (default is True)
    :return: None
    """
    if not isinstance(dbconn,
                      (SQLiteDBConn, PostgresDBConn)):  # pragma: no cover
        raise ValueError("db argument must be a DBConn database connection. "
                         f"got {dbconn}")

    if len(filepaths) == 0:  # pragma: no cover
        raise ValueError("must supply atleast one filepath.")

    dbindex = FileChecksums(dbconn=dbconn)

    # handle zipfiles
    zipped = {f for f in filepaths if f.lower()[-4:] == ".zip" or f.lower()[-3:] == ".gz"}
    not_zipped = sorted(list(set(filepaths) - set(zipped)))

    not_zipped_checksums = []
    unzipped_checksums = []
    zipped_checksums = []
    _skipped = []

    if verbose:
        print("generating file checksums...")

    for item in deepcopy(zipped):
        with open(os.path.abspath(item), "rb") as f:
            signature = dbindex.get_md5(item, f)
        if skip_checksum:
            continue
        if dbindex.checksum_exists(signature):
            _skipped.append(item)
            zipped.remove(item)
            if verbose:
                print(f"found matching checksum, skipping {item}")
        else:
            zipped_checksums.append(signature)

    for item in deepcopy(not_zipped):
        with open(os.path.abspath(item), "rb") as f:
            signature = dbindex.get_md5(item, f)
        if skip_checksum:
            continue
        if dbindex.checksum_exists(signature):
            _skipped.append(item)
            not_zipped.remove(item)
            if verbose:
                print(f"found matching checksum, skipping {item}")
        else:
            not_zipped_checksums.append(signature)

    if zipped:
        fast_unzip(zipped, dbindex.tmp_dir)
        unzipped = sorted([
            os.path.join(dbindex.tmp_dir, f)
            for f in os.listdir(dbindex.tmp_dir)
        ])
        assert unzipped
        if not skip_checksum:
            for item in unzipped:
                with open(os.path.abspath(item), "rb") as f:
                    signature = dbindex.get_md5(item, f)
                unzipped_checksums.append(signature)
    else:
        unzipped = []

    raw_files = not_zipped + unzipped

    # raw_files
    if not raw_files:
        print("All files returned an existing checksum.",
              "Cleaning temporary data...")
        for tmpfile in unzipped:
            os.remove(tmpfile)
        os.removedirs(dbindex.tmp_dir)
        return

    assert skip_checksum or len(not_zipped) == len(not_zipped_checksums)
    assert skip_checksum or len(zipped) == len(zipped_checksums)
    assert skip_checksum or len(unzipped) == len(unzipped_checksums)

    if verbose:
        print("checking file dates...")
    filedates = [getfiledate(f) for f in raw_files]
    months = [
        month.strftime("%Y%m") for month in rrule(
            freq=MONTHLY,
            dtstart=min(filedates) - (timedelta(days=min(filedates).day - 1)),
            until=max(filedates),
        )
    ]

    if verbose:
        print("creating tables and dropping table indexes...")

    # drop constraints and indexes to speed up insert,
    # and rebuild them after inserting
    if isinstance(dbconn, PostgresDBConn):
        with open(os.path.join(sqlpath, "psql_createtable_dynamic_noindex.sql"), "r") as f:
            create_dynamic_table_stmt = f.read()
        with open(os.path.join(sqlpath, "psql_createtable_static.sql"), "r") as f:
            create_static_table_stmt = f.read()
        with open(os.path.join(sqlpath, "psql_dynamic_trigger.sql"), "r") as f:
            create_dynamic_trigger = f.read()
        with open(os.path.join(sqlpath, "psql_static_trigger.sql"), "r") as f:
            create_static_trigger = f.read()
        dbconn.execute(create_dynamic_trigger)
        dbconn.execute(create_static_trigger)
        dbconn.commit()  # create/replace triggers
        for month in months:
            dbconn.execute(create_dynamic_table_stmt.format(month))
            dbconn.execute(create_static_table_stmt.format(month))
            for idx_name in ("mmsi", "time", "longitude", "latitude"):
                dbconn.execute(f"DROP INDEX idx_{month}_dynamic_{idx_name};")
        dbconn.commit()
        completed_files = decoder(dbpath="",
                                  psql_conn_string=dbconn.connection_string,
                                  files=raw_files, source=source, verbose=verbose,
                                  workers=4, type_preference=type_preference, allow_swap=False)

    elif isinstance(dbconn, SQLiteDBConn):
        with open(os.path.join(sqlpath, "createtable_dynamic_clustered.sql"), "r") as f:
            create_table_stmt = f.read()
        for month in months:
            dbconn.execute(create_table_stmt.format(month))
        completed_files = decoder(dbpath=dbconn.dbpath,
                                  psql_conn_string="", files=raw_files,
                                  source=source, verbose=verbose, workers=4,
                                  type_preference=type_preference, allow_swap=False)
    else:
        assert False

    if verbose and not skip_checksum:
        print("saving checksums...")

    for filename, signature in zip(not_zipped + unzipped,
                                   not_zipped_checksums + unzipped_checksums):
        if filename in completed_files:
            dbindex.insert_checksum(signature)
        else:
            if verbose:
                print(f"error processing {filename}, skipping checksum...")

    dbindex.dbconn.commit()

    if verbose:
        print("cleaning temporary data...")

    for tmpfile in unzipped:
        os.remove(tmpfile)
    os.removedirs(dbindex.tmp_dir)

    if isinstance(dbconn, PostgresDBConn):
        if verbose:
            print("rebuilding indexes...")
        for month in months:
            dbconn.execute(create_dynamic_table_stmt.format(month))
            dbconn.execute(create_static_table_stmt.format(month))
        if not raw_insertion:
            for idx_name in ("mmsi", "time", "longitude", "latitude"):
                dbconn.execute(f"CREATE INDEX IF NOT EXISTS idx_{month}_dynamic_{idx_name} "
                               f"ON ais_{month}_dynamic ({idx_name});")
            for month in months:
                dbconn.rebuild_indexes(month, verbose)
            dbconn.execute("ANALYZE")
        dbconn.commit()

    dbconn.aggregate_static_msgs(months, verbose)

    if not raw_insertion:
        if vacuum is not False:
            print("finished parsing data\nvacuuming...")
            if isinstance(dbconn, SQLiteDBConn):
                if vacuum is True:
                    dbconn.execute("VACUUM")
                elif isinstance(vacuum, str):
                    assert not os.path.isfile(vacuum)
                    dbconn.execute("VACUUM INTO ?", (vacuum,))
                else:
                    raise ValueError(
                        "vacuum arg must be boolean or filepath string")
                dbconn.commit()
            elif isinstance(dbconn, (PostgresDBConn, psycopg.Connection)):
                pass
            else:
                raise RuntimeError

    return
