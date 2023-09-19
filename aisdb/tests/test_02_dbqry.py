import os
import warnings
from datetime import datetime, timedelta

from shapely.geometry import Polygon

from aisdb import (
    DBConn,
    DBQuery,
    Domain,
    PostgresDBConn,
    sqlfcn,
    sqlfcn_callbacks,
)
from aisdb.database.create_tables import sql_createtable_dynamic
from aisdb.database.decoder import decode_msgs
from aisdb.tests.create_testing_data import (
    postgres_test_conn,
    sample_database_file,
    sample_gulfstlawrence_bbox,
)
from aisdb.track_gen import TrackGen


def test_query_emptytable(tmpdir):
    warnings.filterwarnings('error')
    dbpath = os.path.join(tmpdir, 'test_query_emptytable.db')
    try:
        with DBConn(dbpath) as dbconn:
            q = DBQuery(
                dbconn=dbconn,
                start=datetime(2021, 1, 1),
                end=datetime(2021, 1, 7),
                callback=sqlfcn_callbacks.in_timerange_validmmsi,
            )
            dbconn.execute(sql_createtable_dynamic.format('202101'))
            rows = q.gen_qry(reaggregate_static=True)
            assert list(rows) == []
    except UserWarning as warn:
        assert 'No static data for selected time range!' in warn.args[0]
    except Exception as err:
        raise err


def test_prepare_qry_domain(tmpdir):

    testdbpath = os.path.join(tmpdir, 'test_prepare_qry_domain.db')
    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain('gulf domain', zones=[{'name': 'z1', 'geometry': z1}])
    with DBConn(testdbpath) as aisdatabase:
        rowgen = DBQuery(
            dbconn=aisdatabase,
            start=start,
            end=end,
            **domain.boundary,
            callback=sqlfcn_callbacks.in_timerange,
        ).gen_qry(reaggregate_static=True)
        next(rowgen)


def test_sql_query_strings(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_sql_query_strings.db')
    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain('gulf domain', zones=[{'name': 'z1', 'geometry': z1}])

    with DBConn(testdbpath) as aisdatabase:
        for callback in [
                sqlfcn_callbacks.in_bbox,
                sqlfcn_callbacks.in_bbox_time,
                sqlfcn_callbacks.in_bbox_time_validmmsi,
                sqlfcn_callbacks.in_time_bbox_inmmsi,
                sqlfcn_callbacks.in_timerange,
                sqlfcn_callbacks.in_timerange_hasmmsi,
                sqlfcn_callbacks.in_timerange_validmmsi,
        ]:
            rowgen = DBQuery(
                dbconn=aisdatabase,
                start=start,
                end=end,
                **domain.boundary,
                callback=callback,
                mmsi=316000000,
                mmsis=[316000000, 316000001],
            ).gen_qry(fcn=sqlfcn.crawl_dynamic_static)
            next(rowgen)


def test_sql_query_strings_postgres(tmpdir):
    testingdata_nm4 = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20211101.nm4')
    testingdata_csv = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20210701.csv')
    testingdata_gz = os.path.join(os.path.dirname(__file__), 'testdata',
                                  'test_data_20211101.nm4.gz')
    testingdata_zip = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20211101.nm4.zip')
    filepaths = [
        testingdata_csv, testingdata_nm4, testingdata_gz, testingdata_zip
    ]
    #testdbpath = os.path.join(tmpdir, 'test_sql_query_strings.db')
    #months = sample_database_file(testdbpath)
    months = ['202107', '202111']
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain('gulf domain', zones=[{'name': 'z1', 'geometry': z1}])
    with PostgresDBConn(**postgres_test_conn) as aisdatabase:
        decode_msgs(filepaths=filepaths,
                    dbconn=aisdatabase,
                    source='TESTING',
                    vacuum=True,
                    verbose=True,
                    skip_checksum=True)
        for callback in [
                sqlfcn_callbacks.in_bbox,
                sqlfcn_callbacks.in_bbox_time,
                sqlfcn_callbacks.in_bbox_time_validmmsi,
                sqlfcn_callbacks.in_time_bbox_inmmsi,
                sqlfcn_callbacks.in_timerange,
                sqlfcn_callbacks.in_timerange_hasmmsi,
                sqlfcn_callbacks.in_timerange_validmmsi,
        ]:
            rowgen = DBQuery(
                dbconn=aisdatabase,
                start=start,
                end=end,
                **domain.boundary,
                callback=callback,
                mmsi=316000000,
                mmsis=[316000000, 316000001],
            ).gen_qry(fcn=sqlfcn.crawl_dynamic_static)
            next(rowgen)
        aisdatabase.rebuild_indexes(months[0], verbose=False)
        aisdatabase.deduplicate_dynamic_msgs(months[0], verbose=True)
        aisdatabase.deduplicate_dynamic_msgs(months[0], verbose=False)


def test_compare_sqlite_postgres_query_output(tmpdir):
    testdbpath = os.path.join(tmpdir,
                              'test_compare_sqlite_postgres_query_output.db')
    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = datetime(int(months[-1][0:4]), int(months[-1][4:6]), 1)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain('gulf domain', zones=[{'name': 'z1', 'geometry': z1}])

    testingdata_nm4 = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20211101.nm4')
    testingdata_csv = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20210701.csv')
    testingdata_gz = os.path.join(os.path.dirname(__file__), 'testdata',
                                  'test_data_20211101.nm4.gz')
    testingdata_zip = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20211101.nm4.zip')
    filepaths = [
        testingdata_csv, testingdata_nm4, testingdata_gz, testingdata_zip
    ]

    with DBConn(testdbpath) as sqlitedb, PostgresDBConn(
            **postgres_test_conn) as pgdb:

        decode_msgs(filepaths=filepaths,
                    dbconn=sqlitedb,
                    source='TESTING_SQLITE',
                    vacuum=False,
                    verbose=True,
                    skip_checksum=True)
        sqlitedb.commit()

        decode_msgs(filepaths=filepaths,
                    dbconn=pgdb,
                    source='TESTING_POSTGRES',
                    vacuum=False,
                    verbose=True,
                    skip_checksum=True)

        pgdb.commit()

        rowgen1 = DBQuery(
            dbconn=sqlitedb,
            start=start,
            end=end,
            **domain.boundary,
            callback=sqlfcn_callbacks.in_time_bbox_validmmsi,
        ).gen_qry(reaggregate_static=True)

        rowgen2 = DBQuery(
            dbconn=pgdb,
            start=start,
            end=end,
            **domain.boundary,
            callback=sqlfcn_callbacks.in_time_bbox_validmmsi,
        ).gen_qry(reaggregate_static=True)

        tracks1 = list(TrackGen(rowgen1, decimate=False))
        tracks2 = list(TrackGen(rowgen2, decimate=False))

    for a, b in zip(tracks1, tracks2):
        assert a == b
