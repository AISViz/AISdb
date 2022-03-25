import os
from collections import Counter

import numpy as np

from database.dbconn import DBConn

from aisdb import sqlpath


def sqlite_create_table_polygons(cur):
    with open(os.path.join(sqlpath, 'createtable_polygon_rtree.sql'),
              'r') as f:
        sql = f.read()
    cur.execute(sql)


def sqlite_createtable_dynamicreport(cur, month):
    with open(os.path.join(sqlpath, 'createtable_dynamic_clustered.sql'),
              'r') as f:
        sql = f.read().format(month)
    cur.execute(sql)


def sqlite_createtable_staticreport(cur, month):
    with open(os.path.join(sqlpath, 'createtable_static.sql'), 'r') as f:
        sql = f.read().format(month)
    cur.execute(sql)


def aggregate_static_msgs(dbpath, months_str):
    ''' collect an aggregate of static vessel reports for each unique MMSI
        identifier. The most frequently repeated values for each MMSI will
        be kept when multiple different reports appear for the same MMSI

        this function should be called every time data is added to the database

        args:
            dbpath (string)
                path to SQLite database file

            months_str (array)
                array of strings with format: YYYYmm
    '''

    aisdb = DBConn(dbpath=dbpath)
    conn, cur = aisdb.conn, aisdb.cur

    for month in months_str:
        sqlite_createtable_staticreport(cur, month)
        print(f'aggregating static reports into static_{month}_aggregate...')
        cur.execute(f'SELECT DISTINCT s.mmsi FROM ais_{month}_static AS s')
        mmsis = np.array(cur.fetchall(), dtype=int).flatten()

        cur.execute(f'DROP TABLE IF EXISTS static_{month}_aggregate')

        with open(os.path.join(sqlpath, 'select_columns_static.sql'),
                  'r') as f:
            sql_select = f.read().format(month)

        agg_rows = []
        for mmsi in mmsis:
            _ = cur.execute(sql_select, (str(mmsi), ))
            cols = np.array(cur.fetchall(), dtype=object).T
            assert len(cols) > 0

            filtercols = np.array(
                [
                    np.array(list(filter(None, col)), dtype=object)
                    for col in cols
                ],
                dtype=object,
            )

            paddedcols = np.array(
                [col if len(col) > 0 else [None] for col in filtercols],
                dtype=object,
            )

            aggregated = [
                Counter(col).most_common(1)[0][0] for col in paddedcols
            ]

            agg_rows.append(aggregated)

        print()

        with open(os.path.join(sqlpath, 'createtable_static_aggregate.sql'),
                  'r') as f:
            sql_aggregate = f.read().format(month)
        cur.execute(sql_aggregate)

        if len(agg_rows) == 0:
            print(f'no rows to aggregate for table static_{month}_aggregate !')
            continue

        skip_nommsi = np.array(agg_rows, dtype=object)
        assert len(skip_nommsi.shape) == 2
        skip_nommsi = skip_nommsi[skip_nommsi[:, 0] != None]
        assert len(skip_nommsi) > 1
        cur.executemany(
            f''' INSERT INTO static_{month}_aggregate
                    VALUES ({','.join(['?' for _ in range(skip_nommsi.shape[1])])}) ''',
            skip_nommsi)

        conn.commit()
    conn.close()


createfcns = {
    'msg1': sqlite_createtable_dynamicreport,
    'msg2': sqlite_createtable_dynamicreport,
    'msg3': sqlite_createtable_dynamicreport,
    'msg5': sqlite_createtable_staticreport,
    'msg18': sqlite_createtable_dynamicreport,
    'msg19': sqlite_createtable_dynamicreport,
    'msg24': sqlite_createtable_staticreport,
    'msg27': sqlite_createtable_dynamicreport,
}
