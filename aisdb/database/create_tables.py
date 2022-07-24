import os
from collections import Counter

import numpy as np
import warnings

from aisdb.database.dbconn import DBConn, get_dbname
from aisdb import sqlpath


def sqlite_createtable_dynamicreport(db, month, dbpath):
    assert isinstance(db, (DBConn)), f'not a DBConn object {db}'
    with open(os.path.join(sqlpath, 'createtable_dynamic_clustered.sql'),
              'r') as f:
        sql = f.read().format(month).replace(
            f'ais_{month}', f'{get_dbname(dbpath)}.ais_{month}')
    print(sql)
    db.cur.execute(sql)


def sqlite_createtable_staticreport(db, month, dbpath):
    assert isinstance(db, (DBConn)), f'not a DBConn object {db}'
    with open(os.path.join(sqlpath, 'createtable_static.sql'), 'r') as f:
        sql = f.read().format(month).replace(
            f'ais_{month}', f'{get_dbname(dbpath)}.ais_{month}')
    db.cur.execute(sql)


def aggregate_static_msgs(db, months_str):
    ''' collect an aggregate of static vessel reports for each unique MMSI
        identifier. The most frequently repeated values for each MMSI will
        be kept when multiple different reports appear for the same MMSI

        this function should be called every time data is added to the database

        args:
            db (:class:`aisdb.database.dbconn.DBConn`)
                database connection object

            months_str (array)
                array of strings with format: YYYYmm
    '''
    if not isinstance(db, DBConn):  # pragma: no cover
        raise ValueError('db argument must be a DBConn database connection')

    conn, cur = db.conn, db.cur

    for dbpath in db.dbpaths:
        assert 'checksums' not in dbpath
        dbname = get_dbname(dbpath)

        for month in months_str:
            #dbpath = [p for p in db.dbpaths if month[0:4] in p][0]
            # check for monthly tables in dbfiles containing static reports
            cur.execute(
                f'SELECT name FROM {dbname}.sqlite_master WHERE type="table" AND name=?',
                [f'ais_{month}_static'])
            if cur.fetchall() == []:
                continue

        sqlite_createtable_staticreport(db, month, dbpath)
        print(
            f'aggregating static reports into {dbname}.static_{month}_aggregate...'
        )
        cur.execute(
            f'SELECT DISTINCT s.mmsi FROM {dbname}.ais_{month}_static AS s')
        mmsis = np.array(cur.fetchall(), dtype=int).flatten()

        cur.execute(f'DROP TABLE IF EXISTS {dbname}.static_{month}_aggregate')

        with open(os.path.join(sqlpath, 'select_columns_static.sql'),
                  'r') as f:
            sql_select = f.read().format(month).replace(
                'FROM ', f'FROM {dbname}.')

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
            sql_aggregate = f.read().format(month).replace(
                f'static_{month}_aggregate',
                f'{dbname}.static_{month}_aggregate')
        cur.execute(sql_aggregate)

        if len(agg_rows) == 0:
            warnings.warn('no rows to aggregate! '
                          f'table: {dbname}.static_{month}_aggregate')
            continue

        skip_nommsi = np.array(agg_rows, dtype=object)
        assert len(skip_nommsi.shape) == 2
        skip_nommsi = skip_nommsi[skip_nommsi[:, 0] != None]
        assert len(skip_nommsi) > 1
        cur.executemany(
            f''' INSERT INTO {dbname}.static_{month}_aggregate
                    VALUES ({','.join(['?' for _ in range(skip_nommsi.shape[1])])}) ''',
            skip_nommsi)

        conn.commit()
        # db.cur.execute('DETACH DATABASE ?', [dbname])


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
