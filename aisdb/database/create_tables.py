import os
from collections import Counter

import numpy as np
import warnings

from aisdb.database.dbconn import DBConn
from aisdb import sqlpath


def sqlite_createtable_dynamicreport(dbconn, month, dbpath):
    assert isinstance(dbconn, (DBConn)), f'not a DBConn object {dbconn}'
    dbconn._attach(dbpath)
    with open(os.path.join(sqlpath, 'createtable_dynamic_clustered.sql'),
              'r') as f:
        sql = f.read().format(month).replace(
            f'ais_{month}', f'{dbconn._get_dbname(dbpath)}.ais_{month}')
    dbconn.execute(sql)


def sqlite_createtable_staticreport(dbconn, month, dbpath):
    assert isinstance(dbconn, (DBConn)), f'not a DBConn object {dbconn}'
    dbconn._attach(dbpath)
    with open(os.path.join(sqlpath, 'createtable_static.sql'), 'r') as f:
        sql = f.read().format(month).replace(
            f'ais_{month}', f'{dbconn._get_dbname(dbpath)}.ais_{month}')
    dbconn.execute(sql)


def aggregate_static_msgs(dbconn, months_str, verbose=False):
    ''' collect an aggregate of static vessel reports for each unique MMSI
        identifier. The most frequently repeated values for each MMSI will
        be kept when multiple different reports appear for the same MMSI

        this function should be called every time data is added to the database

        args:
            dbconn (:class:`aisdb.database.dbconn.DBConn`)
                database connection object
            months_str (array)
                array of strings with format: YYYYmm
            verbose (bool)
                logs messages to stdout
    '''

    if not isinstance(dbconn, DBConn):  # pragma: no cover
        raise ValueError('db argument must be a DBConn database connection')

    assert not hasattr(dbconn, 'dbpath')
    assert hasattr(dbconn, 'dbpaths')
    assert 'main' not in dbconn.dbpaths

    for dbpath in dbconn.dbpaths:
        dbname = dbconn._get_dbname(dbpath)
        assert dbname != 'main'
        cur = dbconn.cursor()

        for month in months_str:
            # check for monthly tables in dbfiles containing static reports
            cur.execute(
                f'SELECT name FROM {dbname}.sqlite_master WHERE type="table" AND name=?',
                [f'ais_{month}_static'])
            if cur.fetchall() == []:
                continue

        sqlite_createtable_staticreport(dbconn, month, dbpath)
        if verbose:
            print('aggregating static reports into '
                  f'{dbname}.static_{month}_aggregate...')
        cur.execute('SELECT DISTINCT s.mmsi FROM '
                    f'{dbname}.ais_{month}_static AS s')
        mmsis = np.array(cur.fetchall(), dtype=int).flatten()

        cur.execute('DROP TABLE IF EXISTS '
                    f'{dbname}.static_{month}_aggregate')

        #with open(os.path.join(sqlpath, 'select_columns_static.sql'), 'r') as f:
        #    sql_select = f.read().format(month).replace( 'FROM ', f'FROM {dbname}.')

        sql_select = '''
          SELECT
            s.mmsi, s.imo, TRIM(vessel_name) as vessel_name, s.ship_type,
            s.call_sign, s.dim_bow, s.dim_stern, s.dim_port, s.dim_star,
            s.draught
          FROM ais_{}_static AS s WHERE s.mmsi = ?
        '''.format(month).replace('FROM ', f'FROM {dbname}.')

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

        with open(os.path.join(sqlpath, 'createtable_static_aggregate.sql'),
                  'r') as f:
            sql_aggregate = f.read().format(month).replace(
                f'static_{month}_aggregate',
                f'{dbname}.static_{month}_aggregate')

        cur.execute(sql_aggregate)

        if len(agg_rows) == 0:  # pragma: no cover
            warnings.warn('no rows to aggregate! '
                          f'table: {dbname}.static_{month}_aggregate')
            continue

        skip_nommsi = np.array(agg_rows, dtype=object)
        assert len(skip_nommsi.shape) == 2
        skip_nommsi = skip_nommsi[skip_nommsi[:, 0] != None]
        assert len(skip_nommsi) > 1
        cur.executemany((
            f'INSERT INTO {dbname}.static_{month}_aggregate '
            f"VALUES ({','.join(['?' for _ in range(skip_nommsi.shape[1])])}) "
        ), skip_nommsi)

        dbconn.commit()
