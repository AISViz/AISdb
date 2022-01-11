from collections import Counter

import numpy as np

from database.dbconn import DBConn
#from aisdb.common import table_prefix


def sqlite_create_table_polygons(cur):
    cur.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS rtree_polygons USING rtree(
                id,
                minX, maxX,
                minY, maxY,
                +objname TEXT,
                +domain TEXT,
                +binary BLOB
        );
    ''')


def sqlite_createtable_dynamicreport(cur, month):
    ''' sqlite schema for vessel position reports '''
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS ais_{month}_dynamic (
            mmsi integer NOT NULL,
            time INTEGER,
            longitude FLOAT,
            latitude FLOAT,
            rot FLOAT,
            sog FLOAT,
            cog FLOAT,
            heading FLOAT,
            maneuver TEXT,
            utc_second INTEGER,
            PRIMARY KEY (mmsi, time, longitude, latitude)
        ) WITHOUT ROWID ''')


def sqlite_createtable_staticreport(cur, month):
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS ais_{month}_static (
            mmsi INTEGER,
            time INTEGER,
            vessel_name TEXT,
            call_sign TEXT,
            imo INTEGER,
            dim_bow INTEGER,
            dim_stern INTEGER,
            dim_port INTEGER,
            dim_star INTEGER,
            draught INTEGER,
            destination TEXT,
            ais_version TEXT,
            fixing_device STRING,
            eta_month INTEGER,
            eta_day INTEGER,
            eta_hour INTEGER,
            eta_minute INTEGER,
            PRIMARY KEY (mmsi, time, imo)
        ) WITHOUT ROWID ''')


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
        mmsis = np.array(cur.fetchall(), dtype=object).flatten()

        cur.execute(f'DROP TABLE IF EXISTS static_{month}_aggregate')

        fancyprint = lambda cols, widths=[
            12, 24, 12, 12, 12, 12, 12, 12
        ]: ''.join([
            str(c) + ''.join([' ' for _ in range(w - len(str(c)))])
            for c, w in zip(cols, widths)
        ])
        colnames = [
            'mmsi', 'vessel_name', 'ship_type', 'dim_bow', 'dim_stern',
            'dim_port', 'dim_star', 'imo'
        ]
        print(fancyprint(colnames))

        agg_rows = []
        for mmsi in mmsis:
            _ = cur.execute(
                f"""
            SELECT s.mmsi, s.vessel_name,
                --s.ship_type,
                0 as ship_type,
                s.dim_bow,
                s.dim_stern, s.dim_port, s.dim_star, s.imo
              FROM ais_{month}_static AS s
              WHERE s.mmsi = ?
            """, [mmsi])
            cols = np.array(cur.fetchall(), dtype=object).T
            assert len(cols) > 0
            filtercols = np.array([
                np.array(list(filter(None, col)), dtype=object) for col in cols
            ],
                                  dtype=object)

            paddedcols = np.array(
                [col if len(col) > 0 else [None] for col in filtercols],
                dtype=object)

            aggregated = [
                Counter(col).most_common(1)[0][0] for col in paddedcols
            ]

            agg_rows.append(aggregated)

            print('\r' + fancyprint(aggregated), end='       ')

        print()

        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS static_{month}_aggregate (
                mmsi INTEGER PRIMARY KEY,
                vessel_name TEXT,
                ship_type INTEGER,
                dim_bow INTEGER,
                dim_stern INTEGER,
                dim_port INTEGER,
                dim_star INTEGER,
                imo INTEGER
            ) ''')

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
    'msg5': sqlite_createtable_staticreport,
    'msg18': sqlite_createtable_dynamicreport,
    'msg24': sqlite_createtable_staticreport,
    'msg27': sqlite_createtable_dynamicreport,
}
