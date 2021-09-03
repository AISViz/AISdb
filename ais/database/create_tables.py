from collections import Counter

import numpy as np

from database import dbconn
#import dbconn

#aisdb = dbconn()
#dbtype = aisdb.dbtype
#cur = aisdb.conn.cursor()


def sqlite_create_table_polygons(cur):
    cur.execute(f'''
            CREATE VIRTUAL TABLE IF NOT EXISTS rtree_polygons USING rtree(
                id, 
                minX, maxX, 
                minY, maxY, 
                +objname TEXT, 
                +domain TEXT,
                +binary BLOB
        );
    ''')
    #cur.execute('DROP TABLE rtree_polygons')
    #sqlite_create_table_polygons(cur)


def sqlite_create_table_msg123(cur, month):
    ''' sqlite schema using rtree virtual table as an index '''
    cur.execute(f'''
            CREATE VIRTUAL TABLE rtree_{month}_msg_1_2_3 USING rtree(
                id, 
                --mmsi integer NOT NULL,
                mmsi0, mmsi1,
                --"time" timestamp without time zone NOT NULL,
                t0, t1,
                --longitude double precision,
                x0, x1,
                --latitude double precision,
                y0, y1,
                --+millisecond smallint,
                +msgtype integer,
                +navigational_status smallint,
                +rot double precision,
                +sog real,
                +cog real,
                +heading real,
                +maneuver "char",
                +utc_second smallint 
            );
        ''')
    cur.execute(f'''
            CREATE TABLE ais_{month}_msg_1_2_3 (
                --id INTEGER NOT NULL,
                mmsi integer NOT NULL,
                --"time" timestamp without time zone NOT NULL,
                time INTEGER,
                --millisecond INTEGER,
                msgtype INTEGER,
                --base_station integer,
                navigational_status smallint,
                rot double precision,
                sog real,
                longitude double precision,
                latitude double precision,
                cog real,
                heading real,
                maneuver "char",
                utc_second smallint
            --) 
                ,
                PRIMARY KEY (mmsi, time)
            ) WITHOUT ROWID;
        ''')

    # temporal resolution is reduced to one minute at insertion - only the first item is kept
    #cur.execute(f''' CREATE UNIQUE INDEX idx_{month}_msg123_mmsi_time ON 'ais_{month}_msg_1_2_3' (mmsi, time) ''')

    cur.execute(f''' 
            CREATE TRIGGER idx_rtree_{month}_msg_123 
            AFTER INSERT ON ais_{month}_msg_1_2_3 
            BEGIN
                INSERT INTO rtree_{month}_msg_1_2_3( 
                    --id, 
                    mmsi0, mmsi1, t0, t1, 
                    x0, x1, y0, y1,
                    msgtype, navigational_status, rot, sog, 
                    cog, heading, maneuver, utc_second
                ) 
                VALUES (
                    --new.ROWID, 
                    new.mmsi, new.mmsi, new.time, new.time, 
                    new.longitude, new.longitude, new.latitude, new.latitude,
                    new.msgtype, new.navigational_status, new.rot, new.sog, 
                    new.cog, new.heading, new.maneuver, new.utc_second
                )
            ; END
        ''')



def sqlite_create_table_msg18(cur, month):
    ''' 
    rtree virtual table is used in addition to a normal table
    the idea is to use it as a covering index with faster bounding-box search
    '''
    cur.execute(f'''
            CREATE VIRTUAL TABLE rtree_{month}_msg_18 USING rtree(
                id, 
                mmsi0, mmsi1,
                t0, t1,
                x0, x1,
                y0, y1,
                --+region smallint,
                --+country smallint,
                +msgtype integer,
                +navigational_status smallint,
                --+rot double precision,
                +sog real,
                +cog real,
                +heading real,
                --+maneuver "char",
                +utc_second smallint 
            );
        ''')

    cur.execute(f'''
            CREATE TABLE ais_{month}_msg_18 (
                --id INTEGER NOT NULL,
                mmsi integer NOT NULL,
                --"time" timestamp without time zone NOT NULL,
                time INTEGER,
                --millisecond smallint,
                msgtype INTEGER,
                --region smallint,
                --country smallint,
                --base_station integer,
                navigational_status smallint,
                sog real,
                accuracy boolean,
                longitude double precision,
                latitude double precision,
                cog real,
                heading real,
                utc_second smallint
            --)
                ,
                PRIMARY KEY (mmsi, time)
            ) WITHOUT ROWID;
                --communication_state integer,
                --PRIMARY KEY (mmsi, time, longitude, latitude)
        ''')

    # temporal resolution is reduced to one minute at insertion - only the first item is kept
    #cur.execute(f''' CREATE UNIQUE INDEX idx_{month}_msg18_mmsi_time ON 'ais_{month}_msg_18' (mmsi, time) ''')

    cur.execute(f''' 
            CREATE TRIGGER idx_rtree_{month}_msg_18
            AFTER INSERT ON ais_{month}_msg_18
            BEGIN
                INSERT INTO rtree_{month}_msg_18( 
                    --id, 
                    mmsi0, mmsi1, t0, t1, x0, x1, y0, y1, 
                    navigational_status, sog, cog, 
                    heading, utc_second
                ) 
                VALUES (
                    --new.ROWID, 
                    new.mmsi, new.mmsi, new.time, new.time, 
                    new.longitude, new.longitude, new.latitude, new.latitude,
                    new.navigational_status, new.sog, new.cog, new.heading, 
                    new.utc_second
                )
            ; END
        ''')


def create_table_msg5(cur, month):
    cur.execute(f'''
            CREATE TABLE ais_{month}_msg_5 (
                --id INTEGER PRIMARY KEY AUTOINCREMENT,
                --unq_id_prefix character varying(11),
                --lineno integer,
                --errorflag boolean,
                mmsi integer,
                message_id smallint,
                repeat_indicator "char",
                --"time" timestamp without time zone,
                --millisecond smallint,
                time INTEGER,
                --region smallint,
                --country smallint,
                --base_station integer,
                --online_data character varying(6),
                --group_code character varying(4),
                --sequence_id smallint,
                --channel character varying(3),
                --data_length character varying(20),
                vessel_name character varying(20),
                call_sign character varying(7),
                imo integer,
                ship_type smallint,
                dim_bow smallint,
                dim_stern smallint,
                dim_port smallint,
                dim_star smallint,
                draught smallint,
                destination character varying(20),
                ais_version "char",
                fixing_device smallint,
                --trans_control smallint,
                eta_month smallint,
                eta_day smallint,
                eta_hour smallint,
                eta_minute smallint,
                sequence smallint,
                dte boolean,
                mode smallint
            ) ''')
    if dbtype == 'sqlite3':
        cur.execute(f''' CREATE INDEX idx_{month}_msg5_mmsi ON 'ais_{month}_msg_5' (mmsi)''')
        cur.execute(f''' CREATE INDEX idx_{month}_msg5_time ON 'ais_{month}_msg_5' (time)''')
        cur.execute(f''' CREATE INDEX idx_{month}_msg5_imo  ON 'ais_{month}_msg_5' (imo)''')
    elif dbtype == 'postgres':
        print('indexes not implemented yet for postgres')
        pass
    else: assert False



def create_table_msg24(cur, month):
    cur.execute(f'''
            CREATE TABLE ais_{month}_msg_24 (
                --id INTEGER PRIMARY KEY AUTOINCREMENT,
                --unq_id_prefix character varying(11),
                --lineno integer,
                --errorflag boolean,
                mmsi integer,
                message_id smallint,
                repeat_indicator "char",
                --"time" timestamp without time zone,
                time integer,
                --millisecond smallint,
                --region smallint,
                --country smallint,
                --base_station integer,
                --online_data character varying(6),
                --group_code character varying(4),
                sequence_id smallint,
                channel character varying(3),
                --data_length character varying(20),
                vessel_name character varying(20),
                call_sign character varying(7),
                --imo integer,
                ship_type smallint,
                dim_bow smallint,
                dim_stern smallint,
                dim_port smallint,
                dim_star smallint,
                --fixing_device smallint,
                part_number boolean,
                vendor_id character varying(8),
                mother_ship_mmsi integer,
                --spare character varying(4), 
                model smallint, 
                serial integer
            );
        ''')
    #if dbtype == 'sqlite3':
    cur.execute(f''' CREATE INDEX idx_{month}_msg24_mmsi ON 'ais_{month}_msg_24' (mmsi)''')
    cur.execute(f''' CREATE INDEX idx_{month}_msg24_time ON 'ais_{month}_msg_24' (time)''')
    #elif dbtype == 'postgres':
    #    print('indexes not implemented yet for postgres')
    #    pass
    #else: assert False


def aggregate_static_msg5_msg24(dbpath, months_str):
    ''' collect an aggregate of static vessel reports for each unique MMSI 
        identifier. The most frequently repeated values for each MMSI will
        be kept in instances where there are different reports for the same MMSI

        this function should be called every time new data is added to the table

        args: 
            dbpath (string)
                path to SQLite database file

            months_str (string)
                string describing the month of the tables to be aggregated. 
                format: YYYYMM 
    '''

    aisdb = dbconn(dbpath=dbpath)
    conn, cur = aisdb.conn, aisdb.cur

    for month in months_str:
        print(f'aggregating static reports 5, 24 into static_{month}_aggregate...')
        cur.execute(f''' DROP TABLE IF EXISTS static_{month}_aggregate ''')

        cur.execute(f"""
            SELECT DISTINCT m5.mmsi
              FROM ais_{month}_msg_5 AS m5
             UNION 
            SELECT DISTINCT m24.mmsi
              FROM ais_{month}_msg_24 AS m24
              ORDER BY 1 """)
        mmsis = np.array(cur.fetchall(), dtype=object).flatten()

        fancyprint = lambda cols, widths=[12, 24, 12, 12, 12, 12, 12, 12]: ''.join([str(c) + ''.join([' ' for _ in range(w - len(str(c)))]) for c,w in zip(cols, widths)])
        colnames = ['mmsi', 'vessel_name', 'ship_type', 'dim_bow', 'dim_stern', 'dim_port', 'dim_star', 'imo']
        print(fancyprint(colnames))

        agg_rows = []
        for mmsi in mmsis :
            _ = cur.execute(f"""
            SELECT m5.mmsi, m5.vessel_name, m5.ship_type, m5.dim_bow, m5.dim_stern, 
                m5.dim_port, m5.dim_star, m5.imo
              FROM ais_{month}_msg_5 AS m5
              WHERE m5.mmsi = ?
              UNION ALL
            SELECT m24.mmsi, m24.vessel_name, m24.ship_type, m24.dim_bow, m24.dim_stern, 
                   m24.dim_port, m24.dim_star, NULL as imo
              FROM ais_{month}_msg_24 AS m24
              WHERE m24.mmsi = ?
            """, [mmsi, mmsi])
            cols = np.array(cur.fetchall(), dtype=object).T
            assert len(cols) > 0
            filtercols = np.array([np.array(list(filter(None, col)), dtype=object) for col in cols ], dtype=object)
            paddedcols = np.array([col if len(col) > 0 else [None] for col in filtercols], dtype=object)
            aggregated =  [Counter(col).most_common(1)[0][0] for col in paddedcols]
            agg_rows.append(aggregated)
            print('\r' + fancyprint(aggregated), end='       ')
        print()

        skip_nommsi = np.array(agg_rows, dtype=object)
        skip_nommsi = skip_nommsi[skip_nommsi[:,0] != None]

        cur.execute(f''' 
            CREATE TABLE static_{month}_aggregate (
                mmsi INTEGER PRIMARY KEY, 
                vessel_name TEXT,
                ship_type INTEGER,
                dim_bow INTEGER,
                dim_stern INTEGER,
                dim_port INTEGER,
                dim_star INTEGER,
                imo INTEGER
            ) ''')
        cur.executemany(f''' INSERT INTO static_{month}_aggregate VALUES (?,?,?,?,?,?,?,?) ''', skip_nommsi)
        conn.commit()
    conn.close()


def create_table_msg27(cur, month):
    cur.execute(f'''
            CREATE TABLE ais_{month}_msg_27 (
                --id INTEGER PRIMARY KEY AUTOINCREMENT,
                --unq_id_prefix character varying(11),
                --lineno integer,
                --errorflag boolean,
                mmsi integer,
                --message_id smallint,
                --repeat_indicator "char",
                "time" timestamp without time zone,
                millisecond smallint,
                region smallint,
                country smallint,
                --base_station integer,
                --online_data character varying(6),
                --group_code character varying(4),
                --sequence_id smallint,
                --channel character varying(3),
                --data_length character varying(20),
                navigational_status smallint,
                sog real,
                accuracy boolean,
                longitude double precision,
                latitude double precision,
                cog real
                --raim_flag boolean,
                --gnss_status boolean,
                --spare character varying(4)
            );
        ''')
    #cur.execute(f''' SELECT AddGeometryColumn('ais_{month}_msg_27', 'ais_geom', 4326, 'POINT', 'XY') ''')
    #cur.execute(f''' SELECT CreateSpatialIndex('ais_{month}_msg_27', 'ais_geom') ''')
    #cur.execute(f''' CREATE UNIQUE INDEX idx_{month}_msg27_mmsi_time_lat_lon ON 'ais_{month}_msg_27' (mmsi, time, longitude, latitude)''')
    cur.execute(f''' CREATE INDEX idx_{month}_msg27_mmsi ON 'ais_{month}_msg_27' (mmsi) ''')
    cur.execute(f''' CREATE INDEX idx_{month}_msg27_time ON 'ais_{month}_msg_27' (time) ''')
    cur.execute(f''' CREATE INDEX idx_{month}_msg27_lon ON 'ais_{month}_msg_27' (longitude) ''')
    cur.execute(f''' CREATE INDEX idx_{month}_msg27_lat ON 'ais_{month}_msg_27' (latitude)  ''')





# original schemas that are compatible with postgres - unused, but kept in case sqlite is switched to postgres later    

def create_table_msg123(cur, month):
    ''' postgres schema '''
    cur.execute(f'''
            CREATE TABLE ais_{month}_msg_1_2_3 (
                --id INTEGER PRIMARY KEY AUTOINCREMENT,
                --unq_id_prefix character varying(11),
                --lineno integer,
                --errorflag boolean,
                mmsi integer NOT NULL,
                --message_id smallint,
                --repeat_indicator "char",
                "time" timestamp without time zone NOT NULL,
                millisecond smallint,
                region smallint,
                country smallint,
                --base_station integer,
                --online_data character varying(6),
                --group_code character varying(4),
                --sequence_id smallint,
                --channel character varying(3),
                --data_length character varying(20),
                navigational_status smallint,
                rot double precision,
                sog real,
                --accuracy boolean,
                longitude double precision,
                latitude double precision,
                cog real,
                heading real,
                maneuver "char",
                --raim_flag boolean,
                --communication_state integer,
                utc_second smallint 
                --,
                --spare character varying(4)
                --PRIMARY KEY (mmsi, time, longitude, latitude)
                --PRIMARY KEY (mmsi, time)
            )
            --) WITHOUT ROWID;
        ''')
    #if dbtype == 'sqlite3':
        #cur.execute(f''' SELECT AddGeometryColumn('ais_{month}_msg_1_2_3', 'ais_geom', 4326, 'POINT', 'XY') ''')
        #cur.execute(f''' SELECT CreateSpatialIndex('ais_{month}_msg_1_2_3', 'ais_geom') ''')

        #cur.execute(f''' CREATE INDEX idx_{month}_msg123_mmsi ON 'ais_{month}_msg_1_2_3' (mmsi)''')
        #cur.execute(f''' CREATE INDEX idx_{month}_msg123_time ON 'ais_{month}_msg_1_2_3' (time)''')
        #cur.execute(f''' CREATE INDEX idx_{month}_msg123_lon ON 'ais_{month}_msg_1_2_3' (latitude)''')
        #cur.execute(f''' CREATE INDEX idx_{month}_msg123_lat ON 'ais_{month}_msg_1_2_3' (longitude)''')

        #cur.execute(f''' CREATE UNIQUE INDEX idx_{month}_msg123_mmsi_time_lat_lon ON 'ais_{month}_msg_1_2_3' (mmsi, time, longitude, latitude)''')
        #cur.execute(f''' CREATE INDEX idx_{month}_msg123_lonlat ON 'ais_{month}_msg_1_2_3' (longitude, latitude)''')
        #cur.execute(f''' CREATE UNIQUE INDEX idx_msg123_mmsi_time ON 'ais_{month}_msg_1_2_3' (mmsi, time) ''')
    #    pass
    #elif dbtype == 'postgres':
    #    print('indexes not implemented yet for postgres')
    #    pass
    #else: assert False


def build_idx_msg123(cur, month):
    dt = datetime.now()
    cur.execute(f''' CREATE INDEX idx_{month}_msg123_mmsi_time ON 'ais_{month}_msg_1_2_3' (mmsi, time) ''')
    print('added primary key')
    cur.execute(f''' CREATE INDEX idx_{month}_msg123_mmsi ON 'ais_{month}_msg_1_2_3' (mmsi) ''')
    print('indexed mmsi')
    cur.execute(f''' CREATE INDEX idx_{month}_msg123_time ON 'ais_{month}_msg_1_2_3' (time) ''')
    print('indexed time')
    cur.execute(f''' CREATE INDEX idx_{month}_msg123_lon ON 'ais_{month}_msg_1_2_3' (longitude) ''')
    print('indexed lon')
    cur.execute(f''' CREATE INDEX idx_{month}_msg123_lat ON 'ais_{month}_msg_1_2_3' (latitude) ''')
    print('indexed lat')
    #cur.execute(f''' VACUUM ''')
    print(f'elapsed: {(datetime.now() - dt).seconds}s')


def create_table_msg18(cur, month):
    cur.execute(f'''
            CREATE TABLE ais_{month}_msg_18 (
                --id INTEGER PRIMARY KEY AUTOINCREMENT,
                --unq_id_prefix character varying(11),
                --lineno integer,
                --errorflag boolean,
                mmsi integer NOT NULL,
                --message_id smallint,
                --repeat_indicator "char",
                "time" timestamp without time zone NOT NULL,
                millisecond smallint,
                region smallint,
                country smallint,
                --base_station integer,
                --online_data character varying(6),
                --group_code character varying(4),
                --sequence_id smallint,
                --channel character varying(3),
                --data_length character varying(20),
                sog real,
                accuracy boolean,
                longitude double precision,
                latitude double precision,
                cog real,
                heading real,
                utc_second smallint
                --,
                --unit_flag boolean,
                --display boolean,
                --dsc boolean,
                --band boolean,
                --msg22 boolean,
                --mode smallint,
                --raim_flag boolean,
                --communication_flag boolean,
                --communication_state integer,
                --spare character varying(4),
                --spare2 character varying(4)
                --PRIMARY KEY (mmsi, time, longitude, latitude)
                --PRIMARY KEY (mmsi, time)
            )
            --) WITHOUT ROWID
        ''')
    #if dbtype == 'sqlite3':
        #cur.execute(f''' SELECT AddGeometryColumn('ais_{month}_msg_18', 'ais_geom', 4326, 'POINT', 'XY') ''')
        #cur.execute(f''' SELECT CreateSpatialIndex('ais_{month}_msg_18', 'ais_geom') ''')

    """
        from datetime import datetime
        datetime.now()
        #cur.execute(f''' ALTER TABLE 'ais_{month}_msg_18' ADD CONSTRAINT idx_{month}_msg_18_mmsi_time PRIMARY KEY CLUSTERED (mmsi, time) ''')
        #print('added primary key')
        cur.execute(f''' CREATE INDEX idx_{month}_msg18_mmsi ON 'ais_{month}_msg_18' (mmsi) ''')
        print('indexed mmsi')
        cur.execute(f''' CREATE INDEX idx_{month}_msg18_time ON 'ais_{month}_msg_18' (time) ''')
        print('indexed time')
        cur.execute(f''' CREATE INDEX idx_{month}_msg18_lon ON 'ais_{month}_msg_18' (longitude) ''')
        print('indexed lon')
        cur.execute(f''' CREATE INDEX idx_{month}_msg18_lat ON 'ais_{month}_msg_18' (latitude) ''')
        print('indexed lat')
        cur.execute(f''' VACUUM 'ais_{month}_msg_18 '''')
        datetime.now()
        cur.execute(f''' VACUUM ''')
        datetime.now()
    """

        #cur.execute(f''' CREATE UNIQUE INDEX idx_{month}_msg18_mmsi_time_lat_lon ON 'ais_{month}_msg_18' (mmsi, time, longitude, latitude)''')
        #cur.execute(f''' CREATE UNIQUE INDEX idx_msg18_mmsi_time ON 'ais_{month}_msg_18' (mmsi, time) ''')
    #    pass
    #elif dbtype == 'postgres':
    #    print('indexes not implemented yet for postgres')
    #    pass
    #else: assert False


def build_idx_msg18(cur, month):
    dt = datetime.now()
    cur.execute(f''' CREATE INDEX idx_{month}_msg18_mmsi_time ON 'ais_{month}_msg_18' (mmsi, time) ''')
    print('added primary key')
    cur.execute(f''' CREATE INDEX idx_{month}_msg18_mmsi ON 'ais_{month}_msg_18' (mmsi) ''')
    print('indexed mmsi')
    cur.execute(f''' CREATE INDEX idx_{month}_msg18_time ON 'ais_{month}_msg_18' (time) ''')
    print('indexed time')
    cur.execute(f''' CREATE INDEX idx_{month}_msg18_lon ON 'ais_{month}_msg_18' (longitude) ''')
    print('indexed lon')
    cur.execute(f''' CREATE INDEX idx_{month}_msg18_lat ON 'ais_{month}_msg_18' (latitude) ''')
    print('indexed lat')
    #cur.execute(f''' VACUUM ''')
    print(f'elapsed: {(datetime.now() - dt).seconds}s')
