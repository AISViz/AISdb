

# original schemas that are compatible with postgres - unused, but kept in case sqlite is switched to postgres later    

def create_table_msg123(cur, month):
    ''' postgres schema '''
    cur.execute(f'''
            CREATE TABLE IF NOT EXISTS ais_{month}_msg_1_2_3 (
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

def create_table_msg18(cur, month):
    cur.execute(f'''
            CREATE TABLE IF NOT EXISTS ais_{month}_msg_18 (
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


def create_table_msg27(cur, month):
    cur.execute(f'''
            CREATE TABLE IF NOT EXISTS ais_{month}_msg_27 (
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

insertfcn = {
        'msg1' : insert_msg123,
        'msg2' : insert_msg123,
        'msg3' : insert_msg123,
        'msg5' : insert_msg5,
        'msg18' : insert_msg18,
        #'msg19' : ,
        'msg24' : insert_msg24,
        #'msg27' : insert_msg123,
    }
createfcn = {
        'msg1' : sqlite_create_table_msg123,
        #'msg2' : sqlite_create_table_msg123,
        #'msg3' : sqlite_create_table_msg123,
        'msg5' : create_table_msg5,
        'msg18' : sqlite_create_table_msg18,
        'msg24' : create_table_msg24,
    }
