import os


def create_table_coarsetype(cur):
    ''' static tables are created during db instantiation 
        included here instead of create_tables.py to prevent circular import error
    ''' 

    cur.execute(''' CREATE TABLE IF NOT EXISTS coarsetype_ref (
            coarse_type integer,
            coarse_type_txt character varying(75)
        ); ''')

    cur.execute(''' CREATE UNIQUE INDEX idx_coarsetype ON 'coarsetype_ref' (coarse_type)''')

    cur.executemany (''' INSERT OR IGNORE INTO coarsetype_ref (coarse_type, coarse_type_txt) VALUES (?,?) ''', (
        (20,	'Wing in ground craft'),
        (30,	'Fishing'),
        (31,	'Towing'),
        (32,	'Towing - length >200m or breadth >25m'),
        (33,	'Engaged in dredging or underwater operations'),
        (34,	'Engaged in diving operations'),
        (35,	'Engaged in military operations'),
        (36,	'Sailing'),
        (37,	'Pleasure craft'),
        (38,	'Reserved for future use'),
        (39,	'Reserved for future use'),
        (40,	'High speed craft'),
        (50,	'Pilot vessel'),
        (51,	'Search and rescue vessels'),
        (52,	'Tugs'),
        (53,	'Port tenders'),
        (54,	'Vessels with anti-pollution facilities or equipment'),
        (55,	'Law enforcement vessels'),
        (56,	'Spare for assignments to local vessels'),
        (57,	'Spare for assignments to local vessels'),
        (58,	'Medical transports (1949 Geneva convention)'),
        (59,	'Ships and aircraft of States not parties to an armed conflict'),
        (60,	'Passenger ships'),
        (70,	'Cargo ships'),
        (80,	'Tankers'),
        (90,	'Other types of ship'),
        (100,	'Unknown'),
        )
    )

class dbconn():
    def __init__(self, dbpath=None, postgres=False, timeout=5):
        if postgres or os.environ.get('POSTGRESDB'):
            import psycopg2 
            import psycopg2.extras
            if __name__ == '__main__':
                psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)  # enable interrupt
            conn = psycopg2.connect(dbname='ee_ais', user=os.environ.get('PGUSER'), port=os.environ.get('PGPORT'), password=os.environ.get('PGPASS'), host='localhost')
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.conn, self.cur = conn, cur
            self.lambdas = dict(
                in_poly     = lambda poly,alias='m123',**_: f'ST_Contains(\n    ST_GeomFromText(\'{poly}\'),\n    ST_MakePoint({alias}.longitude, {alias}.latitude)\n  )',
                in_radius   = lambda *,x,y,radius,**_: f'ST_DWithin(Geography(m123.ais_geom), Geography(ST_MakePoint({x}, {y})), {radius})',
                in_radius_time  = lambda *,x,y,radius,alias='m123',**kwargs: f'ST_DWithin(Geography({alias}.ais_geom), Geography(ST_MakePoint({x}, {y})), {radius}) AND {alias}.time BETWEEN \'{kwargs["start"].strftime("%Y-%m-%d %H:%M:%S")}\'::date AND \'{kwargs["end"].strftime("%Y-%m-%d %H:%M:%S")}\'::date',
                in_bbox     = lambda south, north, west, east,**_:    f'ais_geom && ST_MakeEnvelope({west},{south},{east},{north})',
            )
            self.dbtype = 'postgres'
            #create_table_coarsetype(self.cur)

        else:
            import sqlite3
            self.lambdas = dict(
                #in_poly = lambda poly,alias='m123',**_: f'Contains(\n    GeomFromText(\'{poly}\'),\n    MakePoint({alias}.longitude, {alias}.latitude)\n  )',
                in_poly = lambda poly,alias='m123',**_: f'ST_Contains(\n    ST_GeomFromText(\'{poly}\'),\n    MakePoint({alias}.longitude, {alias}.latitude)\n  )',
                in_radius = lambda *,x,y,radius,**_: f'Within(Geography(m123.ais_geom), Geography(MakePoint({x}, {y})), {radius})', 
                in_radius_time = lambda *,x,y,radius,alias='m123',**kwargs: f''' Within(Geography({alias}.ais_geom), Geography(MakePoint({x}, {y})), {radius}) AND {alias}.time BETWEEN \'{kwargs["start"].strftime("%Y-%m-%d %H:%M:%S")}\'::date AND \'{kwargs["end"].strftime("%Y-%m-%d %H:%M:%S")}\'::date ''',
                in_bbox = lambda south, north, west, east,**_:    f'ais_geom && MakeEnvelope({west},{south},{east},{north})'
            )
            self.dbtype = 'sqlite3'

            if dbpath is not None:
                #newdb = not os.path.isfile(dbpath)
                self.conn = sqlite3.connect(dbpath, timeout=timeout, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
                self.cur = self.conn.cursor()
                #self.conn.enable_load_extension(True)
                #self.cur.execute('SELECT load_extension("mod_spatialite.so")')
                self.cur.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="coarsetype_ref";')
                if not self.cur.fetchall():
                    create_table_coarsetype(self.cur)
                #    self.cur.execute('SELECT InitSpatialMetaDataFull(1)')
                self.conn.commit()
                #self.cur.execute('PRAGMA page_size=8192')
                #self.cur.execute('PRAGMA temp_store=2')
                #self.cur.execute('PRAGMA cache_size=10000')
                #self.cur.execute('PRAGMA synchronous=0')
                self.cur.execute('PRAGMA journal_mode=WAL')
                assert (j := self.cur.fetchall()) == [('wal',)], f'journal mode: {j}'

