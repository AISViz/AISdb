# query position reports using rtree indexes
dynamic = lambda month, callback, kwargs: (f'''
    SELECT d.mmsi, d.time, d.longitude, d.latitude
        --, d.cog, d.sog
      FROM ais_{month}_dynamic AS d
      WHERE {callback(month=month, alias='d', **kwargs)}''')

# query static vessel data from monthly aggregate tables
static = lambda month, **_: (f'''
    SELECT mmsi, vessel_name, ship_type, dim_bow,
            dim_stern, dim_port, dim_star, imo
        FROM static_{month}_aggregate''')

# common table expression SELECT statements for concatenation with UNION
leftjoin = lambda month: (f'''
SELECT dynamic_{month}.mmsi, dynamic_{month}.time,
            dynamic_{month}.longitude, dynamic_{month}.latitude,
            --dynamic_{month}.cog, dynamic_{month}.sog,
            static_{month}.imo, static_{month}.vessel_name,
            static_{month}.dim_bow, static_{month}.dim_stern,
            static_{month}.dim_port, static_{month}.dim_star,
            static_{month}.ship_type
        FROM dynamic_{month}
    LEFT JOIN static_{month}
        ON dynamic_{month}.mmsi = static_{month}.mmsi ''')

# declare common table expressions for use in SQL 'WITH' statements
aliases = lambda month, callback, kwargs: (f'''
dynamic_{month} AS ( {dynamic(month, callback, kwargs)}
),
static_{month} AS ( {static(month)}
)
''')

# query position reports using rtree indexes
testfcn = lambda month, callback, kwargs: (f'''
    SELECT d.mmsi, d.time, d.longitude, d.latitude
        --, d.cog, d.sog
      FROM ais_{month}_dynamic AS d
      WHERE {callback(month=month, alias='d', **kwargs)}''')

# iterate over monthly tables to create an SQL query spanning desired time range
crawl = lambda months, callback, **kwargs: ('WITH' + ','.join([
    aliases(month=month, callback=callback, kwargs=kwargs) for month in months
]) + '\nUNION'.join([leftjoin(month=month)
                     for month in months]) + '\nORDER BY 1, 2')
