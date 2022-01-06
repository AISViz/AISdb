# query position reports using rtree indexes
dynamic = lambda month, callback, kwargs: (f'''
    SELECT CAST(d.mmsi AS INT) as d.mmsi, d.time, d.longitude, d.latitude, d.cog, d.sog
      FROM ais_{month}_dynamic AS d
      WHERE {callback(month=month, alias='d', **kwargs)}
''')

# query static vessel data from monthly aggregate tables
static = lambda month, **_: (f'''
    SELECT mmsi, vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star, imo FROM static_{month}_aggregate ''')

# declare common table expressions for use in SQL 'WITH' statements
aliases = lambda month, callback, kwargs: (f'''
dynamic_{month} AS ( {dynamic(month, callback, kwargs)}
),
static_{month} AS ( {static(month)}
)''')

# iterate over monthly tables to create an SQL query spanning desired time range
crawl = lambda months, callback, **kwargs: (
        'WITH'
        + ','.join([aliases(month=month, callback=callback, kwargs=kwargs) for month in months])
        + '\n'
        + '\nUNION'.join([leftjoin(month=month) for month in months])
        + '\nORDER BY 1, 2'
    )
