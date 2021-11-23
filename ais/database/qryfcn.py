from database.qryfcn_legacy import *


# query a union of position reports using rtree indexes
rtree_dynamic = lambda month, callback, kwargs: (f'''
    SELECT CAST(m123.mmsi0 AS INT) as mmsi, m123.t0, m123.x0, m123.y0, m123.cog, m123.sog, m123.msgtype
      FROM rtree_{month}_msg_1_2_3 AS m123
      WHERE {callback(month=month, alias='m123', **kwargs)}
    UNION
    SELECT CAST(m18.mmsi0 AS INT) as mmsi, m18.t0, m18.x0, m18.y0, m18.cog, m18.sog, m18.msgtype
      FROM rtree_{month}_msg_18 AS m18
      WHERE {callback(month=month, alias='m18', **kwargs)} ''')


# query static vessel data from monthly aggregate tables
static = lambda month, **_: (f'''
    SELECT mmsi, vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star, imo FROM static_{month}_aggregate ''')


# declare common table expressions for use in SQL 'WITH' statements
aliases = lambda month, callback, kwargs: (f'''
dynamic_{month} AS ( {rtree_dynamic(month, callback, kwargs)} 
),
static_{month} AS ( {static(month)} 
)''')


# common table expression SELECT statements for concatenation with UNION
leftjoin = lambda month: (f'''
SELECT dynamic_{month}.mmsi, dynamic_{month}.t0, 
            dynamic_{month}.x0, dynamic_{month}.y0, 
            dynamic_{month}.cog, dynamic_{month}.sog, 
            dynamic_{month}.msgtype, 
            static_{month}.imo, static_{month}.vessel_name,
            static_{month}.dim_bow, static_{month}.dim_stern, 
            static_{month}.dim_port, static_{month}.dim_star,
            static_{month}.ship_type, ref.coarse_type_txt 
        FROM dynamic_{month} 
    LEFT JOIN static_{month}
        ON dynamic_{month}.mmsi = static_{month}.mmsi
    LEFT JOIN coarsetype_ref AS ref 
        ON (static_{month}.ship_type = ref.coarse_type) ''')


# iterate over monthly tables to create an SQL query spanning desired time range
crawl = lambda months, callback, **kwargs: (
        'WITH'
        + ','.join([aliases(month=month, callback=callback, kwargs=kwargs) for month in months])
        + '\n' 
        + '\nUNION'.join([leftjoin(month=month) for month in months])
        + '\nORDER BY 1, 2'
    )


