import os


def dynamic(month, callback, kwargs):
    ''' SQL common table expression for selecting from dynamic tables '''
    sqlfile = 'cte_dynamic_clusteredidx.sql'
    with open(os.path.join('aisdb_sql', sqlfile), 'r') as f:
        sql = f.read()
    args = [month for _ in range(len(sql.split('{}')) - 1)]
    return sql.format(*args) + callback(month=month, alias='d', **kwargs)


def static(month='197001', **_):
    ''' SQL common table expression for selecting from static tables '''
    sqlfile = 'cte_static_aggregate.sql'
    with open(os.path.join('aisdb_sql', sqlfile), 'r') as f:
        sql = f.read()
    args = [month for _ in range(len(sql.split('{}')) - 1)]
    return sql.format(*args)


def leftjoin(month='197001'):
    ''' SQL select statement using common table expressions.
        Joins columns from dynamic, static, and coarsetype_ref tables.
    '''
    sqlfile = 'select_join_dynamic_static_clusteredidx.sql'
    with open(os.path.join('aisdb_sql', sqlfile), 'r') as f:
        sql = f.read()
    args = [month for _ in range(len(sql.split('{}')) - 1)]
    return sql.format(*args)


def aliases(month, callback, kwargs):
    ''' declare common table expression aliases '''
    sqlfile = 'cte_aliases.sql'
    with open(os.path.join('aisdb_sql', sqlfile), 'r') as f:
        sql = f.read()
    args = (month, dynamic(month, callback, kwargs), month, static(month))
    return sql.format(*args)


def crawl(months, callback, **kwargs):
    ''' iterate over tables to create SQL query spanning desired time range '''
    sqlfile = 'cte_coarsetype.sql'
    with open(os.path.join('aisdb_sql', sqlfile), 'r') as f:
        sql_coarsetype = f.read()
    sql_aliases = ''.join([
        aliases(month=month, callback=callback, kwargs=kwargs)
        for month in months
    ])
    sql_union = '\nUNION\n'.join([leftjoin(month=month) for month in months])
    sql_qry = f'WITH\n{sql_aliases}\n{sql_coarsetype}\n{sql_union}'
    return sql_qry
