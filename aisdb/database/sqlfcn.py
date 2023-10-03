''' pass these functions to DBQuery.gen_qry() as the function argument '''
import os

from aisdb import sqlpath

with open(os.path.join(sqlpath, 'cte_dynamic_clusteredidx.sql'), 'r') as f:
    sql_dynamic = f.read()

with open(os.path.join(sqlpath, 'cte_static_aggregate.sql'), 'r') as f:
    sql_static = f.read()

with open(os.path.join(sqlpath, 'select_join_dynamic_static_clusteredidx.sql'),
          'r') as f:
    sql_leftjoin = f.read()

with open(os.path.join(sqlpath, 'cte_aliases.sql'), 'r') as f:
    sql_aliases = f.read()


def _dynamic(*, month, callback, **kwargs):
    ''' SQL common table expression for selecting from dynamic tables '''
    args = [month for _ in range(len(sql_dynamic.split('{}')) - 1)]
    sql = sql_dynamic.format(*args)
    sql += callback(month=month, alias='d', **kwargs)
    return sql


def _static(*, month='197001', **_):
    ''' SQL common table expression for selecting from static tables '''
    args = [month for _ in range(len(sql_static.split('{}')) - 1)]
    return sql_static.format(*args)


def _leftjoin(month='197001'):
    ''' SQL select statement using common table expressions.
        Joins columns from dynamic, static, and coarsetype_ref tables.
    '''
    args = [month for _ in range(len(sql_leftjoin.split('{}')) - 1)]
    return sql_leftjoin.format(*args)


def _aliases(*, month, callback, kwargs):
    ''' declare common table expression aliases '''
    args = (month, _dynamic(month=month, callback=callback,
                            **kwargs), month, _static(month=month))
    return sql_aliases.format(*args)


def crawl_dynamic(*, months, callback, **kwargs):
    ''' iterate over position reports tables to create SQL query spanning
        desired time range

        this function should be passed as a callback to DBQuery.gen_qry(),
        and should not be called directly
    '''
    sql_dynamic = '\nUNION\n'.join([
        _dynamic(month=month, callback=callback, **kwargs) for month in months
    ]) + '\nORDER BY 1,2'
    return sql_dynamic


def crawl_dynamic_static(*, months, callback, **kwargs):
    ''' iterate over position reports and static messages tables to create SQL
        query spanning desired time range

        this function should be passed as a callback to DBQuery.gen_qry(),
        and should not be called directly
    '''
    sqlfile = 'cte_coarsetype.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql_coarsetype = f.read()
    sql_aliases = ''.join([
        _aliases(month=month, callback=callback, kwargs=kwargs)
        for month in months
    ])
    sql_union = '\nUNION\n'.join([_leftjoin(month=month) for month in months])
    sql_qry = f'WITH\n{sql_aliases}\n{sql_coarsetype}\n{sql_union}'
    sql_qry += ' ORDER BY 1,2'
    return sql_qry
