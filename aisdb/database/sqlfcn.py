''' pass these functions to DBQuery.gen_qry() as the function argument '''
import os

from aisdb import sqlpath


def _dynamic(month, callback, **kwargs):
    ''' SQL common table expression for selecting from dynamic tables '''
    sqlfile = 'cte_dynamic_clusteredidx.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql = f.read()
    args = [month for _ in range(len(sql.split('{}')) - 1)]
    return sql.format(*args) + callback(month=month, alias='d', **kwargs)


def _static(month='197001', **_):
    ''' SQL common table expression for selecting from static tables '''
    sqlfile = 'cte_static_aggregate.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql = f.read()
    args = [month for _ in range(len(sql.split('{}')) - 1)]
    return sql.format(*args)


def _leftjoin(month='197001'):
    ''' SQL select statement using common table expressions.
        Joins columns from dynamic, static, and coarsetype_ref tables.
    '''
    sqlfile = 'select_join_dynamic_static_clusteredidx.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql = f.read()
    args = [month for _ in range(len(sql.split('{}')) - 1)]
    return sql.format(*args)


def _aliases(month, callback, kwargs):
    ''' declare common table expression aliases '''
    sqlfile = 'cte_aliases.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql = f.read()
    args = (month, _dynamic(month, callback, **kwargs), month, _static(month))
    return sql.format(*args)


def crawl_dynamic(months, callback, **kwargs):
    ''' iterate over position reports tables to create SQL query spanning
        desired time range

        this function should be passed as a callback to DBQuery.gen_qry(),
        and should not be called directly
    '''
    sql_dynamic = '\nUNION\n'.join([
        _dynamic(month=month, callback=callback, **kwargs) for month in months
    ]) + '\nORDER BY 1,2'
    return sql_dynamic


def crawl_dynamic_static(months, callback, **kwargs):
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
    sql_qry = f'WITH\n{sql_aliases}\n{sql_coarsetype}\n{sql_union}' + 'ORDER BY 1,2'
    return sql_qry
