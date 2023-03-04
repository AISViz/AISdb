''' pass these functions to DBQuery.gen_qry() as the function argument '''
import os

from aisdb import sqlpath
from aisdb.database.dbconn import DBConn


def _dynamic(*, dbpath, month, callback, **kwargs):
    ''' SQL common table expression for selecting from dynamic tables '''
    sqlfile = 'cte_dynamic_clusteredidx.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql = f.read()
    args = [month for _ in range(len(sql.split('{}')) - 1)]
    return sql.format(*args).replace(
        f'ais_{month}_dynamic',
        f'{DBConn._get_dbname(None, dbpath)}.ais_{month}_dynamic') + callback(
            month=month, alias='d', **kwargs)
    return sql.format(*args)


def _static(*, dbpath, month='197001', **_):
    ''' SQL common table expression for selecting from static tables '''
    sqlfile = 'cte_static_aggregate.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql = f.read()
    args = [month for _ in range(len(sql.split('{}')) - 1)]
    return sql.format(*args).replace(
        f'static_{month}_aggregate',
        f'{DBConn._get_dbname(None, dbpath)}.static_{month}_aggregate')


def _leftjoin(month='197001'):
    ''' SQL select statement using common table expressions.
        Joins columns from dynamic, static, and coarsetype_ref tables.
    '''
    sqlfile = 'select_join_dynamic_static_clusteredidx.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql = f.read()
    args = [month for _ in range(len(sql.split('{}')) - 1)]
    return sql.format(*args)


def _aliases(*, dbpath, month, callback, kwargs):
    ''' declare common table expression aliases '''
    sqlfile = 'cte_aliases.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql = f.read()
    args = (month,
            _dynamic(dbpath=dbpath, month=month, callback=callback,
                     **kwargs), month, _static(dbpath=dbpath, month=month))
    return sql.format(*args)


def crawl_dynamic(*, dbpath, months, callback, **kwargs):
    ''' iterate over position reports tables to create SQL query spanning
        desired time range

        this function should be passed as a callback to DBQuery.gen_qry(),
        and should not be called directly
    '''
    sql_dynamic = '\nUNION\n'.join([
        _dynamic(dbpath=dbpath, month=month, callback=callback, **kwargs)
        for month in months
    ]) + '\nORDER BY 1,2'
    return sql_dynamic


def crawl_dynamic_static(*, dbpath, months, callback, **kwargs):
    ''' iterate over position reports and static messages tables to create SQL
        query spanning desired time range

        this function should be passed as a callback to DBQuery.gen_qry(),
        and should not be called directly
    '''
    sqlfile = 'cte_coarsetype.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql_coarsetype = f.read()
    sql_aliases = ''.join([
        _aliases(dbpath=dbpath, month=month, callback=callback, kwargs=kwargs)
        for month in months
    ])
    sql_union = '\nUNION\n'.join([_leftjoin(month=month) for month in months])
    sql_qry = f'WITH\n{sql_aliases}\n{sql_coarsetype}\n{sql_union}' + 'ORDER BY 1,2'
    return sql_qry
