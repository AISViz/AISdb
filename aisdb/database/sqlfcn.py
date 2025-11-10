''' pass these functions to DBQuery.gen_qry() as the function argument '''
import os

from aisdb import sqlpath

def load_sql(name: str, dbtype: str = 'postgresql') -> str:
    suffix = '_global'
    path = os.path.join(sqlpath, f'{name}{suffix}.sql')
    with open(path, 'r') as f:
        return f.read()


def _dynamic(*, callback, dbtype='postgresql', **kwargs):
    ''' SQL common table expression for selecting from dynamic tables '''
    sql_template = load_sql('cte_dynamic_clusteredidx', dbtype)
    sql = sql_template.format('global')
    sql += callback(month=None, alias='d', **kwargs)
    return sql

def _static(*, dbtype='postgresql', **_):
    """CTE for static tables."""
    sql_template = load_sql('cte_static_aggregate', dbtype)
    return sql_template.format('global')


def _leftjoin(*, dbtype='postgresql'):
    ''' SQL select statement using common table expressions.
        Joins columns from dynamic, static, and coarsetype_ref tables.
    '''
    sql_template = load_sql('select_join_dynamic_static_clusteredidx', dbtype)
    return sql_template.format('global')


def _aliases(*, callback, kwargs, dbtype='postgresql'):
    ''' declare common table expression aliases '''
    dyn_sql = _dynamic(callback=callback, dbtype=dbtype, **kwargs)
    stat_sql = _static(dbtype=dbtype)
    sql_template = load_sql('cte_aliases', dbtype)
    return sql_template.format(dyn_sql, stat_sql)
   

def crawl_dynamic(*, callback, dbtype='postgresql', **kwargs):
    ''' iterate over position reports tables to create SQL query spanning
        desired time range

        this function should be passed as a callback to DBQuery.gen_qry(),
        and should not be called directly
    '''
    sql = _dynamic(callback=callback, dbtype=dbtype, **kwargs)
    return sql + '\nORDER BY 1,2'


def crawl_dynamic_static(*, callback, dbtype='postgresql', **kwargs):
    ''' iterate over position reports and static messages tables to create SQL
        query spanning desired time range

        this function should be passed as a callback to DBQuery.gen_qry(),
        and should not be called directly
    '''
    sqlfile = 'cte_coarsetype.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql_coarsetype = f.read()

    sql_alias = _aliases(callback=callback, kwargs=kwargs, dbtype=dbtype)
    sql_union = _leftjoin(dbtype=dbtype)
    return f'WITH\n{sql_alias},\n{sql_coarsetype}\n{sql_union}\nORDER BY 1,2'
