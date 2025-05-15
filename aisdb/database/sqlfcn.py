''' pass these functions to DBQuery.gen_qry() as the function argument '''
import os

from aisdb import sqlpath

# with open(os.path.join(sqlpath, 'cte_dynamic_clusteredidx.sql'), 'r') as f:
#     sql_dynamic = f.read()

# with open(os.path.join(sqlpath, 'cte_static_aggregate.sql'), 'r') as f:
#     sql_static = f.read()

# with open(os.path.join(sqlpath, 'select_join_dynamic_static_clusteredidx.sql'),
#           'r') as f:
#     sql_leftjoin = f.read()

# with open(os.path.join(sqlpath, 'cte_aliases.sql'), 'r') as f:
#     sql_aliases = f.read()

def load_sql(name: str, dbtype: str = 'sqlite') -> str:
    """Load the correct SQL template based on dbtype (sqlite or postgresql)."""
    suffix = '_global' if dbtype == 'postgresql' else ''
    path = os.path.join(sqlpath, f'{name}{suffix}.sql')
    with open(path, 'r') as f:
        return f.read()


def _dynamic(*, month, callback, dbtype, **kwargs):
    ''' SQL common table expression for selecting from dynamic tables '''
    sql_template = load_sql('cte_dynamic_clusteredidx', dbtype)
    args = [month] * sql_template.count('{}')
    sql = sql_template.format(*args)
    sql += callback(month=month if dbtype == 'sqlite' else None, alias='d', **kwargs)
    return sql


def _static(*, month='197001', dbtype='sqlite', **_):
    """CTE for static tables."""
    sql_template = load_sql('cte_static_aggregate', dbtype)
    args = [month] * sql_template.count('{}')
    return sql_template.format(*args)


def _leftjoin(*, month='197001', dbtype='sqlite'):
    ''' SQL select statement using common table expressions.
        Joins columns from dynamic, static, and coarsetype_ref tables.
    '''
    sql_template = load_sql('select_join_dynamic_static_clusteredidx', dbtype)
    args = [month] * sql_template.count('{}')
    return sql_template.format(*args)


def _aliases(*, month, callback, kwargs, dbtype='sqlite'):
    ''' declare common table expression aliases '''
    dyn_sql = _dynamic(month=month, callback=callback, dbtype=dbtype, **kwargs)
    stat_sql = _static(month=month, dbtype=dbtype)
    sql_template = load_sql('cte_aliases', dbtype)

    if dbtype == 'postgresql':
        # Template has only 2 placeholders for SQL fragments
        return sql_template.format(dyn_sql, stat_sql)
    else:
        # Template has 4 placeholders (e.g., dynamic_{}, static_{})
        return sql_template.format(month, dyn_sql, month, stat_sql)

def crawl_dynamic(*, months=None, callback, dbtype='sqlite', **kwargs):
    ''' iterate over position reports tables to create SQL query spanning
        desired time range

        this function should be passed as a callback to DBQuery.gen_qry(),
        and should not be called directly
    '''
    if dbtype=='postgresql':
        sql = _dynamic(month='global', callback=callback, dbtype=dbtype, **kwargs)
        return sql + '\nORDER BY 1,2'
    else:
        sql_dynamic = '\nUNION\n'.join([
            _dynamic(month=month, callback=callback, dbtype=dbtype, **kwargs) for month in months
        ]) + '\nORDER BY 1,2'
        return sql_dynamic


def crawl_dynamic_static(*, months=None, callback, dbtype='sqlite', **kwargs):
    ''' iterate over position reports and static messages tables to create SQL
        query spanning desired time range

        this function should be passed as a callback to DBQuery.gen_qry(),
        and should not be called directly
    '''
    sqlfile = 'cte_coarsetype.sql'
    with open(os.path.join(sqlpath, sqlfile), 'r') as f:
        sql_coarsetype = f.read()
    
    if dbtype=='postgresql':
        sql_alias = _aliases(month='global', callback=callback, kwargs=kwargs, dbtype=dbtype)
        sql_union = _leftjoin(month='global', dbtype=dbtype)
        return f'WITH\n{sql_alias},\n{sql_coarsetype}\n{sql_union}\nORDER BY 1,2'
    elif dbtype == 'sqlite':
        if not months:
            raise ValueError("'months' must be provided when using SQLite backend.")

        sql_aliases = ''.join([
            _aliases(month=month, callback=callback, kwargs=kwargs, dbtype=dbtype)
            for month in months
        ])
        sql_union = '\nUNION\n'.join([_leftjoin(month=month, dbtype=dbtype) for month in months])
        sql_qry = f'WITH\n{sql_aliases}\n{sql_coarsetype}\n{sql_union}'
        sql_qry += ' ORDER BY 1,2'
        return sql_qry
