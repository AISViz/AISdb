import os

from aisdb import sqlpath

with open(os.path.join(sqlpath, 'createtable_dynamic_clustered.sql'),
          'r') as f:
    sql_createtable_dynamic = f.read()

with open(os.path.join(sqlpath, 'createtable_static.sql'), 'r') as f:
    sql_createtable_static = f.read()

with open(os.path.join(sqlpath, 'createtable_static_aggregate.sql'), 'r') as f:
    sql_aggregate = f.read()
