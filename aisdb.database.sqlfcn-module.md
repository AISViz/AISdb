# aisdb.database.sqlfcn module

pass these functions to DBQuery.gen\_qry() as the function argument

aisdb.database.sqlfcn.crawl\_dynamic(_\*_, _months_, _callback_, _\*\*kwargs_)[\[source\]](about:blank/\_modules/aisdb/database/sqlfcn.html#crawl\_dynamic)

iterate over position reports tables to create SQL query spanning desired time range

this function should be passed as a callback to DBQuery.gen\_qry(), and should not be called directly

aisdb.database.sqlfcn.crawl\_dynamic\_static(_\*_, _months_, _callback_, _\*\*kwargs_)[\[source\]](about:blank/\_modules/aisdb/database/sqlfcn.html#crawl\_dynamic\_static)

iterate over position reports and static messages tables to create SQL query spanning desired time range

this function should be passed as a callback to DBQuery.gen\_qry(), and should not be called directly
