from aisdb.websocket_server import SocketServ
from aisdb.webdata.marinetraffic import VesselInfo

import asyncio
import os
import subprocess

# db server options
dbpath = os.environ.get('AISDBPATH', 'AIS.sqlitedb')
metadata = os.environ.get('TRAFFICDBPATH', 'metadata.db')
domain = None
vinfo = VesselInfo(metadata)

# js client options
os.environ['VITE_DISABLE_SSL'] = '1'
os.environ['VITE_NO_DB_LIMIT'] = '1'
os.environ['VITE_AISDBHOST'] = 'localhost'
os.environ['VITE_AISDBPORT'] = '9924'
os.environ['VITE_TILESERVER'] = 'aisdb.meridian.cs.dal.ca'
os.environ['VITE_BINGMAPTILES'] = '1'
args = ['npx', 'vite', '--port', '3000', 'aisdb_web/map']

# start client in a background thread, and run database server
try:
    front_end = subprocess.Popen(args, env=os.environ)
    database_server = SocketServ(dbpath, domain, metadata)
    db_handle = asyncio.run(database_server.main())
finally:
    front_end.kill()
