import asyncio
from os import environ

from aisdb.websocket_server import SocketServ
from aisdb import DomainFromTxts

from dotenv import load_dotenv

load_dotenv()

dbpath = environ.get('AISDBPATH', '/home/ais_env/ais/ais.db')
zones_dir = environ.get('AISDBZONES', '/home/ais_env/ais/zones/')
trafficDBpath = environ.get('AISDBMARINETRAFFIC',
                            '/home/ais_env/ais/marinetraffic.db')

domain = DomainFromTxts(domainName='example', folder=zones_dir)

print(f'starting websocket\n{dbpath = }\n{zones_dir = }\n{trafficDBpath = }\n')

serv = SocketServ(dbpath=dbpath, domain=domain, trafficDBpath=trafficDBpath)
asyncio.run(serv.main())
