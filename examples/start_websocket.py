import asyncio
from os import environ

from aisdb.websocket_server import SocketServ
from aisdb import DomainFromPoints
#from aisdb import DomainFromTxts

from dotenv import load_dotenv

load_dotenv()

dbpath = environ.get('AISDBPATH', '/home/ais_env/ais/ais.db')
trafficDBpath = environ.get('AISDBMARINETRAFFIC',
                            '/home/ais_env/ais/marinetraffic.db')

# zone geometry from a point and minimum radius
domain = DomainFromPoints(
    points=[(-63.5533, 44.4686), (-63.5872, 44.6374)],
    radial_distances=[100, 100],
    names=['AIS Station 2: NRC', 'AIS Station 1: Dalhousie'],
    domainName='Halifax',
)

serv = SocketServ(dbpath=dbpath, domain=domain, trafficDBpath=trafficDBpath)
asyncio.run(serv.main())
