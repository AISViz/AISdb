import asyncio
from aisdb.websocket_server import SocketServ
from aisdb import DomainFromTxts

domain = DomainFromTxts(
    domainName='example',
    folder='/RAID0/ais/zones',
    correct_coordinate_range=False,
)

serv = SocketServ(
    dbpath='/RAID0/ais/DFO_2021_vacuumed.db',
    domain=domain,
    trafficDBpath='/RAID0/ais/marinetraffic_V2.db',
    enable_ssl=False,
)

asyncio.run(serv.main())
