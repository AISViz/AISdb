# Python standard library packages
import asyncio
import os
import sys
from datetime import datetime, timedelta

# These packages need to be installed with pip
import orjson
import websockets.client

# Query the MERIDIAN web API, or reconfigure the URL with an environment variable
db_hostname = 'wss://aisdb.meridian.cs.dal.ca/ws'
db_hostname = os.environ.get('AISDBHOST', db_hostname)

# Default docker IPv6 address and port number.
# See docker-compose.yml for configuration.
# db_hostname = 'ws://[fc00::6]:9924'


class DatabaseRequest():
    ''' Methods in this class are used to generate JSON-formatted AIS requests,
        as an interface to the AISDB WebSocket API.
        The orjson library is used for fast utf8-encoded JSON serialization.
    '''

    def validrange() -> bytes:
        return orjson.dumps({'msgtype': 'validrange'})

    def zones() -> bytes:
        return orjson.dumps({'msgtype': 'zones'})

    def track_vectors(x0: float, y0: float, x1: float, y1: float,
                      start: datetime, end: datetime) -> dict:
        ''' database query for a given time range and bounding box '''
        return orjson.dumps(
            {
                "msgtype": "track_vectors",
                "start": int(start.timestamp()),
                "end": int(end.timestamp()),
                "area": {
                    "x0": x0,
                    "x1": x1,
                    "y0": y0,
                    "y1": y1
                }
            },
            option=orjson.OPT_SERIALIZE_NUMPY)


async def query_valid_daterange(db_socket: websockets.client, ) -> dict:
    ''' Query the database server for minimum and maximum time range values.
        Values are formatted as unix epoch seconds, i.e. the total number of
        seconds since Jan 1 1970, 12am UTC.
    '''

    # Create a new server daterange request using a dictionary
    query = DatabaseRequest.validrange()
    await db_socket.send(query)

    # Wait for server response, and parse JSON from the response binary
    response = orjson.loads(await db_socket.recv())
    print(f'Received daterange response from server: {response}')

    # Print the server response
    start = datetime.fromtimestamp(response['start'])
    end = datetime.fromtimestamp(response['end'])

    return {'start': start, 'end': end}


async def query_tracks_24h(db_socket: websockets.client, ):
    ''' query recent ship movements near Dalhousie '''

    boundary = {'x0': -64.8131, 'x1': -62.2928, 'y0': 43.5686, 'y1': 45.3673}
    query = DatabaseRequest.track_vectors(
        start=datetime.now() - timedelta(hours=24),
        end=datetime.now(),
        **boundary,
    )
    await db_socket.send(query)

    response = orjson.loads(await db_socket.recv())
    while response['msgtype'] == 'track_vector':
        print(f'got track vector data:\n\t{response}')
        response = orjson.loads(await db_socket.recv())
    print(response, end='\n\n\n')


async def query_zones(db_socket: websockets.client, ):
    await db_socket.send(DatabaseRequest.zones())

    response = orjson.loads(await db_socket.recv())
    while response['msgtype'] == 'zone':
        print(f'got zone polygon data:\n\t{response}')
        response = orjson.loads(await db_socket.recv())
    print(response, end='\n\n\n')


async def main():
    ''' asynchronously query the web API for valid timerange, 24 hours of
        vectorized vessel data, and zone polygons
    '''
    useragent = 'AISDB WebSocket Client'
    useragent += f' ({os.name} {sys.implementation.cache_tag})'

    async with websockets.client.connect(
            db_hostname, user_agent_header=useragent) as db_socket:
        daterange = await query_valid_daterange(db_socket)
        print(
            f'start={daterange["start"].isoformat()}\t'
            f'end={daterange["end"].isoformat()}',
            end='\n\n\n')

        await query_tracks_24h(db_socket)

        await query_zones(db_socket)


if __name__ == '__main__':
    asyncio.run(main())
