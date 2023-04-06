# Python standard library packages
from datetime import datetime, timedelta
import asyncio
import io

# These packages need to be installed with pip
from websockets import client
import aisdb
import orjson  # fast JSON serialization

# Database server IPv6 address and port number.
# See docker-compose.yml or docker-compose.override.yml for
# configuration
db_hostname = 'ws://[fc00::6]:9924'


class DatabaseRequest():
    ''' Methods in this class are used to generate JSON-formatted AIS requests,
        as an interface to the AISDB WebSocket API
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


async def query_valid_daterange(
    db_socket: client,
    response_buffer: io.BytesIO,
) -> dict:
    ''' Query the database server for minimum and maximum time range values.
        Values are formatted as unix epoch seconds, i.e. the total number of
        seconds since Jan 1 1970, 12am UTC.
    '''

    # Create a new server daterange request using a dictionary
    query = DatabaseRequest.validrange()
    await db_socket.send(query)

    # Wait for server response, and save response data in a bytes buffer
    response_buffer.write(await db_socket.recv())
    response_buffer.flush()

    # Parse JSON from the response binary data
    response = orjson.loads(response_buffer.getvalue())
    print(f'Received daterange response from server: {response}')

    # Print the server response
    start = datetime.fromtimestamp(response['start'])
    end = datetime.fromtimestamp(response['end'])
    response_buffer.truncate()

    return {'start': start, 'end': end}


async def query_tracks_24h(
    db_socket: client,
    response_buffer: io.BytesIO,
):
    ''' query recent ship movements near Dalhousie '''

    boundary = aisdb.gis.radial_coordinate_boundary(
        x=-63.553,
        y=44.468,
        radius=100000,
    )
    query = DatabaseRequest.track_vectors(
        x0=boundary['xmin'],
        x1=boundary['xmax'],
        y0=boundary['ymin'],
        y1=boundary['ymax'],
        start=datetime.now() - timedelta(hours=24),
        end=datetime.now(),
    )
    await db_socket.send(query)

    response = orjson.loads(await db_socket.recv())
    while response['msgtype'] == 'track_vector':
        print(f'got track vector data: {response}')
        response = orjson.loads(await db_socket.recv())

    print(response['status'])


async def main():
    async with client.connect(
            db_hostname,
            user_agent_header='AISDB WebSocket Client') as db_socket:

        # server responses will be stored inside this memory buffer
        response_buffer = io.BytesIO()

        # query the timerange of data in the database
        daterange = await query_valid_daterange(db_socket, response_buffer)

        print(f'start={daterange["start"].isoformat()}\t'
              f'end={daterange["end"].isoformat()}')

        await query_tracks_24h(db_socket, response_buffer)


assert __name__ == '__main__'
asyncio.run(main())
