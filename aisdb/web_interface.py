import asyncio
import webbrowser
import logging
from datetime import datetime
from functools import partial

import orjson
import websockets.server

logging.getLogger("websockets").setLevel(logging.WARNING)


def serialize_track_json(track):
    vector = {
        'msgtype': 'track_vector',
        # currently, database_server sends all metadata to be strings
        # reproduce this behaviour by coercion to string type, even for numbers
        'meta': {
            'mmsi': str(track['mmsi'])
        },
        't': track['time'],
        'x': track['lon'],
        'y': track['lat'],
    }

    meta = {k: track[k] for k in track['static'] if k != 'marinetraffic_info'}
    meta['msgtype'] = 'vesselinfo'

    if 'marinetraffic_info' in track.keys():
        meta.update({
            k: track['marinetraffic_info'][k]
            for k in track['marinetraffic_info'].keys()
        })

    vector_json = orjson.dumps(vector, option=orjson.OPT_SERIALIZE_NUMPY)
    meta_json = orjson.dumps(meta)
    return (vector_json, meta_json)


async def send_tracks(websocket, tracks_json):
    ''' send tracks serialized as JSON to the websocket client '''
    done = {}
    async for message_json in websocket:
        message = orjson.loads(message_json)

        if message == {"msgtype": "validrange"}:
            now = datetime.now().timestamp()
            validrange = {"msgtype": "validrange", "start": now, "end": now}
            await websocket.send(orjson.dumps(validrange))
            done['validrange'] = True

        elif message == {"msgtype": "zones"}:
            await websocket.send(b'{"msgtype": "doneZones"}')
            done['zones'] = True
        elif message == {"msgtype": "meta"}:
            done['meta'] = True
        else:
            raise RuntimeError(f'unknown request {message_json}')

        if 'validrange' in done.keys() and 'zones' in done.keys():
            assert len(done.keys()) == 2
            for (vector_json, meta_json) in tracks_json:
                await websocket.send(vector_json)

        elif 'meta' in done.keys():
            assert len(done.keys()) == 1
            for (vector_json, meta_json) in tracks_json:
                await websocket.send(meta_json)


async def visualize_async(tracks_json, host='localhost', port=9924):
    ''' Display tracks in the web interface. Serves data to the web client '''
    print('Querying database...', end='\t')
    fcn = partial(send_tracks, tracks_json=list(tracks_json))
    print('done query')
    print('Opening a new browser window to display track data')
    print('Press Ctrl-C to close the webpage')
    webbrowser.open_new_tab('localhost:3000/?python=1&z=2')
    async with websockets.server.serve(fcn, host, port):
        await asyncio.Future()


def visualize(tracks, host='localhost', port=9924):
    ''' Synchronous wrapper for visualize_async().
        Display tracks in the web interface
    '''
    asyncio.run(visualize_async(map(serialize_track_json, tracks), host, port))
