import asyncio
import logging
import os
import subprocess
import sys
import webbrowser
from datetime import datetime
from functools import partial, reduce

import orjson
import websockets.server

from aisdb import wwwpath, wwwpath_alt

logging.getLogger("websockets").setLevel(logging.WARNING)


def start_webapp(visualearth=False):
    if not visualearth:
        return subprocess.Popen(
            [sys.executable, '-m', 'http.server', '-d', wwwpath, '3000'])
    else:
        return subprocess.Popen(
            [sys.executable, '-m', 'http.server', '-d', wwwpath_alt, '3000'])


def serialize_track_json(track):
    ''' serializes a single track dictionary to JSON format encoded as UTF8 '''
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


async def _send_tracks(websocket, tracks_json):
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


async def _visualize_async(tracks_json):
    ''' Display tracks in the web interface. Serves data to the web client '''
    print('Querying database...', end='\t')
    fcn = partial(_send_tracks, tracks_json=list(tracks_json))
    print('done query')
    print('Opening a new browser window to display track data')
    print('Press Ctrl-C to close the webpage')
    webbrowser.open_new_tab('localhost:3000/?python=1&z=2')
    async with websockets.server.serve(fcn, 'localhost', 9924):
        await asyncio.Future()


def visualize(tracks, visualearth=False):
    ''' Synchronous wrapper for visualize_async().
        Tracks input to this function will be converted to JSON automatically.
        Display tracks in the web interface
    '''
    app = start_webapp(visualearth)

    try:
        asyncio.run(_visualize_async(map(serialize_track_json, tracks)))
    finally:
        app.terminate()
