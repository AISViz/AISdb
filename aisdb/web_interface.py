import asyncio
import logging
import sys
import webbrowser
from datetime import datetime
from functools import partial
from subprocess import Popen
from tempfile import SpooledTemporaryFile

import orjson
import websockets.server

from aisdb import wwwpath, wwwpath_alt

logging.getLogger("websockets").setLevel(logging.WARNING)


def start_webapp(visualearth=False):
    if not visualearth:
        path = wwwpath
    else:
        path = wwwpath_alt
    return Popen([sys.executable, '-m', 'http.server', '-d', path, '3000'])


def serialize_track_json(track):
    ''' serializes a single track dictionary to JSON format encoded as UTF8 '''
    vector = {
            'msgtype': 'track_vector',
            # currently, database_server sends all metadata as strings
            # reproduce this behaviour by coercion to string type, even for int
            'meta': {'mmsi': str(track['mmsi'])},
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


async def _send_tracks(websocket, tmp_vectors, tmp_meta):
    ''' send tracks serialized as JSON to the connected websocket client '''
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
            tmp_vectors.seek(0)
            for vector_json in tmp_vectors:
                await websocket.send(vector_json)

        elif 'meta' in done.keys():
            assert len(done.keys()) == 1
            tmp_meta.seek(0)
            for meta_json in tmp_meta:
                await websocket.send(meta_json)


async def _visualize_async(tracks_json, display=True):
    ''' Display tracks in the web interface. Serves data to the web client '''
    print('Querying database...', end='\t')
    with (SpooledTemporaryFile(max_size=512*1e6, newline=b'\n') as vectors,
          SpooledTemporaryFile(max_size=512*1e6, newline=b'\n') as meta):
        for t in tracks_json:
            vectors.write(t[0])
            vectors.write(b'\n')
            meta.write(t[1])
            meta.write(b'\n')

        print('done query')

        if display:
            print('Opening a new browser window to display track data')
            print('Press Ctrl-C to close the webpage')
            url = f'http://localhost:3000/?python={int(datetime.now().timestamp())}&z=2'
            if not webbrowser.open_new_tab(url):
                print(f'Failed to open webbrowser, instead use URL: {url}')

        fcn = partial(_send_tracks, tmp_vectors=vectors, tmp_meta=meta)
        async with websockets.server.serve(fcn, 'localhost', 9924):
            await asyncio.Future()


def visualize(tracks, visualearth=False, display=True):
    ''' Synchronous wrapper for visualize_async().
        Tracks input to this function will be converted to JSON automatically.
        Display tracks in the web interface
    '''
    app = start_webapp(visualearth)

    try:
        asyncio.run(_visualize_async(map(serialize_track_json, tracks), display=display))
    finally:
        app.terminate()
