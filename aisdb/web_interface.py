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

from aisdb import wwwpath

logging.getLogger("websockets").setLevel(logging.WARNING)


def start_webapp_docker():
    check_install = subprocess.run(['docker', '--version'])
    if check_install.returncode != 0:
        raise RuntimeError(
            'Could not find docker version! ',
            'To start the web interface automatically, '
            'ensure that docker is installed and running.')

    already_running = reduce(
        lambda a, b: a or b,
        map(
            lambda j: j['Image'].split(':')[0] ==
            'meridiancfi/aisdb-web-interface' or 'aisdb-web-interface' in j[
                'Names'],
            map(
                orjson.loads,
                subprocess.run(['docker', 'ps', '--format', 'json'],
                               capture_output=True).stdout.split(b'\n')[:-1])))

    if not already_running:
        # yapf: disable
        docker_cmd = subprocess.run([
            'docker', 'run',
            '--detach',
            '--rm',
            '--publish', '3000:8080',
            '--env', 'VITE_DISABLE_SSL_DB=1',
            '--env', 'VITE_BINGMAPTILES=1',
            '--env', 'VITE_TILESERVER=aisdb.meridian.cs.dal.ca',
            '--env', 'VITE_AISDBHOST=localhost',
            '--env', 'VITE_AISDBPORT=9924',
            '--name', 'aisdb-web-interface',
            'meridiancfi/aisdb-web-interface'
            ], capture_output=True)
        if not docker_cmd.returncode == 0:
            raise RuntimeError(docker_cmd.stderr.decode())


def start_webapp_python():
    return subprocess.Popen([sys.executable, '-m', 'http.server', '-d', wwwpath, '3000'], env=os.environ)


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


def visualize(tracks, host='localhost', port=9924, start_app=True):
    ''' Synchronous wrapper for visualize_async().
        Display tracks in the web interface
    '''
    if start_app:
        #start_webapp_docker()
        app = start_webapp_python()
    try:
        asyncio.run(visualize_async(map(serialize_track_json, tracks), host, port))

    finally:
        if start_app:
            print('stopping webserver...')
            app.terminate()
