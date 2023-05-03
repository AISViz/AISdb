import asyncio
import logging
import sys
import webbrowser
import http.server
import socketserver
from datetime import datetime
from functools import partial
from subprocess import Popen
from tempfile import SpooledTemporaryFile
from concurrent.futures import ProcessPoolExecutor

import orjson
import websockets.server

from aisdb import wwwpath, wwwpath_alt

logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("shapely").setLevel(logging.WARNING)


async def _start_webclient(visualearth=False):
    if not visualearth:
        path = wwwpath
    else:
        path = wwwpath_alt
        
    class AISDB_HTML(http.server.SimpleHTTPRequestHandler):
        extensions_map = {
                '': 'application/octet-stream',
                '.css':	'text/css',
                '.html': 'text/html',
                '.jpg': 'image/jpg',
                '.js':	'application/x-javascript',
                '.json': 'application/json',
                '.png': 'image/png',
                '.wasm': 'application/wasm',
        }

        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=path, **kwargs)

    #return Popen([sys.executable, '-m', 'http.server', '-d', path + os.path.sep + 'index.html',  '3000'])
    with socketserver.TCPServer(("localhost", 3000), AISDB_HTML) as httpd:
        httpd.serve_forever()


def serialize_zone_json(name, zone):
    zone_dict = {'msgtype': 'zone',
                 'meta': {'name': name},
                 'x': tuple(zone['geometry'].boundary.xy[0]),
                 'y': tuple(zone['geometry'].boundary.xy[1]),
                 't': [],
                 }
    return orjson.dumps(zone_dict)


def serialize_track_json(track) -> bytes:
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


async def _send_tracks(websocket, tmp_vectors, tmp_meta, domain=None):
    ''' send tracks serialized as JSON to the connected websocket client '''
    done = {}
    async for message_json in websocket:
        message = orjson.loads(message_json)
        print('got msg:', message)

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

            if domain is not None:
                for name, zone in domain.zones.items():
                    zone_json = serialize_zone_json(name, zone)
                    await websocket.send(zone_json)

            tmp_vectors.seek(0)
            for vector_json in tmp_vectors:
                await websocket.send(vector_json)

        elif 'meta' in done.keys():
            assert len(done.keys()) == 1
            tmp_meta.seek(0)
            for meta_json in tmp_meta:
                await websocket.send(meta_json)


async def _start(tracks, domain=None, visualearth=False, open_browser=True):
    ''' Display tracks in the web interface. Serves data to the web client '''
    print('Querying database...', end='\t')
    executor = ProcessPoolExecutor(1)
    loop = asyncio.get_running_loop()
    with SpooledTemporaryFile(max_size=1024*1e6, newline=b'\n') as vectors, \
            SpooledTemporaryFile(max_size=256*1e6, newline=b'\n') as meta:
        for vector, info in map(serialize_track_json, tracks):
            vectors.write(vector)
            vectors.write(b'\n')
            meta.write(info)
            meta.write(b'\n')

        print('done query')


        if open_browser:
            print('Opening a new browser window to display track data')
            print('Press Ctrl-C to close the webpage')
            tag = 1 if not visualearth else 2
            tag = int(datetime.now().timestamp())
            url = f'http://localhost:3000/index.html?python={tag}&z=2'
            if not webbrowser.open_new_tab(url):
                print(f'Failed to open webbrowser, instead use URL: {url}')
                
        fcn = partial(_send_tracks, tmp_vectors=vectors, tmp_meta=meta, domain=domain)
        async with websockets.server.serve(fcn, 'localhost', 9924) as server:
            stop = asyncio.Future()
            #a = asyncio.create_task(stop)
            #server = asyncio.create_task()
            client = asyncio.create_task(_start_webclient(visualearth))
            #asyncio.gather([stop,client,server])
            await asyncio.wait([
                loop.run_in_executor(executor, client),
                loop.run_in_executor(executor, server),
                loop.run_in_executor(executor, stop),
            ])
            #await asyncio.wait([stop, server, client])
            #await stop
            #await server.close()
            #return stop, server

'''
async def _start(tracks, domain=None, visualearth=False, open_browser=True):
    stop, server = await _start_webserver(tracks, domain=None, visualearth=False, open_browser=True)
    #client = asyncio.create_task(_start_webclient(visualearth))
    client = _start_webclient(visualearth)
    #await asyncio.wait([server, client])
    await asyncio.wait([stop, server])
    #await server.stop()
'''




def visualize(tracks, domain=None, visualearth=False, open_browser=True):
    ''' Synchronous wrapper for _start_webclient_webserver().
        Tracks input to this function will be converted to JSON automatically.
        Display tracks in the web interface
    '''
    try:
        asyncio.run(_start(tracks, domain, visualearth, open_browser))
        #_start(tracks, domain, visualearth, open_browser)
    except KeyboardInterrupt:
        pass
    
    
