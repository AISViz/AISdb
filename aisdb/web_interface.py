import asyncio
import http.server
import logging
import multiprocessing
import os
import socketserver
import webbrowser
from datetime import datetime
from functools import partial
from tempfile import SpooledTemporaryFile

import orjson
import websockets.server

logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("shapely").setLevel(logging.WARNING)

wwwpath = os.path.abspath(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), 'aisdb_web',
                 'dist_map'))

wwwpath_visualearth = os.path.abspath(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), 'aisdb_web',
                 'dist_map_bingmaps'))


def _start_webclient(visualearth=False):
    if not visualearth:
        path = wwwpath
    else:
        path = wwwpath_visualearth

    class AISDB_HTML(http.server.SimpleHTTPRequestHandler):

        extensions_map = {
            '': 'application/octet-stream',
            '.css': 'text/css',
            '.html': 'text/html',
            '.jpg': 'image/jpg',
            '.js': 'application/x-javascript',
            '.json': 'application/json',
            '.png': 'image/png',
            '.wasm': 'application/wasm',
        }

        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=path, **kwargs)

    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("localhost", 3000), AISDB_HTML) as httpd:
        try:
            print('Serving HTTP assets on localhost:3000')
            httpd.serve_forever()
        except KeyboardInterrupt:
            httpd.server_close()
            httpd.shutdown()
        except Exception as e:
            httpd.server_close()
            httpd.shutdown()
            raise e


def serialize_zone_json(name, zone) -> bytes:
    zone_dict = {
        'msgtype': 'zone',
        'meta': {
            'name': name
        },
        'x': tuple(zone['geometry'].boundary.xy[0]),
        'y': tuple(zone['geometry'].boundary.xy[1]),
        't': [],
    }
    return orjson.dumps(zone_dict)


def serialize_track_json(track) -> (bytes, bytes):
    ''' serializes a single track dictionary to JSON format encoded as UTF8 '''
    vector = {
        'msgtype': 'track_vector',
        # currently, database_server sends all metadata as strings
        # reproduce this behaviour by coercion to string type, even for int
        'meta': {
            'mmsi': str(track['mmsi'])
        },
        't': track['time'],
        'x': track['lon'],
        'y': track['lat'],
    }

    meta = {k: track[k] for k in track['static'] if k != 'marinetraffic_info'}

    if 'color' in track.keys():
        meta['color'] = track['color']

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
        print(
            f'{websocket.remote_address[0]}:{websocket.remote_address[1]} - received: {message}'
        )

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


async def _start_webserver(tracks,
                           domain=None,
                           visualearth=False,
                           open_browser=True):
    ''' Display tracks in the web interface. Serves data to the web client '''
    print('Querying database...', end='\t')
    with SpooledTemporaryFile(max_size=1024 * 1e6, newline=b'\n') as vectors, \
            SpooledTemporaryFile(max_size=256 * 1e6, newline=b'\n') as meta:
        for vector, info in map(serialize_track_json, tracks):
            vectors.write(vector)
            vectors.write(b'\n')
            meta.write(info)
            meta.write(b'\n')

        print('done query')

        if open_browser:
            print('Opening a new browser window to display track data. '
                  'Press Ctrl-C to stop the server and close the webpage')
            tag = 1 if not visualearth else 2
            url = f'http://localhost:3000/index.html?python={tag}&z=2'
            if not webbrowser.open_new_tab(url):
                print(f'Failed to open webbrowser, instead use URL: {url}')

        fcn = partial(_send_tracks,
                      tmp_vectors=vectors,
                      tmp_meta=meta,
                      domain=domain)
        async with websockets.server.serve(fcn, 'localhost', 9924) as server:
            stop = asyncio.Future()
            await stop
            await server


def visualize(tracks, domain=None, visualearth=False, open_browser=True):
    ''' Display tracks using the web interface.

        Starts the web client HTTP server in a separate process, and
        serves track data via websocket on port 9924.

        If a domain object is given, zone polygons will be drawn on the map
        from domain zone geometries.

        If visualearth is True, microsoft visual earth map tiles will be used
        for the map background.

        If open_browser is True, python will attempt to open the web application
        in a new tab using the default browser.

        To customize the color of each vessel track, set the 'color' value to
        a color string or RGB value string:

        >>> def color_tracks(tracks):
        ...     for track in tracks:
        ...         track['color'] = 'red' or 'rgb(255,0,0)'
        ...         yield track
        ...
        >>> tracks = [
        ...     {'mmsi': 204242000, 'lon': [-8.931666], 'lat':[41.45], 'time': [1625176725]},
        ...     {'mmsi': 204814000, 'lon': [-25.668333], 'lat': [37.736668], 'time': [1625147353]},
        ... ]
        >>> tracks_colored = color_tracks(tracks)
    '''
    proc = multiprocessing.Process(target=_start_webclient, args=[visualearth])
    proc.start()
    try:
        asyncio.run(_start_webserver(tracks, domain, visualearth,
                                     open_browser))
        proc.join()
    except KeyboardInterrupt:
        print('Received KeyboardInterrupt, stopping server...')
        proc.terminate()
    except Exception as err:
        proc.terminate()
        raise err
