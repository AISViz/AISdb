# Python standard library packages
#from datetime import datetime, timedelta
import asyncio
from functools import partial

# These packages need to be installed with pip
import orjson
import websockets.server

async def _ping_client(client_addr='127.0.0.1:9924'):
    ''' verify that the web client is online and ready to accept messages '''
    pass
#raise RuntimeError(f'Did not receive connection ping from {client_addr}! Is the front end web service running?')


#async def _handler(websocket):
    #async for message in websocket:
    #    print(message)


async def serialize_tracks(websocket, tracks):
    # TODO: assert webserver is running in the background
    '''
    vector
    {'meta': {'mmsi': '316032262'}, 'msgtype': 'track_vector', 't': [1682361350, 1682447459], 'x': [-63.5701, -63.5665], 'y': [44.659, 44.6513]}

    meta
    {'flag': 'Croatia [HR]', 'gross_tonnage': 63802, 'length_breadth': '249.9 x 44 m', 'mmsi': 238020000, 'msgtype': 'vesselinfo', 'summer_dwt': 114305, 'vessel_name2': 'FRANKOPAN', 'vesseltype_detailed': 'Crude Oil Tanker', 'vesseltype_generic': 'Fishing', 'year_built': 2017}


    '''
    for track in tracks:
        if len(track['time']) == 1: continue
        vector = {
                'msgtype': 'track_vector',
                'meta': {'mmsi': track['mmsi']},
                't': track['time'],
                'x': track['lon'],
                'y': track['lat'],
                }

        meta = {k:track[k] for k in track['static'] if k != 'marinetraffic_info'}
        meta['msgtype'] = 'vesselinfo'

        if 'marinetraffic_info' in track.keys():
            meta.update({k:track['marinetraffic_info'][k] for k in track['marinetraffic_info'].keys()})

        vector_json = orjson.dumps(vector, option=orjson.OPT_SERIALIZE_NUMPY)
        await websocket.send(vector_json)

        meta_json = orjson.dumps(meta)
        await websocket.send(meta_json)

        print(vector_json)
        print(meta_json)
        break

async def _serve_tracks(tracks, host='localhost', port=9924):
    ''' serve up some data to the web client '''
    fcn = partial(serialize_tracks, tracks=tracks)
    #async with websockets.server.serve(_send_to_web_ui_async, host, port):
    async with websockets.server.serve(fcn, host, port, timeout=2):
        print('awaiting connection from UI socket...')
        #await _ping_client()
        await asyncio.Future()


def serve_tracks(tracks):
    asyncio.run(_serve_tracks(tracks))


#def send_to_web_ui(tracks, listen_addr='0.0.0.0:9924'):
#    asyncio.run(_send_to_web_ui_async(tracks, listen_addr))
#    return

#if __name__ == '__main__':
#    asyncio.run(_serve_tracks())
