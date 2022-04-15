import asyncio
import os
# import ssl
import json
import websockets
from datetime import datetime, timedelta

from aisdb import zones_dir, DomainFromTxts
from aisdb import sqlfcn_callbacks, DBQuery
from aisdb import (
    TrackGen,
    encode_greatcircledistance,
    # split_timedelta,
    # max_tracklength,
)
from aisdb.webdata.marinetraffic import trafficDB, _vinfo

host = os.environ.get('AISDBHOSTALLOW', '*')
port = os.environ.get('AISDBPORT', 9924)
port = int(port)

domain = DomainFromTxts(zones_dir.rsplit(os.path.sep, 1)[1], zones_dir)


class SocketServ():

    async def handler(self, websocket):
        async for clientmsg in websocket:
            print(f'{datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} '
                  f'{websocket.remote_address} {str(clientmsg)}')

            req = json.loads(clientmsg)

            if req['type'] == 'zones':
                await self.req_zones(req, websocket)

            elif req['type'] == 'track_vectors':
                start = datetime(*map(int, req['start'].split('-')))
                end = datetime(*map(int, req['end'].split('-')))
                await self.req_tracks_raw(
                    req,
                    websocket,
                    start=start,
                    end=end,
                )

    async def req_zones(self, req, websocket):
        zones = {'type': 'WKBHex', 'geometries': []}
        for zone in domain.zones:
            event = {
                'geometry': zone['geometry'].wkb_hex,
                'meta': {
                    'label': zone['name'],
                },
            }
            zones['geometries'].append(event)
        await websocket.send(json.dumps(zones).replace(' ', ''))

    async def req_tracks_raw(self, req, websocket, *, start, end):
        qry = DBQuery(
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_bbox_time_validmmsi,
            xmin=domain.minX,
            xmax=domain.maxX,
            ymin=domain.minY,
            ymax=domain.maxY,
        )
        qrygen = encode_greatcircledistance(
            TrackGen(qry.gen_qry(printqry=os.environ.get('DEBUG', False))),
            distance_threshold=250000,
            minscore=0,
            speed_threshold=50,
        )
        with trafficDB as conn:
            count = 0
            for track in qrygen:
                _vinfo(track, conn)
                event = {
                    'msgtype': 'track_vector',
                    'x': list(track['lon']),
                    'y': list(track['lat']),
                    't': list(track['time']),
                    'meta': {
                        str(k): str(v)
                        for k, v in dict(
                            **track['marinetraffic_info']).items()
                    },
                }
                await websocket.send(json.dumps(event).replace(' ', ''))
                count += 1
                clientresponse = await websocket.recv()
                response = json.loads(clientresponse)
                if 'type' not in response.keys():
                    raise RuntimeWarning(
                        f'Unhandled client message: {response}')
                elif response['type'] == 'ack':
                    pass
                elif response['type'] == 'stop':
                    await websocket.send(
                        json.dumps({
                            'type': 'done',
                            'status': 'Halted search'
                        }))
                    break
                else:
                    raise RuntimeWarning(
                        f'Unhandled client message: {response}')
            await websocket.send(
                json.dumps({
                    'type': 'done',
                    'status': f'Done. Count: {count}'
                }))

    async def main(self):
        async with websockets.serve(self.handler,
                                    host=host,
                                    port=port,
                                    close_timeout=300,
                                    ping_interval=None):
            await asyncio.Future()


serv = SocketServ()
asyncio.run(serv.main())
