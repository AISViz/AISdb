import asyncio
import os
import ssl
import websockets
import calendar
import sqlite3
from datetime import datetime

import orjson as json
import numpy as np

import aisdb
from aisdb import (
    DBConn,
    DBQuery,
    DomainFromTxts,
    sqlfcn,
    sqlfcn_callbacks,
)
from aisdb.webdata.marinetraffic import _vinfo
from aisdb.track_gen import (
    TrackGen_async,
    encode_greatcircledistance_async,
)


def request_size(*, xmin, xmax, ymin, ymax, start, end):
    ''' restrict box size to 3000 kilometers diagonal distance per month'''
    dist_diag = aisdb.haversine(x1=xmin, y1=ymin, x2=xmax, y2=ymax) / 1000
    delta_t = (end - start).total_seconds() / 60 / 60 / 24 / 30
    if dist_diag * delta_t > 3000:
        return False
    else:
        return True


class SocketServ():
    ''' Make vessel data available via websocket datastream, and respond
        to client data requests
    '''

    def __init__(self, dbpath, domain, trafficDBpath, enable_ssl=True):
        self.dbpath = dbpath
        self.trafficDB = sqlite3.Connection(trafficDBpath)
        self.trafficDB.row_factory = sqlite3.Row
        self.host = os.environ.get('AISDBHOSTALLOW', '*')
        port = os.environ.get('AISDBPORT', 9924)
        self.port = int(port)
        self.domain = domain

        if enable_ssl:
            sslpath = os.path.join('/etc/letsencrypt/live/',
                                   os.environ.get('AISDBHOST', '127.0.0.1'))
            CRT = os.path.join(sslpath, 'fullchain.pem')
            KEY = os.path.join(sslpath, 'privkey.pem')
            print(f'loading SSL context: {CRT} {KEY}')
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_context.load_cert_chain(CRT, KEY)
            self.ssl_args = {'ssl': ssl_context}
        else:
            self.ssl_args = {}

    async def handler(self, websocket):
        async for clientmsg in websocket:
            print(f'{datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} '
                  f'{websocket.remote_address} {str(clientmsg)}')

            req = json.loads(clientmsg)

            if req['type'] == 'zones':
                await self.req_zones(req, websocket)

            elif req['type'] == 'track_vectors':
                await self.req_tracks_raw(req, websocket)

            elif req['type'] == 'validrange':
                await self.req_valid_range(req, websocket)

            elif req['type'] == 'ack':
                pass

    async def await_response(self, websocket):
        clientresponse = await websocket.recv()
        response = json.loads(clientresponse)
        if 'type' not in response.keys():
            raise RuntimeWarning(f'Unhandled client message: {response}')
        elif response['type'] == 'ack':
            pass
        elif response['type'] == 'stop':
            await websocket.send(
                json.dumps({
                    'type': 'done',
                    'status': 'Halted search'
                }))
            return 'HALT'
        elif response['type'] == 'zones':
            await self.req_zones(response, websocket)

        elif response['type'] == 'track_vectors':
            await self.req_tracks_raw(response, websocket)
        else:
            raise RuntimeWarning(f'Unhandled client message: {response}')
        return 0

    async def req_valid_range(self, req, websocket):
        with DBConn(self.dbpath).conn as conn:
            res = sorted([
                s.split('_')[1] for line in conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name LIKE '%_dynamic'").fetchall()
                for s in line
            ])
        startyear = max(int(res[0][:4]), 2012)
        start_month_range = calendar.monthrange(startyear, int(res[0][4:]))[-1]
        end_month_range = calendar.monthrange(int(res[-1][:4]),
                                              int(res[-1][4:]))[-1]
        startval = f'{startyear}-{res[0][4:]}-{start_month_range}'
        endval = f'{res[-1][:4]}-{res[-1][4:]}-{end_month_range}'
        await websocket.send(
            json.dumps({
                'type': 'validrange',
                'start': startval,
                'end': endval,
            }))

    async def req_zones(self, req, websocket):
        for zone in self.domain.zones:
            x, y = zone['geometry'].boundary.coords.xy
            event = {
                'msgtype': 'zone',
                'x': np.array(x, dtype=np.float32),
                'y': np.array(y, dtype=np.float32),
                't': [],
                'meta': {
                    'name': zone['name'],
                    'maxradius': str(zone['maxradius']),
                },
            }
            await websocket.send(
                json.dumps(event, option=json.OPT_SERIALIZE_NUMPY))
            if await self.await_response(websocket) == 'HALT':
                return
        await websocket.send(json.dumps({'type': 'doneZones'}))

    async def req_tracks_raw(self, req, websocket):
        start = datetime(*map(int, req['start'].split('-')))
        end = datetime(*map(int, req['end'].split('-')))
        qry = DBQuery(
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_bbox_time_validmmsi,
            xmin=req['area']['minX'],
            xmax=req['area']['maxX'],
            ymin=req['area']['minY'],
            ymax=req['area']['maxY'],
        )
        qrygen = encode_greatcircledistance_async(
            TrackGen_async(
                qry.async_qry(self.dbpath, fcn=sqlfcn.crawl_dynamic_static)),
            distance_threshold=250000,
            minscore=0,
            speed_threshold=50,
        )
        with self.trafficDB as conn:
            count = 0
            async for track in qrygen:
                _vinfo(track, conn)
                event = {
                    'msgtype': 'track_vector',
                    'x': track['lon'],
                    'y': track['lat'],
                    't': track['time'],
                    'meta': {
                        str(k): str(v)
                        for k, v in dict(
                            **track['marinetraffic_info']).items()
                    },
                }
                await websocket.send(
                    json.dumps(event, option=json.OPT_SERIALIZE_NUMPY))
                count += 1

                if await self.await_response(websocket) == 'HALT':
                    return

            if count > 0:
                await websocket.send(
                    json.dumps({
                        'type': 'done',
                        'status': f'Done. Count: {count}'
                    }))
            else:
                await websocket.send(
                    json.dumps({
                        'type': 'done',
                        'status': 'No data for selection'
                    }))

    async def main(self):
        async with websockets.serve(
                self.handler,
                host=self.host,
                port=self.port,
                **self.ssl_args,
                ping_timeout=300,
        ):
            await asyncio.Future()


if __name__ == '__main__':
    # by default let nginx handle SSL
    serv = SocketServ(enable_ssl=False)
    asyncio.run(serv.main())
