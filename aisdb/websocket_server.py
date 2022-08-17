import asyncio
import calendar
import os
import ssl
import warnings
import websockets
import websockets.exceptions
from datetime import datetime, timedelta

import aiosqlite
import numpy as np
import orjson as json

from aisdb import (
    sqlfcn,
    sqlfcn_callbacks,
)
from aisdb.database.dbqry import DBQuery_async
from aisdb.interp import interp_time_async
from aisdb.track_gen import (
    TrackGen_async,
    encode_greatcircledistance_async,
    split_timedelta_async,
)
from aisdb.webdata.marinetraffic import _vessel_info_dict, _nullinfo


class SocketServ():
    ''' Make vessel data available via websocket datastream, and respond
        to client data requests
    '''

    def __init__(self, dbpath, domain, trafficDBpath, enable_ssl=True):
        self.dbpath = dbpath
        self.host = os.environ.get('AISDBHOSTALLOW', '*')
        port = os.environ.get('AISDBPORT', 9924)
        self.port = int(port)
        self.domain = domain
        assert self.domain.zones != []
        self.vesselinfo = _vessel_info_dict(trafficDBpath)

        # let nginx in docker manage SSL by default
        if enable_ssl:  # pragma: no cover
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
        ''' handle messages received by the websocket '''
        self.dbconn = await aiosqlite.connect(self.dbpath)

        async for clientmsg in websocket:
            print(f'{datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} '
                  f'{websocket.remote_address} {str(clientmsg)}')

            req = json.loads(clientmsg)

            try:
                if req['type'] == 'zones':
                    await self.req_zones(req, websocket)

                elif req['type'] == 'track_vectors':
                    await self.req_tracks_raw(req, websocket)

                elif req['type'] == 'validrange':
                    await self.req_valid_range(req, websocket)

                elif req['type'] == 'heatmap':
                    await self.req_heatmap(req, websocket)

                elif req['type'] == 'ack':  # pragma: no cover
                    warnings.warn(
                        f'received unknown ack from {websocket.remote_address}'
                    )
            except (asyncio.exceptions.CancelledError,
                    asyncio.exceptions.TimeoutError):
                print(f'client timed out {websocket.remote_address}')
                break
            except websockets.exceptions.ConnectionClosedOK:
                print(f'disconnected client {websocket.remote_address}')
                break
            except websockets.exceptions.ConnectionClosedError:
                print(f'terminated client {websocket.remote_address}')
                break
            except Exception as err:
                await websocket.close()
                await self.dbconn.close()
                raise err
        await self.dbconn.close()
        await websocket.close()

    async def await_response(self, websocket):
        ''' await the client response and react accordingly '''
        clientresponse = await asyncio.wait_for(websocket.recv(), timeout=10)
        response = json.loads(clientresponse)

        if 'type' not in response.keys():
            raise RuntimeWarning(f'Unhandled client message: {response}')

        elif response['type'] == 'ack':
            return 0

        elif response['type'] == 'stop':
            return 'HALT'

        if response['type'] == 'zones':
            warnings.warn(
                f'client creating branching request! {websocket.remote_address}'
            )
            await self.req_zones(response, websocket)

        else:
            raise RuntimeWarning(f'Unhandled client message: {response}')

    async def _send_and_await(self, event, websocket, qry):
        await websocket.send(json.dumps(event,
                                        option=json.OPT_SERIALIZE_NUMPY))
        response = await self.await_response(websocket)
        if qry is not None and response == 'HALT':
            await qry.dbconn.close()
        return response

    def _create_dbqry(self, req):
        start = datetime(*map(int, req['start'].split('-')))
        end = datetime(*map(int, req['end'].split('-')))
        qry = DBQuery_async(
            dbpath=self.dbpath,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_bbox_time_validmmsi,
            xmin=req['area']['minX'],
            xmax=req['area']['maxX'],
            ymin=req['area']['minY'],
            ymax=req['area']['maxY'],
        )
        return qry

    async def req_valid_range(self, req, websocket):
        ''' send the range of valid database query time ranges to the client.
            sample request JSON::

                {"type" : "validrange"}

        '''
        res1 = await self.dbconn.execute_fetchall(
            "SELECT name FROM main.sqlite_master "
            "WHERE type='table' AND name LIKE '%_dynamic'")
        dynamic_months = sorted(
            [s.split('_')[1] for line in res1 for s in line])
        startyear = max(int(dynamic_months[0][:4]), 2012)
        start_month_range = calendar.monthrange(startyear,
                                                int(dynamic_months[0][4:]))[-1]
        end_month_range = calendar.monthrange(int(dynamic_months[-1][:4]),
                                              int(dynamic_months[-1][4:]))[-1]
        startval = f'{startyear}-{dynamic_months[0][4:]}-{start_month_range}'
        endval = f'{dynamic_months[-1][:4]}-{dynamic_months[-1][4:]}-{end_month_range}'
        await websocket.send(
            json.dumps({
                'type': 'validrange',
                'start': startval,
                'end': endval,
            }))

    async def req_zones(self, req, websocket):
        ''' send zone polygons to client. sample request JSON::

                {"type" : "zones"}

        '''
        for key in self.domain.zones.keys():
            zone = self.domain.zones[key]
            x, y = zone['geometry'].boundary.coords.xy
            event = {
                'msgtype': 'zone',
                'x': np.array(x, dtype=np.float32),
                'y': np.array(y, dtype=np.float32),
                't': [],
                'meta': {
                    'name': key,
                    'maxradius': str(zone['maxradius']),
                },
            }
            if await self._send_and_await(event, websocket, None) == 'HALT':
                return
        await websocket.send(json.dumps({'type':
                                         'doneZones'}))  # pragma: no cover

    async def req_tracks_raw(self, req, websocket):
        ''' create database query, generate track vectors from rows,
            and clean tracks using
            :func:`aisdb.track_gen.encode_greatcircledistance_async`,
            then send resulting vectors to client. sample request JSON::

                {
                    "type": "track_vectors",
                    "start": "2021-07-01",
                    "end": "2021-07-14",
                    "area": {
                          "minX": -66.23671874999998,
                          "maxX": -60.15029296874998,
                          "minY": 41.70498349725793,
                          "maxY": 45.413175940838045
                        }
                }

        '''
        qry = self._create_dbqry(req)
        trackgen = encode_greatcircledistance_async(
            TrackGen_async(qry.gen_qry(fcn=sqlfcn.crawl_dynamic_static)),
            distance_threshold=250000,
            minscore=0,
            speed_threshold=50,
        )
        count = 0
        async for track in trackgen:
            if track['mmsi'] in self.vesselinfo.keys():  # pragma: no cover
                track['marinetraffic_info'] = self.vesselinfo[track['mmsi']]
            else:
                track['marinetraffic_info'] = _nullinfo(track)
            event = {
                'msgtype': 'track_vector',
                'x': track['lon'],
                'y': track['lat'],
                't': track['time'],
                'meta': {
                    str(k): str(v)
                    for k, v in dict(**track['marinetraffic_info']).items()
                },
            }
            if await self._send_and_await(event, websocket, qry) == 'HALT':
                await trackgen.aclose()
                return
            else:
                count += 1

        await websocket.send(
            json.dumps({
                'type':
                'done',
                'status':
                f'Done. Count: {count}'
                if count > 0 else 'No data for selection'  # pragma: no cover
            }))

    async def req_heatmap(self, req, websocket):
        ''' create database query, generate track vectors from rows,
            and clean tracks using
            :func:`aisdb.track_gen.encode_greatcircledistance_async`.
            interpolate tracks to 10-minute resolution, and return point
            geometry for rendering the heatmap. send resulting geometry to
            client. sample request JSON::

                {
                    "type": "heatmap",
                    "start": "2021-07-01",
                    "end": "2021-07-14",
                    "area": {
                          "minX": -66.23671874999998,
                          "maxX": -60.15029296874998,
                          "minY": 41.70498349725793,
                          "maxY": 45.413175940838045
                        }
                }

        '''
        qry = self._create_dbqry(req)
        interps = interp_time_async(encode_greatcircledistance_async(
            split_timedelta_async(TrackGen_async(qry.gen_qry()),
                                  maxdelta=timedelta(hours=12)),
            distance_threshold=250000,
            minscore=0,
            speed_threshold=50,
        ),
                                    step=timedelta(minutes=60))
        count = 0
        async for itr in interps:
            response = {
                'type': 'heatmap',
                #'mmsi': itr['mmsi'],
                'xy': np.array(list(zip(itr['lon'], itr['lat'])))
            }
            if await self._send_and_await(response, websocket, qry) == 'HALT':
                await interps.aclose()
                return
            count += 1

        await websocket.send(  # pragma: no cover
            json.dumps({
                'type': 'done',
                'status': f'done loading heatmap. vessel count: {count}'
            }))

    async def main(self):  # pragma: no cover
        ''' run the server main loop asynchronously. should be called with
            :func:`asyncio.run`
        '''
        async with websockets.serve(
                self.handler,
                host=self.host,
                port=self.port,
                **self.ssl_args,
                ping_timeout=120,
        ):
            await asyncio.Future()


if __name__ == '__main__':
    # by default let nginx handle SSL
    serv = SocketServ(enable_ssl=False)
    asyncio.run(serv.main())
