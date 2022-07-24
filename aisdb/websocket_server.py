import asyncio
import os
import ssl
import websockets
import calendar
from datetime import datetime, timedelta

import orjson as json
import numpy as np

from aisdb import (
    sqlfcn,
    sqlfcn_callbacks,
)
from aisdb.database.dbconn import DBConn_async
from aisdb.database.dbqry import DBQuery_async
from aisdb.webdata.marinetraffic import _metadict, _nullinfo
from aisdb.track_gen import (
    TrackGen_async,
    encode_greatcircledistance_async,
    min_speed_filter_async,
    split_timedelta_async,
)
from aisdb.interp import interp_time_async


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
        self.vesselinfo = _metadict(trafficDBpath)

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

            elif req['type'] == 'heatmap':
                await self.req_heatmap(req, websocket)

            elif req['type'] == 'ack':
                pass

    async def await_response(self, websocket):
        ''' await the client response and react accordingly '''
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

    def _create_dbqry(self, req, db):
        start = datetime(*map(int, req['start'].split('-')))
        end = datetime(*map(int, req['end'].split('-')))
        qry = DBQuery_async(
            db=db,
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
        async with DBConn_async(dbpath=self.dbpath) as db:
            res1 = await db.conn.execute_fetchall(
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
        async with DBConn_async(dbpath=self.dbpath) as db:
            qry = self._create_dbqry(req, db)
            qrygen = encode_greatcircledistance_async(
                TrackGen_async(
                    qry.gen_qry(self.dbpath, fcn=sqlfcn.crawl_dynamic_static)),
                distance_threshold=250000,
                minscore=0,
                speed_threshold=50,
            )
            #with self.trafficDB as conn:
            count = 0
            async for track in qrygen:
                #_vinfo(track, conn)
                if track['mmsi'] in self.vesselinfo.keys():
                    track['marinetraffic_info'] = self.vesselinfo[
                        track['mmsi']]
                else:
                    track['marinetraffic_info'] = _nullinfo(track)
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

    async def req_heatmap(self, req, websocket):
        ''' create database query, generate track vectors from rows,
            and clean tracks using
            :func:`aisdb.track_gen.encode_greatcircledistance_async`.
            interpolate tracks to 10-minute resolution, and return point
            geometry for rendering the heatmap. send resulting geometry to
            client. sample request JSON::

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
        subqry = qry.copy()
        count = 0
        for day in np.arange(
                qry['start'],
                qry['end'],
                timedelta(days=1),
        ).astype(datetime):
            subqry['start'] = day
            subqry['end'] = day + timedelta(days=1)
            interps = interp_time_async(
                min_speed_filter_async(
                    encode_greatcircledistance_async(
                        split_timedelta_async(TrackGen_async(
                            subqry.async_qry(self.dbpath,
                                             fcn=sqlfcn.crawl_dynamic)),
                                              maxdelta=timedelta(hours=2)),
                        distance_threshold=250000,
                        minscore=0,
                        speed_threshold=50,
                    ),
                    minspeed=1,
                ),
                step=timedelta(minutes=60),
            )
            async for itr in interps:
                response = {
                    'type': 'heatmap',
                    'xy': list(zip(itr['lon'], itr['lat']))
                }
                await websocket.send(
                    json.dumps(response, option=json.OPT_SERIALIZE_NUMPY))
            count += 1
            json.dumps({
                'type':
                'done',
                'status':
                f'Searching... Heatmap Days Processed: {count}'
            })

    async def main(self):
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
