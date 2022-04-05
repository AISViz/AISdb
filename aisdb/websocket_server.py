import asyncio
import os
import shutil
import json
from json import JSONDecodeError
from functools import partial
from multiprocessing import Pool
from datetime import datetime, timedelta

import topojson as tp
import websockets
from shapely.geometry import Point, LineString

from aisdb import output_dir
from aisdb import glob_files
from aisdb import zones_dir, ZoneGeomFromTxt, Domain
from aisdb import sqlfcn_callbacks, DBQuery
from aisdb import TrackGen, encode_greatcircledistance, split_timedelta, max_tracklength
from aisdb.network_graph import colorhash
from aisdb.gis import shiftcoord

host = os.environ.get('AISDBHOSTALLOW', '*')
port = os.environ.get('AISDBPORT', 9924)
port = int(port)

print(f"starting server on {host}:{port}")

shapefilepaths = glob_files(zones_dir, ext='.txt')
zonegeoms = {z.name: z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]}
domain = Domain('east', zonegeoms, cache=False)


def domainWKB(domain):
    wkb = []
    for zoneID, geom in domain.geoms.items():
        if hasattr(geom, 'geometryCollection'):
            for g in geom.geometryCollection:
                wkb.append(g.wkb_hex)
        else:
            wkb.append(geom.geometry.wkb_hex)
    txt = "newWKBHexVectorLayer(['" + "','".join(wkb) + "'], {});"

    with open(os.path.join(output_dir, 'zonegeoms_test.hexgeom'), 'w') as f:
        f.write(txt)


def trajectories_binary(tracks, ident=lambda track: str(track['mmsi'])):
    for track in tracks:
        fname = f"{track['mmsi']}_{int(track['time'][0])}-{int(track['time'][-1])}.geom"
        fpath = os.path.join(output_dir, fname)

        if track['time'].size <= 1:
            geom = Point(track['lon'][0], track['lat'][0])
        else:
            geom = LineString(zip(track['lon'], track['lat']))

        topo = tp.Topology(geom).toposimplify(.001, prevent_oversimplify=True)

        objectData = "newTopoVectorLayer('"
        objectData += json.dumps(topo.output).replace(' ', '')
        objectData += "',{color:" + colorhash(ident(track)) + "});"
        with open(fpath, 'w') as f:
            f.write(objectData)


def trajectories_json(tracks, ident=lambda track: str(track['mmsi'])):
    for track in tracks:
        fname = f"{track['mmsi']}_{int(track['time'][0])}-{int(track['time'][-1])}.geom"
        fpath = os.path.join(output_dir, fname)

        if track['time'].size <= 1:
            geom = Point(track['lon'][0], track['lat'][0])
        else:
            geom = LineString(zip(track['lon'], track['lat']))

        topo = tp.Topology(geom).toposimplify(.001, prevent_oversimplify=True)

        yield (
            #json.dumps(topo.output).replace(' ', ''),
            topo.output,
            #json.dumps({"color": '"' + colorhash(ident(track)) + '"'}),
            {
                "color": colorhash(ident(track))
            },
        )

    return


def pipeline(rowset, params):
    timesplit = partial(split_timedelta, maxdelta=params['cuttime_force'])
    distsplit = partial(
        encode_greatcircledistance,
        speed_threshold=params['speed_threshold'],
        #time_threshold=params['time_threshold'],
        distance_threshold=params['distance_threshold'],
        minscore=params['minscore'],
    )
    #return trajectories_binary(TrackGen([rowset]))
    return trajectories_binary(
        distsplit(timesplit(max_tracklength(TrackGen([rowset])))))


def track_vectors(rowgen, domain, output='tracks.txt', processes=0, **params):
    fcn = partial(pipeline, params=params)
    if processes == 0:
        for rowset in rowgen:
            print(rowset[0][0])
            fcn(rowset)
    else:
        with Pool(processes=processes) as p:
            p.imap_unordered(fcn, rowgen)
            p.close()
            p.join()

    geomfiles = glob_files(output_dir, '.geom')
    print(f'concatenating {len(geomfiles)} files')
    outfiles = 0
    outpath = os.path.join(output_dir, output)
    while len(geomfiles) > 0:
        nextfile = outpath + str(outfiles)
        with open(nextfile, 'w') as f:
            while (os.stat(nextfile).st_size / 1048576 < 32
                   and len(geomfiles) > 0):
                g = geomfiles.pop(0)
                if os.stat(g).st_size / 1048576 > 5:
                    continue
                with open(g, 'r') as gfile:
                    shutil.copyfileobj(gfile, f)
                os.remove(g)
        outfiles += 1


class SocketServ():

    async def handler(self, websocket):
        enabled = True
        while enabled:
            try:
                clientmsg = await websocket.recv()
            except websockets.ConnectionClosedOK:
                continue
            except websockets.ConnectionClosed:
                print('closing...')
                await websocket.close()
                break
            except KeyboardInterrupt:
                print('exiting...')
                enabled = False
                exit(0)
                break
            except Exception as err:
                print('error awaiting client: ', end='')
                if hasattr(err, '__module__'):
                    print(err.__module__, end=': ')
                print(err.with_traceback(None))
                continue

            print('|' + clientmsg + '|')

            if clientmsg.strip() == 'test':
                await websocket.send('ack')

            try:
                req = json.loads(clientmsg)
                assert 'type' in req.keys()
            except JSONDecodeError:
                print('not json', clientmsg)
                continue
            except websockets.ConnectionClosed:
                print('closing...')
                await websocket.close()
                break
            except Exception as err:
                print('error sending parsing request: ', end='')
                if hasattr(err, '__module__'):
                    print(err.__module__, end=': ')
                print(err.with_traceback(None))
                continue

            if req['type'] == 'zones':
                await self.req_zones(req, websocket)

            elif req['type'] == 'tracks_month':
                y, m = req['month'][:4], req['month'][4:]
                year, month = int(y), int(m)
                start = datetime(year, month, 1)
                end = datetime(year + int(month == 12), month % 12 + 1, 1)
                await self.req_tracks(
                    req,
                    websocket,
                    start=start,
                    end=end,
                )

            elif req['type'] == 'tracks_week':
                y, m, d = req['day'][:4], req['day'][4:6], req['day'][6:]
                year, month, day = int(y), int(m), int(d)
                start = datetime(year, month, day)
                end = start + timedelta(days=7)
                await self.req_tracks(
                    req,
                    websocket,
                    start=start,
                    end=end,
                )

            elif req['type'] == 'tracks_day':
                y, m, d = req['day'][:4], req['day'][4:6], req['day'][6:]
                year, month, day = int(y), int(m), int(d)
                start = datetime(year, month, day)
                end = start + timedelta(days=1)
                await self.req_tracks(
                    req,
                    websocket,
                    start=start,
                    end=end,
                )

    async def req_zones(self, req, websocket):
        zones = {'type': 'WKBHex', 'geometries': []}
        for key, zonegeom in domain.geoms.items():
            event = {
                'geometry': zonegeom.geometry.wkb_hex,
                'opts': {
                    'label': zonegeom.name,
                },
            }
            zones['geometries'].append(event)
        await websocket.send(json.dumps(zones).replace(' ', ''))

    async def req_tracks(self, req, websocket, *, start, end):
        qry = DBQuery(
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_bbox_time_validmmsi,
            xmin=domain.minX,
            xmax=domain.maxX,
            ymin=domain.minY,
            ymax=domain.maxY,
        )
        qrygen = qry.gen_qry(printqry=os.environ.get('DEBUG', False))
        pipeline = trajectories_json(
            encode_greatcircledistance(
                split_timedelta(
                    max_tracklength(TrackGen(qrygen)),
                    maxdelta=timedelta(weeks=1),
                ),
                distance_threshold=250000,
                minscore=0,
                speed_threshold=60,
            ))
        eventbatch = {'type': 'topology', 'geometries': []}
        count = 0
        for topology, opts in pipeline:
            count += 1
            event = {'topology': topology, 'opts': opts}
            eventbatch['geometries'].append(event)
            if count % 50 != 0:
                continue
            try:
                await websocket.send(json.dumps(eventbatch).replace(' ', ''))
            except websockets.ConnectionClosed:
                print('closing...')
                await websocket.close()
                break
            except Exception as err:
                print('error sending topology: ', end='')
                if hasattr(err, '__module__'):
                    print(err.__module__, end=': ')
                raise err.with_traceback(None)
            eventbatch = {'type': 'topology', 'geometries': []}

    async def main(self):
        async with websockets.serve(self.handler, host=host, port=port):
            await asyncio.Future()


serv = SocketServ()
asyncio.run(serv.main())
