import os
import asyncio
import pytest

from aisdb import websocket_server, decode_msgs
from aisdb.webdata.marinetraffic import VesselInfo
from aisdb.tests.create_testing_data import random_polygons_domain


@pytest.mark.asyncio
async def test_websocket_nodata_nosocket(tmpdir):

    dbpath = os.path.join(tmpdir, 'test_websocket_server.db')
    trafficDBpath = str(os.path.join(tmpdir,
                                     'test_websocket_trafficDBpath.db'))
    # create tables in __init__ fcn
    VesselInfo(trafficDBpath=trafficDBpath)
    datapath = os.path.join(os.path.dirname(__file__),
                            'testingdata_20211101.nm4')
    decode_msgs(
        filepaths=[datapath],
        dbpath=dbpath,
        source='TESTING',
        vacuum=False,
        skip_checksum=True,
    )

    req = {
        "type": "track_vectors",
        "start": "2021-11-01",
        "end": "2021-11-02",
        "area": {
            "minX": -64.6484375,
            "maxX": -63.593749999999986,
            "minY": 52.44827245284063,
            "maxY": 52.92774421348463
        }
    }
    serv = websocket_server.SocketServ(dbpath=dbpath,
                                       domain=random_polygons_domain(),
                                       trafficDBpath=trafficDBpath,
                                       enable_ssl=False)
    #loop = asyncio.get_event_loop()

    try:
        loop = asyncio.new_event_loop()
        loop.run_in_executor(serv.req_tracks_raw, (req, None))
        #loop.run_until_complete(loop.shutdown_asyncgens())
    # intended behaviour when websocket is None
    except AttributeError as err:
        print(f'caught exception: {err.with_traceback(None)}')
        assert (str(err.with_traceback(None))
                == "'NoneType' object has no attribute 'send'"
                or str(err.with_traceback(None))
                == "'function' object has no attribute 'submit'")
    except Exception as err:
        raise err
    finally:
        loop.close()
        await loop.shutdown_asyncgens()
    return
