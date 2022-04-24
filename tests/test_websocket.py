import os
import asyncio
import pytest
import time

from aisdb import websocket_server


@pytest.mark.asyncio
async def test_websocket_nodata_nosocket():

    # this test will never exit, must be closed manually
    if not os.environ.get('ASYNCDEBUG'):
        return 0

    req = {
        "type": "track_vectors",
        "start": "2016-01-01",
        "end": "2016-01-02",
        "area": {
            "minX": -64.6484375,
            "maxX": -63.593749999999986,
            "minY": 52.44827245284063,
            "maxY": 52.92774421348463
        }
    }
    serv = websocket_server.SocketServ(enable_ssl=False)
    loop = asyncio.get_event_loop()

    try:
        await loop.run_until_complete(await serv.req_tracks_raw(req, None))
        #loop.run_until_complete(loop.shutdown_asyncgens())
    # intended behaviour when websocket is None
    except AttributeError as err:
        print(f'caught exception: {err.with_traceback(None)}')
        assert str(err.with_traceback(
            None)) == "'NoneType' object has no attribute 'send'"
    except Exception as err:
        raise err
    finally:
        pass
    return
