import orjson
import os
import pytest

from aisdb.websocket_server import SocketServ
from aisdb.tests.create_testing_data import (
    random_polygons_domain,
    sample_database_file,
)

dbpath = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'testdata',
    'test_10_websocket_server.db')

domain = random_polygons_domain(10)
testdir = os.environ.get(
    'AISDBTESTDIR',
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                 'testdata'))
trafficDBpath = os.path.join(testdir, 'marinetraffic_test.db')


class FakeWebSocketClient(list):
    remote_address = 'FakeWebSocketClient@localhost'

    ack = {'type': 'ack'}

    stop = {'type': 'stop'}

    validrange = {'type': 'validrange'}

    zones = {'type': 'zones'}

    track_vectors = {
        "type": "track_vectors",
        "start": "2021-07-01",
        "end": "2021-07-14",
        "area": {
            "minX": -180,
            "maxX": 0,
            "minY": 0,
            "maxY": 90,
        }
    }

    heatmap = {
        "type": "heatmap",
        "start": "2021-07-01",
        "end": "2021-07-30",
        "area": {
            "minX": -180,
            "maxX": 180,
            "minY": -90,
            "maxY": 90
        }
    }

    def __init__(self, requests=[validrange, zones, track_vectors, heatmap]):
        super().__init__(map(orjson.dumps, requests))
        self.values = []
        self._reset_responses()

    def _reset_responses(self, responses=[ack, ack, stop]):
        self.responses = list(map(orjson.dumps, responses))

    async def __aiter__(self):
        self._reset_responses()
        for request in self:
            yield request

    async def send(self, val):
        ''' send values from server to client '''
        print(f'RECEIVED FROM SERVER: {orjson.loads(val)}')
        self.values.append(orjson.loads(val))

    async def recv(self):
        ''' recv values from client to server '''
        response = self.responses.pop(0)
        print(f'RESPONDING {response}')
        if len(self.responses) == 0:
            self._reset_responses()
        return response

    async def close(self):
        pass


@pytest.fixture
def websocket():
    if os.path.isfile(dbpath):
        os.remove(dbpath)
    sample_database_file(dbpath)
    return FakeWebSocketClient()


@pytest.mark.asyncio
async def test_clientsocket_handler(websocket):

    serv = SocketServ(dbpath=dbpath,
                      domain=domain,
                      trafficDBpath=trafficDBpath)
    await serv.handler(websocket)
    await serv.dbconn.close()
