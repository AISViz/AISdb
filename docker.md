# Docker

### Python Docker Quick Start

For most users, the recommended way to use the AISDB Python package is by installing it with pip. The main purpose of the Python docker images in this repository is to provide a build environment for the Python wheel files that can be used for pip installation, as well as providing a reference and testing environment. The `meridiancfi/aisdb:latest` image is based on `python:slim` with the AISDB package wheel installed. In the `meridiancfi/aisdb-manylinux:latest` image, wheel binary files are compiled from Rust and Python source code using a `manylinux` base docker image. The [Manylinux project](https://github.com/pypa/manylinux) aims to provide a convenient way to distribute binary Python extensions as wheels on Linux. To start the container, ensure that `docker` and `docker-compose` are installed, and enter into the command line:

```
docker pull meridiancfi/aisdb
docker run --interactive --tty --volume ./:/aisdb/ meridiancfi/aisdb
```

The current working directory will be mounted inside the container as a volume. The same can be achieved with `docker-compose` in the context of the repository compose file:

```
docker-compose --file ./docker-compose.yml run --rm --volume "`pwd`:/aisdb" aisdb-python
```

### Compose Services

In addition to the Python AISDB docker image, AISDB provides a complete suite of cloud services for storing, accessing, viewing, and networking AIS data. These services are defined in the `docker-compose.yml` file in the project repository. Cloud services are implemented in a microservices architecture, using a software-defined network defined in the compose file. Services include:

* docserver
  * NodeJS webserver to display Sphinx documentation (such as this webpage)
* webserver
  * NodeJS web application front-end interface for AISDB.
* postgresdb
  * AISDB Postgres database storage.
* db-
  * Web application back-end. Serves vectorized AIS tracks from the Postgres database in response to JSON-formatted queries.
* receiver
  * Listens for IP-enabled AIS transmitters over TLS/TCP or UDP channels, and stores incoming AIS messages in the postgres database. Optionally forwards filtered messages to a downstream UDP channel in raw or parsed format.
* upstream-ais
  * Live streaming AIS server. Listens for AIS messages forwarded by the `receiver` service, and reverse-proxy messages to downstream TCP clients or UDP channels. This service provides live-streaming data to the front end `webserver`.

To start all AISDB cloud services, navigate to the repository root directory and enter into the command line (root permissions may be required):

```
docker-compose up --build nginx db-server webserver docserver receiver upstream-ais
```

### Development and Deployment

The following services are used for the development and deployment of AISDB

* build-wheels
  * Build manylinux-compatible wheels for Python package distribution. The resulting wheel file is installed in aisdb-python.
* python-test
  * Run Python package integration tests. Based on the aisdb-python docker image..
* nginx
  *   NGINX gateway router. Routes incoming traffic to the appropriate cloud service. The following ports are exposed by the gateway:

      * `80`: redirects to port 443
      * `443`: serves web application endpoints over HTTPS
      * `9920`: Proxy for the `upstream-ais` service stream output (raw format).
      * `9922`: Proxy for the `receiver` service stream output (JSON format).

      The following endpoints are available over HTTPS:

      * `/`: Proxy for the `webserver` service.
      * `/doc`: Proxy for the `docserver` service.
      * `/ws`: Proxy for the `db-server` service.
      * `/stream`: Alias of port `9922`.
      * `/stream-raw`: Alias of port `9920`.
      * `/coverage`: Alias of `/docs/coverage`.
* certbot
  * TLS/SSL certificate renewal service. Renews certificates used by the `nginx` service. `privkey.pem` and `fullchain.pem` certificates are mounted in the `nginx` container inside directory `/etc/letsencrypt/live/$FQDN/`, where `$FQDN` is the domain name, e.g. `127.0.0.1`.

### Environment

Services running with docker compose will read environment variables from a `.env` file in the project root directory. An example `.env` file is included here:

```
# Front end config (bundled with Vite for NodeJS)

# AISDB database server and livestream server hostname
VITE_AISDBHOST='127.0.0.1'

# Bing maps token
# Get your token here: https://www.bingmapsportal.com/
#VITE_BINGMAPSKEY='<my-token-here>'

# Disable SSL/TLS for incoming livestream data.
# When using this option, the front end will connect to the livestream
# server at ws://$VITE_AISDBHOST:9922
# Otherwise, the front end will connect to wss://$VITE_AISDBHOST/stream
VITE_DISABLE_SSL_STREAM=1

# Disable SSL for the database server connection
VITE_DISABLE_SSL_DB=1

# Port used for database server connection.
# This setting is only active when VITE_DISABLE_SSL_DB is enabled,
# otherwise, an SSL connection will be made to https://VITE_AISDBHOST/ws
VITE_AISDBPORT=9924

# Allow users to query an unlimited amount of data at one time
#VITE_NO_DB_LIMIT=1

# if enabled, Bing Maps will be used for WMTS instead of OpenStreetMaps
VITE_BINGMAPTILES=1

# Default WMTS server
#VITE_TILESERVER="dev.virtualearth.net"
VITE_TILESERVER="aisdb.meridian.cs.dal.ca"


# Back end config

# Hostname
AISDBHOST='127.0.0.1'
#AISDBHOST='aisdb.meridian.cs.dal.ca'

# Database server port
AISDBPORT=9924

# Python database path
AISDBPATH='./AIS.sqlitedb'

# Postgres database client config
PGPASSFILE=$HOME/.pgpass
PGUSER="postgres"
PGHOST="[fc00::9]"
PGPORT="5432"

# Postgres database server config
# More info here: https://hub.docker.com/_/postgres/
POSTGRES_PASSWORD="example"

# This volume will be mounted for the postgres data directory
POSTGRES_VOLUME_DIR='./postgres_data'

# NGINX CSP header endpoints
NGINX_CSP_FRAME_ANCESTORS=""
#NGINX_CSP_FRAME_ANCESTORS="https://aisdb.meridian.cs.dal.ca/"


# Tests config

# Mounted AISDB metadata directory.
# Will be used during testing
AISDBDATADIR='/RAID0/ais/'
AISDBMARINETRAFFIC='/RAID0/ais/marinetraffic_V2.db'
```

### Interacting with Postgres Database

In some cases, Postgres may preferred over SQLite. Postgres offers improved concurrency and scalability over SQLite, at the cost of requiring more disk space and compute resources. The easiest and recommended way to use AISDB with the Postgresql database is via docker (to manually install dependencies, see [Web Application Development](about:blank/webapp.html#webapp)). To get started, navigate to the repository root directory, and ensure docker and docker-compose are installed. Start the AIS receiver, database server, and postgres database docker images. Sudo permissions may be required for Docker and docker-compose.

#### Python API

The receiver will fetch live data streaming from the MERIDIAN AIS network, and store it in the postgres database. Start the AIS receiver and Postgres database services from the command line with docker-compose:

```
export POSTGRES_PASSWORD="example"
docker-compose up --build receiver postgresdb
```

The Postgres database may be interfaced using Python in the same manner as the default SQLite database by using [`aisdb.database.dbconn.PostgresDBConn`](about:blank/api/aisdb.database.dbconn.html#aisdb.database.dbconn.PostgresDBConn) as a drop-in replacement for the default [`aisdb.database.dbconn.DBConn`](about:blank/api/aisdb.database.dbconn.html#aisdb.database.dbconn.DBConn) that uses SQLite.

```
import os
from aisdb.database.dbconn import PostgresDBConn

# keyword arguments
dbconn = PostgresDBConn(
    hostaddr='127.0.0.1',
    user='postgres',
    port=5432,
    password=os.environ.get('POSTGRES_PASSWORD'),
)

# Alternatively, connect using a connection string:
dbconn = PostgresDBConn('Postgresql://localhost:5433')
```

The resulting dbconn may then be used similar to how `DBConn` is used in the [Intro Doc](about:blank/intro.html#intro)

#### Web API

Start the AIS receiver, Postgres database, and database webserver services from the command line using the following command. See Docker for more info on docker services. Alternatively, the services can be run in a local environment instead of a docker environment as described in [Web Application Development](about:blank/webapp.html#webapp).

```
docker-compose up --build receiver postgresdb db-server
```

The receiver service will listen for new data from MERIDIAN’s AIS receiver, and store it in the postgres database. The db-server service provides a web API for the AIS data stored in the postgres database. This listens for WebSocket connections on port 9924, and returns JSON-formatted vessel tracks in response to queries. The following Python code provides an example of how to asynchronously query AIS data from db-server. This code can either run in a local Python environment or in the aisdb-python docker image. While this example uses Python, the web API can be accessed using any language or package using the [Websocket Protocol](https://www.rfc-editor.org/rfc/rfc6455), such as JavaScript, as long as requests are formatted as utf8-encoded JSON.

```
# Python standard library packages
from datetime import datetime, timedelta
import asyncio
import os
import sys

# These packages need to be installed with pip
import orjson
import websockets.client

# Query the MERIDIAN web API, or reconfigure the URL with an environment variable
db_hostname = 'wss://aisdb.meridian.cs.dal.ca/ws'
db_hostname = os.environ.get('AISDBHOST', db_hostname)

# Default docker IPv6 address and port number.
# See docker-compose.yml for configuration.
# db_hostname = 'ws://[fc00::6]:9924'


class DatabaseRequest():
    ''' Methods in this class are used to generate JSON-formatted AIS requests,
        as an interface to the AISDB WebSocket API.
        The orjson library is used for fast utf8-encoded JSON serialization.
    '''

    def validrange() -> bytes:
        return orjson.dumps({'msgtype': 'validrange'})

    def zones() -> bytes:
        return orjson.dumps({'msgtype': 'zones'})

    def track_vectors(x0: float, y0: float, x1: float, y1: float,
                      start: datetime, end: datetime) -> dict:
        ''' database query for a given time range and bounding box '''
        return orjson.dumps(
            {
                "msgtype": "track_vectors",
                "start": int(start.timestamp()),
                "end": int(end.timestamp()),
                "area": {
                    "x0": x0,
                    "x1": x1,
                    "y0": y0,
                    "y1": y1
                }
            },
            option=orjson.OPT_SERIALIZE_NUMPY)


async def query_valid_daterange(db_socket: websockets.client, ) -> dict:
    ''' Query the database server for minimum and maximum time range values.
        Values are formatted as unix epoch seconds, i.e. the total number of
        seconds since Jan 1 1970, 12am UTC.
    '''

    # Create a new server daterange request using a dictionary
    query = DatabaseRequest.validrange()
    await db_socket.send(query)

    # Wait for server response, and parse JSON from the response binary
    response = orjson.loads(await db_socket.recv())
    print(f'Received daterange response from server: {response}')

    # Print the server response
    start = datetime.fromtimestamp(response['start'])
    end = datetime.fromtimestamp(response['end'])

    return {'start': start, 'end': end}


async def query_tracks_24h(db_socket: websockets.client, ):
    ''' query recent ship movements near Dalhousie '''

    boundary = {'x0': -64.8131, 'x1': -62.2928, 'y0': 43.5686, 'y1': 45.3673}
    query = DatabaseRequest.track_vectors(
        start=datetime.now() - timedelta(hours=24),
        end=datetime.now(),
        **boundary,
    )
    await db_socket.send(query)

    response = orjson.loads(await db_socket.recv())
    while response['msgtype'] == 'track_vector':
        print(f'got track vector data:\n\t{response}')
        response = orjson.loads(await db_socket.recv())
    print(response, end='\n\n\n')


async def query_zones(db_socket: websockets.client, ):
    await db_socket.send(DatabaseRequest.zones())

    response = orjson.loads(await db_socket.recv())
    while response['msgtype'] == 'zone':
        print(f'got zone polygon data:\n\t{response}')
        response = orjson.loads(await db_socket.recv())
    print(response, end='\n\n\n')


async def main():
    ''' asynchronously query the web API for valid timerange, 24 hours of
        vectorized vessel data, and zone polygons
    '''
    useragent = 'AISDB WebSocket Client'
    useragent += f' ({os.name} {sys.implementation.cache_tag})'

    async with websockets.client.connect(
            db_hostname, user_agent_header=useragent) as db_socket:
        daterange = await query_valid_daterange(db_socket)
        print(
            f'start={daterange["start"].isoformat()}\t'
            f'end={daterange["end"].isoformat()}',
            end='\n\n\n')

        await query_tracks_24h(db_socket)

        await query_zones(db_socket)


if __name__ == '__main__':
    asyncio.run(main())
```

### Interacting with the Map
