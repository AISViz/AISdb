'''
Run the server in a development environment:
    python -m flask --app aisdb/rest_api.py run

Deploying flask to production with IIS:
    <https://learn.microsoft.com/en-us/visualstudio/python/configure-web-apps-for-iis-windows>
'''
from datetime import datetime, timedelta
from tempfile import SpooledTemporaryFile
import gzip
import os
import secrets

import aisdb
from aisdb import PostgresDBConn, DBQuery

from flask import (
    Flask,
    Markup,
    Response,
    request,
)

# Maximum bytes for client CSV files stored in memory before spilling to disk.
# Set the directory for files exceeding this value with $TMPDIR
MAX_CLIENT_MEMORY = 1024 * 1e6  # 1GB

# TODO: auth
app = Flask("aisdb-rest-api")
app.config.from_mapping(SECRET_KEY=secrets.token_bytes())

# postgres database connection arguments
db_args = dict(
    host=os.environ.get('AISDB_REST_DBHOST', 'fc00::17'),
    port=os.environ.get('AISDB_REST_DBPORT', 5431),
    user=os.environ.get('AISDB_REST_DBUSER', 'postgres'),
    password=os.environ.get('AISDB_REST_DBPASSWORD', 'devel'),
)

# verify database connection and prepare an example GET request
with PostgresDBConn(**db_args) as dbconn:
    db_rng = dbconn.db_daterange
    end = db_rng['end']
    start = max(db_rng['start'], db_rng['end'] - timedelta(days=31))
    default_query = {
        'start': int(datetime(start.year, start.month, start.day).timestamp()),
        'end': int(datetime(end.year, end.month, end.day).timestamp()),
        'xmin': -65,
        'xmax': -62,
        'ymin': 43,
        'ymax': 45,
    }


@app.route('/', methods=['GET', 'POST'])
def download():
    http_qry = dict(request.args)
    print(f'received request {http_qry} from client {request.remote_addr}')

    example_GET_qry = '<div id="base_uri" style="display: inline;" ></div>?' + '&'.join(
        f'{k}={v}' for k, v in default_query.items())

    # validate the request parameters
    need_keys = set(default_query.keys())
    recv_keys = set(http_qry.keys())
    missing = need_keys - recv_keys

    if len(recv_keys) == 0:
        return Markup(
            '<h3>AIS REST API</h3>'
            '<p>'
            'Query AIS message history using time and coordinate region to download a CSV data export.&ensp;'
            'Begin request using a GET or POST request to this endpoint.'
            '</p>'
            '<p>Description of query parameters:</p>'
            '<ul>'
            '<li>xmin: minimum longitude (decimal degrees)</li>'
            '<li>xmax: maximum longitude (decimal degrees)</li>'
            '<li>ymin: minimum latitude (decimal degrees)</li>'
            '<li>ymax: maximum latitude (decimal degrees)</li>'
            '<li>start: beginning timestamp (epoch seconds)</li>'
            '<li>end: end timestamp (epoch seconds)</li>'
            '</ul>'
            '<p>'
            'Requests are limited to 31 days at a time. Data is available from'
            f' <code>{db_rng["start"]}</code>'
            ' to'
            f' <code>{db_rng["end"]}</code>.'
            '</p>'
            '<p>Example GET request:</p>'
            f'<code>{example_GET_qry}<code>'
            #'<form action="/" method="POST">'
            #'</form>'
            '''
            <script>
            document.getElementById("base_uri").innerHTML = window.location;

            //let status_display = function() { };
            </script>
            ''')

    if len(missing) > 0:
        return Markup(f'Error: missing keys from request: {missing}<br>'
                      f'example:<br><code>{example_GET_qry}<code>')

    # convert parameter types from string
    http_qry['start'] = datetime.utcfromtimestamp(int(http_qry['start']))
    http_qry['end'] = datetime.utcfromtimestamp(int(http_qry['end']))
    for arg in ['xmin', 'xmax', 'ymin', 'ymax']:
        http_qry[arg] = float(http_qry[arg])

    # error handling for invalid requests
    if http_qry['end'] - http_qry['start'] > timedelta(days=31):
        return Markup("Error: a maximum of 31 days can be queried at once")

    if http_qry['end'] <= http_qry['start']:
        return Markup("Error: end must occur after start")

    if not (-180 <= http_qry['xmin'] < http_qry['xmax'] <= 180):
        return Markup("Error: invalid longitude range")

    if not (-90 <= http_qry['ymin'] < http_qry['ymax'] <= 90):
        return Markup("Error: invalid longitude range")

    with PostgresDBConn(**db_args) as dbconn:
        buf = SpooledTemporaryFile(max_size=MAX_CLIENT_MEMORY)

        dbqry = DBQuery(dbconn=dbconn,
                        callback=aisdb.sqlfcn_callbacks.in_bbox_time_validmmsi,
                        **http_qry).gen_qry(
                            fcn=aisdb.sqlfcn.crawl_dynamic_static,
                            verbose=False)

        tracks = aisdb.TrackGen(dbqry, decimate=0.0001)
        #csv_rows = aisdb.proc_util.tracks_csv(tracks)
        '''
        def generate(csv_rows):
            start_qry = next(csv_rows)
            yield ','.join(map(str, start_qry)) + '\n'
            yield ','.join(map(str, start_qry)) + '\n'
            for row in csv_rows:
                yield ','.join(map(str, row)) + '\n'

        lines = generate(csv_rows)
        # start query generation so that the DBConn object isnt garbage collected
        _ = next(lines)
        '''
        lines = aisdb.proc_util.write_csv(tracks, buf)
        buf.flush()

        download_name = f'ais_{http_qry["start"].date()}_{http_qry["end"].date()}.csv'
        buf.seek(0)
        count = sum(1 for line in buf)
        print(f'sending {count} rows to client {request.remote_addr}',
              flush=True)
        buf.seek(0)
        return Response(
            gzip.compress(buf.read(), compresslevel=9),
            #gzip.compress(lines, compresslevel=7),
            mimetype='application/csv',
            headers={
                'Content-Disposition': f'attachment;filename={download_name}',
                'Content-Encoding': 'gzip',
                'Keep-Alive': 'timeout=0'
            },
        )
        try:
            pass
        except aisdb.track_gen.EmptyRowsException:
            buf.close()
            return Markup("No results found for query")
        except Exception as err:
            raise err


if __name__ == '__main__':
    app.run()
