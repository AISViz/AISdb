# Web Application Development

The easiest way to install and utilize the AISDB web application components together is using the `docker-compose.yml` configuration included in this repository to run them as docker services. See the [Docker](about:blank/docker.html#docker) documentation for more info on how to do this. However, in some situations, such as developing or testing AISDB components, it may be convenient to run these services outside of a docker environment. This page documents how to run these services outside of docker, so that they can be tested or integrated in an existing environment.

The application has three primary components:

> * Database server (Back end web API)
> * Database storage (Postgres database)
> * Web application interface (JS front end)

And some secondary components:

> * Documentation webserver
> * AIS receiver client
> * AIS livestream proxy dispatcher

### Dependencies

The following software is requisite for each AISDB service:

* Database Storage
  * Postgresql Database Server
  * Postgresql Database Client Libraries
  * See the [Postgres Install Tutorial](https://www.postgresql.org/docs/current/tutorial-install.html)
* Database Server
  * Rustup, the Rust Compiler Toolchain [Install Rust](https://www.rust-lang.org/tools/install)
  * OpenSSL
* Web Application Front End
  * Rustup, the Rust Compiler Toolchain [Install Rust](https://www.rust-lang.org/tools/install)
  * Binaryen, the WebAssembly Compiler Toolchain [Binaryen](https://github.com/WebAssembly/binaryen)
  * Wasm-pack, the Rust WebAssembly Packaging Utility [Install wasm-pack](https://rustwasm.github.io/wasm-pack/installer/)
  * Clang, the C/C++ Compiler [Clang Download](https://releases.llvm.org/download.html)
  * OpenSSL Development Toolkit (e.g. `libssl-dev` on ubuntu/debian)
  * Pkg-config [pkg-config](https://en.wikipedia.org/wiki/Pkg-config)
  * NodeJS, the JavaScript Runtime Environment [Node.js download](https://nodejs.org/en)
* Documentation Server
  * Python [Download Python](https://www.python.org/downloads/)
  * Rustup, the Rust Compiler Toolchain [Install Rust](https://www.rust-lang.org/tools/install)
  * Sphinx Doc [Installing and Running Sphinx](https://www.sphinx-doc.org/en/master/#get-started)
  * Maturin Build System [Maturin User Guide](https://www.maturin.rs/)
  * NodeJS, the JavaScript Runtime Environment [Node.js download](https://nodejs.org/en)
* AIS Receiver Client
  * Rustup, the Rust Compiler Toolchain [Install Rust](https://www.rust-lang.org/tools/install)
* AIS Proxy Dispatcher
  * Rustup, the Rust Compiler Toolchain [Install Rust](https://www.rust-lang.org/tools/install)

### Database Storage

Ensure that the Postgres server is running by following the [Postgres Database Server Tutorial](https://www.postgresql.org/docs/current/server-start.html). The other web services will use this server for storage and retrieval of AIS data.

### Database Server

Configure the database server by setting the following environment variables for the postgres database connection:

```
PGPASSFILE=$HOME/.pgpass
PGUSER="postgres"
PGHOST="[fc00::9]"
PGPORT="5432"
```

Navigate to the `database_server` folder in the project repository, install it with cargo, and then run it.

```
cd database_server
cargo install --path .
aisdb-db-server
```

### Web Application Front End

A web interface client is included in the AISDB python package.

```
import os
from datetime import datetime, timedelta

import aisdb
import aisdb.web_interface
from aisdb.tests.create_testing_data import (
    sample_database_file,
    random_polygons_domain,
)

domain = random_polygons_domain()

example_dir = 'testdata'
if not os.path.isdir(example_dir):
    os.mkdir(example_dir)

dbpath = os.path.join(example_dir, 'example_visualize.db')
months = sample_database_file(dbpath)
start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
end = datetime(int(months[1][0:4]), int(months[1][4:6]) + 1, 1)


def color_tracks(tracks):
    ''' set the color of each vessel track using a color name or RGB value '''
    for track in tracks:
        track['color'] = 'red' or 'rgb(255,0,0)'
        yield track


with aisdb.SQLiteDBConn() as dbconn:
    qry = aisdb.DBQuery(
        dbconn=dbconn,
        dbpath=dbpath,
        start=start,
        end=end,
        callback=aisdb.sqlfcn_callbacks.valid_mmsi,
    )
    rowgen = qry.gen_qry()
    tracks = aisdb.track_gen.TrackGen(rowgen, decimate=False)
    tracks_segment = aisdb.track_gen.split_timedelta(tracks,
                                                     timedelta(weeks=4))
    tracks_colored = color_tracks(tracks_segment)

    if __name__ == '__main__':
        aisdb.web_interface.visualize(
            tracks_colored,
            domain=domain,
            visualearth=True,
            open_browser=True,
        )
```
