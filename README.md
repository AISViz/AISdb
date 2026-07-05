# AISdb

AISdb is an open-source database system for storing, retrieving, analyzing, and visualizing Automatic Identification System (AIS) data. It serves the data needs of the maritime domain, supporting research, development, and operational safety with a Python interface backed by a Rust core and SQLite or PostgreSQL/TimescaleDB storage. AISdb is developed and maintained by the [MAPS Lab](https://mapslab.tech/) at Dalhousie University, continuing work that began under the [MERIDIAN](https://meridian.cs.dal.ca) initiative.

## Features

- Efficient data management on SQLite and PostgreSQL/TimescaleDB, scaling from local files to server deployments
- A Python API usable across programming skill levels, with Rust handling decoding and other critical processing paths
- Data enrichment that joins AIS records with environmental, bathymetric, and weather datasets for marine context
- Analytical tools for complex queries, track processing, network graph generation, H3 spatial discretization, and statistical analysis directly against the database
- Dynamic visualization and export options in multiple formats for further analysis or reporting
- Modular design with optimized database schemas built for performance and scalability

AIS data comprises digital messages that ships and base stations transmit over VHF to exchange navigational and identification information. Messages arrive in standardized types covering dynamic position reports, static vessel metadata, and safety-related traffic, collected by on-shore antennas or low-orbit satellites. Beyond vessel tracking, this data underpins maritime research, environmental monitoring, and route optimization. Useful primers include the [AIS message types reference](https://arundaleais.github.io/docs/ais/ais_message_types.html), the [US Navigation Center](https://www.navcen.uscg.gov/ais-messages), the [IMO page on AIS transponders](https://www.imo.org/en/OurWork/Safety/Pages/AIS.aspx), and the [Wikipedia article](https://en.wikipedia.org/wiki/Automatic_identification_system).

## Installation

```sh
python -m venv .venv  # Create and activate a virtual environment
source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
pip install aisdb  # Install AISdb from PyPI
```

The current version of AISdb uses TimescaleDB instead of vanilla PostgreSQL. TimescaleDB is an extension built on top of PostgreSQL, optimized for time-series data with automatic partitioning and compression. For best performance, tune the database with the TimescaleDB installer and configure 7-day data chunks.

## Quick start

Decode raw AIS messages into a local SQLite database, then query them back as vessel tracks.

```python
from datetime import datetime, timedelta

import aisdb

start = datetime(2021, 11, 1)
end = start + timedelta(days=1)

with aisdb.DBConn("ais.sqlite") as dbconn:
    aisdb.decode_msgs(
        filepaths=["aisdb/tests/testdata/test_data_20211101.nm4"],
        dbconn=dbconn,
        source="TESTING",
    )
    qry = aisdb.DBQuery(
        dbconn=dbconn,
        start=start,
        end=end,
        callback=aisdb.sqlfcn_callbacks.in_timerange_validmmsi,
    )
    for track in aisdb.TrackGen(qry.gen_qry(), decimate=False):
        print(track["mmsi"], len(track["time"]))
```

## Development

To contribute to AISdb or develop it further, set up a build environment with these steps.

```sh
python -m venv .venv  # Create a virtual environment
source .venv/bin/activate  # Activation command for Windows `.venv\Scripts\activate`
git clone https://github.com/MAPS-Lab/AISdb.git && cd AISdb  # Clone the repository
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > install-rust.sh  # Install Rust
/bin/bash install-rust.sh -q -y  # Run Rust installer
pip install --upgrade maturin[patchelf]  # Install Maturin for building
maturin develop --release --extras=test  # Build AISdb package
```

Run the test suite with pytest.

```sh
pytest ./aisdb/tests/ --maxfail=10
```

The Rust engine has its own gates, run from the `aisdb_lib` directory with `cargo fmt --check`, `cargo clippy --features sqlite,postgres`, and `cargo test --features sqlite,postgres`. Continuous integration builds wheels for Linux, macOS, and Windows, smoke-tests the Linux wheel installation with uv, and runs the test suite against PostgreSQL 17 (with TimescaleDB on Linux and macOS) for pushes to `master`, tags, and pull requests.

## Services

Beyond the Python package, the repository contains the deployable pieces of a self-hosted AISdb stack, each in its own top-level folder.

- `receiver/` decodes live NMEA streams from an antenna or aggregator feed, persists them to a local SQLite or PostgreSQL database, and rebroadcasts to downstream clients
- `database_server/` serves vectorized vessel tracks from PostgreSQL over a WebSocket API
- `aisdb_web/` is the JavaScript and WebAssembly map front end bundled into the Python wheel
- `client_webassembly/` compiles the in-browser geometry processing used by the map

## Raster data

Bathymetry, distance-to-shore, and distance-to-port rasters used by `aisdb.webdata` are downloaded on first use from the [data release](https://github.com/MAPS-Lab/AISdb/releases/tag/data-v1) of this repository.

## Documentation

[docs](https://aisviz.gitbook.io/documentation/) · [tutorials](https://aisviz.gitbook.io/tutorials/) · [API reference](https://aisviz.cs.dal.ca/AISdb/) · [website](https://aisviz.cs.dal.ca/)

## Related projects

- [AISdb-lite](https://github.com/MAPS-Lab/AISdb-lite) is the PostGIS and TimescaleDB first variant that ingests all AIS messages into two global hypertables
- [NOAA-Integrator](https://github.com/MAPS-Lab/NOAA-Integrator) acquires AIS data from NOAA Marine Cadastre and loads it into AISdb-aligned databases
- [Tutorials](https://github.com/MAPS-Lab/AISdb-Tutorials) collects notebooks with worked examples for AISdb workflows

## License

This project is distributed under the terms of the GNU Affero General Public License v3.0 (AGPL-3.0). See [LICENSE](LICENSE) for details.
