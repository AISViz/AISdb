# AISdb

AISdb is an open-source database system for storing, retrieving, analyzing, and visualizing Automatic Identification System (AIS) data. It serves the data needs of the maritime domain, supporting research, development, and operational safety with a Python interface backed by a Rust core and SQLite or PostgreSQL/TimescaleDB storage.

[![CI status](https://github.com/AISViz/AISdb/actions/workflows/CI.yml/badge.svg)](https://github.com/AISViz/AISdb/actions/workflows/CI.yml)
[![Test installation status](https://github.com/AISViz/AISdb/actions/workflows/Install.yml/badge.svg)](https://github.com/AISViz/AISdb/actions/workflows/Install.yml)
[![PyPI](https://img.shields.io/pypi/v/aisdb)](https://pypi.org/project/aisdb/)
[![Python versions](https://img.shields.io/pypi/pyversions/aisdb)](https://pypi.org/project/aisdb/)
[![License](https://img.shields.io/github/license/aisviz/aisdb)](https://github.com/AISViz/AISdb/blob/master/LICENSE)

## Features

- Efficient data management on SQLite and PostgreSQL/TimescaleDB, scaling from local files to server deployments
- A Python API usable across programming skill levels, with Rust handling decoding and other critical processing paths
- Data enrichment that joins AIS records with environmental and bathymetric datasets for marine context
- Analytical tools for complex queries, track processing, and statistical analysis directly against the database
- Dynamic visualization and export options in multiple formats for further analysis or reporting
- Modular design with optimized database schemas built for performance and scalability

AIS data comprises digital messages that ships and base stations transmit over VHF to exchange navigational and identification information. Messages arrive in standardized types covering dynamic position reports, static vessel metadata, and safety-related traffic, collected by on-shore antennas or low-orbit satellites. Beyond vessel tracking, this data underpins maritime research, environmental monitoring, and route optimization. Useful primers include the [AIS message types reference](https://arundaleais.github.io/docs/ais/ais_message_types.html), the [US Navigation Center](https://www.navcen.uscg.gov/ais-messages), the [IMO page on AIS transponders](https://www.imo.org/en/OurWork/Safety/Pages/AIS.aspx), and the [Wikipedia article](https://en.wikipedia.org/wiki/Automatic_identification_system).

## Installation

```sh
python -m venv AISdb  # Create and activate a virtual environment
source AISdb/bin/activate  # On Windows use `AISdb\Scripts\activate`
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
python -m venv AISdb  # Create a virtual environment
source AISdb/bin/activate  # Activation command for Windows `AISdb\Scripts\activate`
git clone https://github.com/AISViz/AISdb.git && cd AISdb  # Clone the repository
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > install-rust.sh  # Install Rust
/bin/bash install-rust.sh -q -y  # Run Rust installer
pip install --upgrade maturin[patchelf]  # Install Maturin for building
maturin develop --release --extras=test,docs  # Build AISdb package
```

Run the test suite with pytest.

```sh
pytest ./aisdb/tests/ --ignore=./aisdb/tests/test_014_marinetraffic.py --maxfail=10
```

## Documentation

[docs](https://aisviz.gitbook.io/documentation/) · [tutorials](https://aisviz.gitbook.io/tutorials/) · [API reference](https://aisviz.cs.dal.ca/AISdb/) · [website](https://aisviz.cs.dal.ca/)

## Related projects

- [AISdb-lite](https://github.com/AISViz/AISdb-lite) is the PostGIS and TimescaleDB first variant that ingests all AIS messages into two global hypertables
- [NOAA-Integrator](https://github.com/AISViz/NOAA-Integrator) acquires AIS data from NOAA Marine Cadastre and loads it into AISdb-aligned databases
- [Tutorials](https://github.com/AISViz/Tutorials) collects notebooks with worked examples for AISdb workflows

## License

This project is distributed under the terms of the GNU Affero General Public License v3.0 (AGPL-3.0). See [LICENSE](LICENSE) for details.
