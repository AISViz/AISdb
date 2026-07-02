# AISdb

[![PyPI](https://img.shields.io/pypi/pyversions/aisdb)](https://pypi.org/project/aisdb/)
[![Release](https://img.shields.io/github/v/release/aisviz/aisdb)](https://github.com/AISViz/AISdb/releases)
[![Commits](https://img.shields.io/github/commit-activity/t/aisviz/aisdb)](https://github.com/AISViz/AISdb/commits/master)
[![License](https://img.shields.io/github/license/aisviz/aisdb)](https://github.com/AISViz/AISdb/blob/master/LICENSE)
[![CI status](https://github.com/AISViz/AISdb/actions/workflows/CI.yml/badge.svg)](https://github.com/AISViz/AISdb/actions/workflows/CI.yml)
[![CodeQL status](https://github.com/AISViz/AISdb/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/AISViz/AISdb/actions/workflows/github-code-scanning/codeql)
[![Test installation status](https://github.com/AISViz/AISdb/actions/workflows/Install.yml/badge.svg)](https://github.com/AISViz/AISdb/actions/workflows/Install.yml)

## AISdb Package Overview

Welcome to AISdb, the premier open-source database management system for storing, retrieving, analyzing, and visualizing Automatic Identification System (AIS) data. Our system caters to the vast data needs of the maritime industry, making it a vital tool for research, development, and operational safety.

**Key Features:**

- **Efficient Data Management:** AISdb leverages SQLite and PostgreSQL to provide scalable solutions for local and server-based data handling needs.
- **Python Interface:** Offering a Python-based API for ease of use across different programming skill levels, ensuring broad accessibility and efficiency.
- **Data Enrichment:** AISdb integrates AIS data with environmental and bathymetric datasets, allowing users to enrich maritime traffic data with contextual information about the marine environment.
- **Advanced Analytical Tools:** Features a comprehensive suite of analytical tools for conducting complex queries, processing data, and performing statistical analyses directly within the database.
- **Data Visualization and Export:** Supports dynamic data visualization and provides options for data export in various formats for further analysis or reporting.
- **Modular and Scalable:** AISdb is designed with performance, scalability, and ease of use, featuring optimized database schemas and employing Rust for critical data processing tasks.

**Documentation and Resources:**

- [AISViz Website](https://aisviz.github.io)
- [AISdb ReadTheDocs](https://aisviz.cs.dal.ca/AISdb/)
- [AISdb GitBook Tutorials](https://aisviz.gitbook.io/tutorials)
- [AISdb GitBook Documentation](https://aisviz.gitbook.io/documentation)
- [AISViz ChatBot A](https://huggingface.co/spaces/vaishnaveswar/AIVIZ-BOT) (with Gemini, open source)
- [AISViz ChatBot B](https://chat.openai.com/g/g-hTTH0rUBv-aisdb-assistant) (with GPT-4o, subscription based)

## What is AIS Data?

AIS data comprises digital messages that ships and AIS base stations transmit to exchange navigational and identification information. This information is pivotal for ensuring the safety and efficiency of maritime traffic, offering real-time insight into other vessels' locations, intentions, and navigational status.

### Structured Data Exchange

AIS messages come in various types, serving different informational needs, from dynamic vessel information to safety-related messages. They play a crucial role in maritime operations, aiding navigation, traffic management, and risk mitigation.

### Encoding and Transmission

AIS messages are transmitted using VHF radio frequencies, which ensures reliable coverage even in harsh weather conditions. The messages are encoded in a standardized format that promotes interoperability among different AIS equipment manufacturers. On-shore antennas or low-orbit satellites collect these messages, and the temporal resolution of the data varies with the collection method used.

### Significance in Maritime Operations

Beyond tracking, AIS data underpins operations across the maritime sector, enhancing situational awareness and facilitating informed decision-making. Applications of AIS data extend to maritime research, environmental monitoring, and developing optimization algorithms for shipping routes.

**Further reading on AIS:**

- [AIS Message Types](https://arundaleais.github.io/docs/ais/ais_message_types.html)
- [Navigation Center](https://www.navcen.uscg.gov/ais-messages)
- [AIS transponders](https://www.imo.org/en/OurWork/Safety/Pages/AIS.aspx)
- [Wikipedia Article](https://en.wikipedia.org/wiki/Automatic_identification_system)

## Installing

Easily set up AISdb on your system with the following commands:

```sh
python -m venv AISdb  # Create and activate a virtual environment
source AISdb/bin/activate  # On Windows use `AISdb\Scripts\activate`
pip install aisdb  # Install AISdb from PyPI
```

**Note:** The current version of AISdb uses TimescaleDB instead of vanilla PostgreSQL. TimescaleDB is an extension built on top of PostgreSQL, specifically optimized for time-series data featuring automatic partitioning and compression. To enhance performance, we recommend fine-tuning your database with the TimescaleDB installer and configuring the database to use 7-day data chunks.

## Developing

To contribute to AISdb or develop further, set up your development environment with these steps:

```sh
python -m venv AISdb  # Create a virtual environment
source AISdb/bin/activate  # Activation command for Windows `AISdb\Scripts\activate`
git clone https://github.com/AISViz/AISdb.git && cd AISdb  # Clone the repository
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > install-rust.sh  # Install Rust
/bin/bash install-rust.sh -q -y  # Run Rust installer
pip install --upgrade maturin[patchelf]  # Install Maturin for building
maturin develop --release --extras=test,docs  # Build AISdb package
```
