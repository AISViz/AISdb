# Contributing to AISdb

We appreciate your interest in contributing to AISdb. This document outlines how to contribute to the project and what to expect during the process.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Questions and Discussions](#questions-and-discussions)
- [How to Contribute](#how-to-contribute)
- [Development Setup](#development-setup)
- [Quality Gates](#quality-gates)
- [Community and Support](#community-and-support)

## Code of Conduct

Before contributing, please read our [Code of Conduct](CODE_OF_CONDUCT.md) to understand the expectations for behavior. All participants are required to adhere to the Code of Conduct.

## Questions and Discussions

Have a question or want to discuss a topic related to AISdb? Please check the documentation first. If your question remains unanswered, feel free to open an issue on our repository to start a discussion.

## How to Contribute

You can contribute in many ways, such as improving documentation, submitting bug reports, proposing new features, and submitting pull requests with code changes.

### Reporting Bugs

Bugs are reported as GitHub issues. Before creating a bug report, please check the issue tracker to avoid duplication. When creating a bug report, please include as many details as possible:

- Your environment details (OS, database version, Python and Rust versions)
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Any relevant log snippets or error messages

Security vulnerabilities must not be reported as public issues. See [SECURITY.md](SECURITY.md) for the private reporting process.

### Suggesting Enhancements

We welcome suggestions for enhancements. To suggest an enhancement:

- Open an issue on our repository.
- Clearly describe the enhancement with as much detail as possible.
- Explain why this enhancement would be beneficial to AISdb users.

### Contributing Code

For code contributions:

- Fork the repository and create a feature branch.
- Write clear code and ensure it passes the quality gates below.
- Make sure your changes do not break existing functionality.
- Submit a pull request with a clear description of what you changed and why.

### Improving Documentation

Good documentation is crucial:

- Submit improvements as pull requests.
- Explain what you improved and why.

## Development Setup

AISdb is a mixed Rust and Python project built with maturin. A working development environment needs:

1. **Rust:** Install the stable toolchain via [rustup](https://rustup.rs/), then add the WebAssembly target:

   ```sh
   rustup target add wasm32-unknown-unknown
   cargo install --locked wasm-pack wasm-bindgen-cli wasm-opt
   ```

2. **Node.js 20:** Required to bundle the web assets during the build.

3. **Python 3.10 or newer:** Create a virtual environment and build the package in development mode:

   ```sh
   python -m venv .venv
   source .venv/bin/activate
   python -m pip install --upgrade pip maturin
   maturin develop --release --extras=test
   ```

## Quality Gates

Continuous integration enforces the following checks on every pull request. Run them locally before submitting:

- **Rust (aisdb_lib):** From the `aisdb_lib/` directory:

  ```sh
  cargo fmt --check
  cargo clippy --features sqlite,postgres -- -D warnings
  cargo test --features sqlite,postgres
  ```

- **Rust (database_server):** `cargo check` must pass in the `database_server/` directory.

- **Python:** The test suite must pass. Pytest configuration lives in `pyproject.toml` under `[tool.pytest.ini_options]`:

  ```sh
  pytest ./aisdb/tests/
  ```

  Some tests require a local PostgreSQL server with the TimescaleDB extension; the CI workflows in `.github/workflows/` document a working setup for Linux, macOS, and Windows.

- **SQL:** All SQL statements must be parameterized. Never build SQL from string interpolation or concatenation of user input.

- **Wheels:** Release wheels are built by CI for Linux, Windows, and macOS, and must be installable with both pip and uv. You do not need to build wheels locally.

## Community and Support

Join the AISdb community:

- Star the project on GitHub.
- Share the project with colleagues who work with AIS data.
- Contribute to the conversations in issues.
- Share your experiences with AISdb at meetups or conferences.

Thank you for your contributions to AISdb!
