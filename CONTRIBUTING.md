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
- Submit a pull request against the `master` branch with a clear description of what you changed and why.

### Improving Documentation

Good documentation is crucial:

- Submit improvements as pull requests.
- Explain what you improved and why.

## Development Setup

AISdb is a mixed Rust and Python project built with maturin. The maturin build also compiles the WebAssembly map client and bundles the web assets, so a working development environment needs the Rust, wasm, and Node toolchains in addition to Python.

1. **Rust:** Install the stable toolchain via [rustup](https://rustup.rs/), then add the WebAssembly target and the wasm build tools:

   ```sh
   rustup target add wasm32-unknown-unknown
   cargo install --locked wasm-pack wasm-bindgen-cli wasm-opt
   ```

2. **Node.js 20:** Required to run `npm install` and the Vite builds that bundle the web assets during the maturin build.

3. **Python 3.10 or newer:** Create a virtual environment and build the package in development mode:

   ```sh
   python -m venv .venv
   source .venv/bin/activate
   python -m pip install --upgrade pip maturin
   maturin develop --release --extras=test
   ```

   The `test` extra installs the pytest tooling (coverage, pytest, pytest-cov) used by the quality gates below. This is the same command the CI installation workflow uses.

## Quality Gates

### Automated checks (CI)

Continuous integration runs the checks below on pull requests. Run them locally before submitting.

- **Rust (aisdb_lib):** From the `aisdb_lib/` directory:

  ```sh
  cargo fmt --check
  cargo clippy --features sqlite,postgres -- -D warnings
  cargo test --features sqlite,postgres
  ```

  The `sqlite` and `postgres` features are already the crate defaults; the flags are kept to mirror CI.

- **Rust (database_server):** `cargo check` must pass in the `database_server/` directory.

- **Python:** The test suite must pass. Pytest configuration lives in `pyproject.toml` under `[tool.pytest.ini_options]` (testpaths `aisdb/tests`):

  ```sh
  pytest ./aisdb/tests/
  ```

  Many tests require a local PostgreSQL 17 server; the TimescaleDB paths additionally need the TimescaleDB extension. CI installs PostgreSQL 17 on Linux, macOS, and Windows, and installs TimescaleDB on Linux and macOS (the Windows job runs against plain PostgreSQL 17). The workflows in `.github/workflows/` document a working setup for each OS.

- **Wheels:** Release wheels are built by CI for Linux, Windows, and macOS. A representative Linux wheel is smoke-tested by installing and importing it under uv, and every artifact's distribution metadata is validated with `twine check`. You do not need to build wheels locally.

### Coding standards

These are enforced by reviewers rather than an automated CI linter, so keep them in mind before opening a pull request.

- **SQL:** All SQL statements must be parameterized. Never build SQL from string interpolation or concatenation of user input.

## Community and Support

Join the AISdb community:

- Star the project on GitHub.
- Share the project with colleagues who work with AIS data.
- Contribute to the conversations in issues.
- Share your experiences with AISdb at meetups or conferences.

Thank you for your contributions to AISdb!
