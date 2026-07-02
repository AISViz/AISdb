"""Shared test configuration.

Several test modules exercise raster and land-mask features whose source
archives are multi-gigabyte downloads from GitHub release assets. They are
skipped unless AISDB_RASTER_TESTS=1 is set (and GitHub is reachable), so
routine runs stay fast and offline runs stay reliable.
"""

import os
import socket

import pytest

DATA_HOST = ("github.com", 443)

# Test modules that require downloads from DATA_HOST
_EXTERNAL_DATA_TESTS = (
    "test_010_network_graph",
    "test_015_raster_dist",
    "test_016_bathymetry",
    "test_017_inland_denoising",
)

_data_host_up = None


def _data_host_reachable(timeout: float = 5.0) -> bool:
    try:
        with socket.create_connection(DATA_HOST, timeout=timeout):
            return True
    except OSError:
        return False


def pytest_collection_modifyitems(config, items):
    global _data_host_up
    opted_in = os.environ.get("AISDB_RASTER_TESTS") == "1"
    skip_marker = None
    for item in items:
        if any(name in item.nodeid for name in _EXTERNAL_DATA_TESTS):
            if not opted_in:
                item.add_marker(pytest.mark.skip(
                    reason="raster download tests skipped (set AISDB_RASTER_TESTS=1 to run)"
                ))
                continue
            if _data_host_up is None:
                _data_host_up = _data_host_reachable()
            if not _data_host_up:
                if skip_marker is None:
                    skip_marker = pytest.mark.skip(
                        reason=f"external data host {DATA_HOST[0]}:{DATA_HOST[1]} unreachable"
                    )
                item.add_marker(skip_marker)
