"""Shared test configuration.

Several test modules exercise raster and land-mask features whose source
archives are served from the research data host. When that host cannot be
reached (firewalled runner, host down), those tests are skipped instead of
failing, so the suite stays a reliable signal for code regressions.
"""

import socket

import pytest

DATA_HOST = ("bigdata5.research.cs.dal.ca", 80)

# Test modules that require downloads from DATA_HOST
_EXTERNAL_DATA_TESTS = (
    "test_010_network_graph",
    "test_015_raster_dist",
    "test_016_bathymetry",
    "test_017_inland_denoising",
)

_data_host_up = None


def _data_host_reachable(timeout: float = 5.0) -> bool:
    # A bare TCP connect is not enough: intermediate firewalls accept the
    # handshake and reset on data. Require an actual HTTP response.
    request = f"HEAD / HTTP/1.1\r\nHost: {DATA_HOST[0]}\r\nConnection: close\r\n\r\n"
    try:
        with socket.create_connection(DATA_HOST, timeout=timeout) as conn:
            conn.settimeout(timeout)
            conn.sendall(request.encode())
            return bool(conn.recv(64))
    except OSError:
        return False


def pytest_collection_modifyitems(config, items):
    global _data_host_up
    skip_marker = None
    for item in items:
        if any(name in item.nodeid for name in _EXTERNAL_DATA_TESTS):
            if _data_host_up is None:
                _data_host_up = _data_host_reachable()
            if not _data_host_up:
                if skip_marker is None:
                    skip_marker = pytest.mark.skip(
                        reason=f"external data host {DATA_HOST[0]}:{DATA_HOST[1]} unreachable"
                    )
                item.add_marker(skip_marker)
