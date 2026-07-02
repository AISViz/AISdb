"""read vessel information such as deadweight tonnage from a local
marinetraffic.com metadata database.

Web scraping support was removed (see GitHub issue #81): marinetraffic.com
anti-bot measures and paywall make scraping unsupportable. Existing local
traffic databases remain fully readable.
"""

import os
import sqlite3

import numpy as np

from aisdb import sqlpath

_SCRAPING_REMOVED_MSG = (
    "MarineTraffic web scraping was removed from AISdb (GitHub issue #81): "
    "anti-bot measures and paywall make it unsupportable. Only reading from "
    "an existing local traffic database is supported."
)

_createtable_sqlfile = os.path.join(sqlpath, "createtable_webdata_marinetraffic.sql")
with open(_createtable_sqlfile, "r") as f:
    _createtable_sql = f.read()


def _nullinfo(track):
    return {
        "mmsi": track["mmsi"],
        "imo": track["imo"] if "imo" in track.keys() else 0,
        "name": (
            track["vessel_name"]
            if "vessel_name" in track.keys() and track["vessel_name"] is not None
            else ""
        ),
        "vesseltype_generic": None,
        "vesseltype_detailed": None,
        "callsign": None,
        "flag": None,
        "gross_tonnage": None,
        "summer_dwt": None,
        "length_breadth": None,
        "year_built": None,
        "home_port": None,
        "error404": 1,
    }


def _vessel_info_dict(dbconn: sqlite3.Connection) -> dict:
    if not isinstance(dbconn, sqlite3.Connection):
        raise ValueError(
            f"Invalid database connection type: {dbconn}. "
            f"Requires: {sqlite3.Connection}"
        )
    cur = dbconn.cursor()
    res = cur.execute(
        "SELECT * FROM webdata_marinetraffic WHERE error404 != 1"
    ).fetchall()
    return {r["mmsi"]: r for r in res}


def vessel_info(tracks, dbconn: sqlite3.Connection):
    """append vessel metadata from a local marinetraffic database to track
    dictionaries.

    args:
        tracks (iter)
            collection of track dictionaries
        dbconn (sqlite3.Connection)
            connection to the local traffic database, e.g. the
            :attr:`aisdb.webdata.marinetraffic.VesselInfo.trafficDB`
            attribute
    """
    if not isinstance(dbconn, sqlite3.Connection):
        raise ValueError(
            f"Invalid database connection type: {dbconn}. "
            f"Requires: {sqlite3.Connection}"
        )
    meta = _vessel_info_dict(dbconn)
    for track in tracks:
        assert isinstance(track, dict)
        track["static"] = set(track["static"]).union({"marinetraffic_info"})
        if track["mmsi"] in meta.keys():
            track["marinetraffic_info"] = meta[track["mmsi"]]
        else:
            track["marinetraffic_info"] = _nullinfo(track)
        yield track


class VesselInfo:
    """read vessel metadata from a local marinetraffic database.

    opens (creating it first if missing) the local traffic database at
    ``trafficDBpath``. the open connection is available as the
    ``trafficDB`` attribute. use as a context manager, or call
    :meth:`close`, to release the connection.

    args:
        trafficDBpath (string)
            path to the local vessel traffic metadata database
    """

    def __init__(self, trafficDBpath, verbose=False):
        wd = os.path.dirname(trafficDBpath)
        if wd and not os.path.isdir(wd):
            if verbose:
                print(f"creating directory: {wd}")
            os.makedirs(wd)
        self.trafficDB = sqlite3.Connection(trafficDBpath)
        self.trafficDB.row_factory = sqlite3.Row

        # create a new info table if it doesnt exist yet
        with self.trafficDB as conn:
            conn.execute(_createtable_sql)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    def close(self):
        self.trafficDB.close()

    def vessel_info_callback(self, mmsis, retry_404=False, infotxt=""):
        """check that metadata exists in the local database for the given
        MMSI identifiers.

        raises RuntimeError when identifiers are missing from the local
        database, since retrieving them would require web scraping, which
        was removed (GitHub issue #81).

        args:
            mmsis (list)
                list of MMSI identifiers (integers)
        """
        mmsis = np.unique(np.array(mmsis, dtype=int).flatten())

        # placeholder count is structural SQL, not interpolated values
        sql_known = (
            "SELECT mmsi FROM webdata_marinetraffic "
            f"WHERE mmsi IN ({','.join('?' * len(mmsis))}) "
        )
        if retry_404:
            sql_known += "AND error404 != 1 "
        sql_known += "ORDER BY mmsi"
        with self.trafficDB as conn:
            known = conn.execute(sql_known, tuple(map(int, mmsis))).fetchall()

        known_mmsis = np.array([row["mmsi"] for row in known], dtype=int)
        missing = np.setdiff1d(mmsis, known_mmsis, assume_unique=True)
        if missing.size > 0:
            raise RuntimeError(
                f"{infotxt}no local metadata for {missing.size} MMSI "
                f"identifier(s), e.g. {missing[:5].tolist()}. " + _SCRAPING_REMOVED_MSG
            )
