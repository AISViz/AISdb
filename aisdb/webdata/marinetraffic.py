''' scrape vessel information such as deadweight tonnage from marinetraffic.com '''

import os

import numpy as np
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

from aisdb import sqlpath
from aisdb.webdata._scraper import _scraper
from aisdb.database.dbconn import PostgresDBConn, SQLiteDBConn
import sqlite3

baseurl = 'https://www.marinetraffic.com/'

_err404 = 'INSERT INTO webdata_marinetraffic(mmsi, error404) '
_err404 += 'VALUES (CAST(? as INT), 1)'

_createtable_sqlfile = os.path.join(sqlpath,
                                    'createtable_webdata_marinetraffic.sql')
with open(_createtable_sqlfile, 'r') as f:
    _createtable_sql = f.read()

_insert_sqlfile = os.path.join(sqlpath, 'insert_webdata_marinetraffic.sql')
with open(_insert_sqlfile, 'r') as f:
    _insert_sql = f.read()


def _nullinfo(track):
    return {
        'mmsi':
        track['mmsi'],
        'imo':
        track['imo'] if 'imo' in track.keys() else 0,
        'name': (track['vessel_name'] if 'vessel_name' in track.keys()
                 and track['vessel_name'] is not None else ''),
        'vesseltype_generic':
        None,
        'vesseltype_detailed':
        None,
        'callsign':
        None,
        'flag':
        None,
        'gross_tonnage':
        None,
        'summer_dwt':
        None,
        'length_breadth':
        None,
        'year_built':
        None,
        'home_port':
        None,
        'error404':
        1
    }


def _loaded(drv: WebDriver) -> bool:  # pragma: no cover
    asset_type = 'asset_type' in drv.current_url
    e404 = '404' == drv.title[0:3]
    exists = drv.find_elements(
        by='id',
        value='vesselDetails_voyageInfoSection',
    )
    return (exists or e404 or asset_type)


def _updateinfo(info: str, vessel: dict) -> None:  # pragma: no cover
    i = info.split(': ')
    if len(i) < 2:
        return
    vessel[i[0]] = i[1]


def _getrow(vessel: dict) -> tuple:  # pragma: no cover
    if 'MMSI' not in vessel.keys() or vessel['MMSI'] == '-':
        vessel['MMSI'] = 0
    if 'IMO' not in vessel.keys() or vessel['IMO'] == '-':
        vessel['IMO'] = 0
    if 'Name' not in vessel.keys():
        vessel['Name'] = ''
    if 'Call Sign' not in vessel.keys():
        vessel['Call Sign'] = ''
    if 'Gross Tonnage' not in vessel.keys() or vessel['Gross Tonnage'] == '-':
        vessel['Gross Tonnage'] = 0
    elif ('Gross Tonnage' in vessel.keys()
          and isinstance(vessel['Gross Tonnage'], str)):
        vessel['Gross Tonnage'] = int(vessel['Gross Tonnage'].split()[0])
    if 'Summer DWT' not in vessel.keys() or vessel['Summer DWT'] == '-':
        vessel['Summer DWT'] = 0
    elif ('Summer DWT' in vessel.keys()
          and isinstance(vessel['Summer DWT'], str)):
        vessel['Summer DWT'] = int(vessel['Summer DWT'].split()[0])
    if 'Year Built' not in vessel.keys() or vessel['Year Built'] == '-':
        vessel['Year Built'] = 0
    return (
        int(vessel['MMSI']),
        int(vessel['IMO']),
        vessel['Name'],
        vessel['Vessel Type - Generic'],
        vessel['Vessel Type - Detailed'],
        vessel['Call Sign'],
        vessel['Flag'],
        int(vessel['Gross Tonnage']),
        int(vessel['Summer DWT']),
        vessel['Length Overall x Breadth Extreme'],
        int(vessel['Year Built']),
        vessel['Home Port'],
    )


def _insertvesselrow(elem, mmsi, trafficDB):  # pragma: no cover
    vessel = {}
    for info in elem.text.split('\n'):
        _updateinfo(info, vessel)
    if len(vessel.keys()) < 11:
        return
    insertrow = _getrow(vessel)

    print(insertrow)

    with trafficDB as conn:
        conn.execute(_insert_sql, insertrow).fetchall()


def _vessel_info_dict(dbconn) -> dict:
    if isinstance(dbconn, SQLiteDBConn):
        # raise ValueError(f"Invalid database connection type: {dbconn}")
        if not hasattr(dbconn, 'trafficdb'):
            raise ValueError(
                'Database connection does not have an attached traffic database!'
            )
        cur = dbconn.cursor()
        cur.execute(
            'SELECT * FROM sqlite_master '
            'WHERE type="table" AND name LIKE "ais\\_%\\_dynamic" ESCAPE "\\" '
        )
        alias = dbconn._get_dbname(dbconn.trafficdb) + '.'
    elif isinstance(dbconn, sqlite3.Connection):
        alias = ''
    elif isinstance(dbconn, SQLiteDBConn):
        alias = ''
    else:
        raise ValueError(f"Invalid connection type: {dbconn}")
    cur = dbconn.cursor()
    res = cur.execute(
        f'SELECT * FROM {alias}webdata_marinetraffic WHERE error404 != 1'
    ).fetchall()
    info_dict = {r['mmsi']: r for r in res}
    return info_dict


def vessel_info(tracks: iter, dbconn: sqlite3.Connection):
    ''' append metadata scraped from marinetraffic.com to track dictionaries.

        See :meth:`aisdb.database.dbqry.DBQuery.check_marinetraffic` for a
        high-level method to retrieve metadata for all vessels observed within
        a specific query time and region, or alternatively see
        :meth:`aisdb.webdata.marinetraffic.VesselInfo.vessel_info_callback`
        for scraping metadata for a given list of MMSIs

        args:
            tracks (iter)
                collection of track dictionaries
            dbconn (ConnectionType)
                Either a :class:`aisdb.database.dbconn.SQLiteDBConn` or
                :class:`aisdb.database.dbconn.PostgresDBConn` database
                connection objects
    '''
    if not isinstance(dbconn, sqlite3.Connection):
        raise ValueError(
            f"Invalid database connection type: {dbconn}. Requires: {sqlite3.Connection}"
        )
    meta = _vessel_info_dict(dbconn)
    for track in tracks:
        assert isinstance(track, dict)
        track['static'] = set(track['static']).union({'marinetraffic_info'})
        if track['mmsi'] in meta.keys():
            track['marinetraffic_info'] = meta[track['mmsi']]
        else:
            track['marinetraffic_info'] = _nullinfo(track)
        yield track


class VesselInfo():  # pragma: no cover
    ''' scrape vessel metadata from marinetraffic.com

        args:
            trafficDBpath (string)
                path where vessel traffic metadata should be stored

        See :meth:`aisdb.database.dbqry.DBQuery.check_marinetraffic` for a
        high-level method to retrieve metadata for all vessels observed within
        a specific query time and region, or alternatively see
        :meth:`aisdb.webdata.marinetraffic.VesselInfo.vessel_info_callback`
        for scraping metadata for a given list of MMSIs
    '''

    def __init__(self, trafficDBpath, verbose=False):
        self.driver = None
        wd = os.path.dirname(trafficDBpath)
        if not os.path.isdir(wd):  # pragma: no cover
            if verbose: print(f'creating directory: {wd}')
            os.makedirs(wd)
        self.trafficDB = sqlite3.Connection(trafficDBpath)
        self.trafficDB.row_factory = sqlite3.Row

        # create a new info table if it doesnt exist yet
        with self.trafficDB as conn:
            conn.execute(_createtable_sql)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if self.driver is not None:
            self.driver.close()
            self.driver.quit()

    def _getinfo(self, *, url, searchmmsi, infotxt=''):
        if self.driver is None:
            self.driver = _scraper()

        print(infotxt + url, end='\t')
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 15).until(_loaded)
        except TimeoutException:
            print(f'timed out, skipping {searchmmsi=}')

            # if timeout occurs, mark as error 404
            with self.trafficDB as conn:
                conn.execute(_err404, (str(searchmmsi), ))
            return
        except Exception as err:
            self.driver.close()
            self.driver.quit()
            raise err

        # recurse through vessel listings if multiple vessels appear
        if 'asset_type' in self.driver.current_url:
            print('recursing...')

            urls = []
            for elem in self.driver.find_elements(
                    By.CLASS_NAME,
                    value='ag-cell-content-link',
            ):
                urls.append(elem.get_attribute('href'))

            for url in urls:
                self._getinfo(url=url, searchmmsi=searchmmsi)

            with self.trafficDB as conn:
                insert404 = conn.execute(
                    'SELECT COUNT(*) FROM webdata_marinetraffic WHERE mmsi=?',
                    (str(searchmmsi), )).fetchone()[0] == 0
                if insert404:
                    conn.execute(_err404, (str(searchmmsi), ))

        elif 'hc-en' in self.driver.current_url:
            raise RuntimeError('bad url??')

        elif self.driver.title[0:3] == '404':
            print(f'404 error! {searchmmsi=}')
            with self.trafficDB as conn:
                conn.execute(_err404, (str(searchmmsi), ))

        value = 'vesselDetails_vesselInfoSection'
        for elem in self.driver.find_elements(value=value):
            _ = _insertvesselrow(elem, searchmmsi, self.trafficDB)

    def vessel_info_callback(self, mmsis, retry_404=False, infotxt=''):
        ''' search for metadata for given mmsis

            args:
                mmsis (list)
                    list of MMSI identifiers (integers)
        '''
        # only check unique mmsis
        mmsis = np.unique(mmsis).astype(int)
        print('.', end='')  # second dot (first in dbqry.py)

        # check existing
        sqlcount = 'SELECT mmsi FROM webdata_marinetraffic \t'
        sqlcount += f'WHERE mmsi IN ({",".join(["?" for _ in mmsis])})\n'
        if retry_404:
            sqlcount += 'AND error404 != 1 \n'
        sqlcount += 'ORDER BY mmsi'
        with self.trafficDB as conn:
            existing = conn.execute(sqlcount, tuple(map(str,
                                                        mmsis))).fetchall()
        print('.')  # third dot

        # skip existing mmsis
        ex_mmsis = np.array(existing).flatten()
        xor_mmsis = np.setdiff1d(mmsis, ex_mmsis, assume_unique=True)
        if xor_mmsis.size == 0:
            return

        for mmsi in xor_mmsis:
            if not 200000000 <= mmsi <= 780000000:
                continue
            url = f'{baseurl}en/ais/details/ships/mmsi:{mmsi}'
            self._getinfo(url=url, searchmmsi=mmsi, infotxt=infotxt)

        return


'''
# validate IMO
if searchimo != 0:
    checksum = str(
            np.sum(
                np.array(list(map(int, list(str(searchimo)[:-1])))) *
                np.array([7, 6, 5, 4, 3, 2])))[-1]
else:
    checksum = '0'
'''
