''' scrape vessel information such as deadweight tonnage from marinetraffic.com
'''

import os

import numpy as np
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

from aisdb import data_dir, sqlpath
from aisdb.webdata.scraper import Scraper
import sqlite3

trafficDBpath = os.path.join(data_dir, 'marinetraffic.db')
trafficDB = sqlite3.Connection(trafficDBpath)
trafficDB.row_factory = sqlite3.Row


err404 = 'INSERT OR IGNORE INTO webdata_marinetraffic(mmsi, imo, error404) '
err404 += 'VALUES (CAST(? as INT), CAST(? as INT), 1)'


def _loaded(drv: WebDriver) -> bool:
    asset_type = 'asset_type' in drv.current_url
    e404 = '404' == drv.title[0:3]
    exists = drv.find_elements(
            by='id',
            value='vesselDetails_voyageInfoSection',
            )
    return (exists or e404 or asset_type)


def _updateinfo(info: str, vessel: dict) -> None:
    i = info.split(': ')
    if len(i) < 2:
        return
    vessel[i[0]] = i[1]


def _getrow(vessel: dict) -> tuple:
    if 'MMSI' not in vessel.keys() or vessel['MMSI'] == '-':
        vessel['MMSI'] = 0
    if 'IMO' not in vessel.keys() or vessel['IMO'] == '-':
        vessel['IMO'] = 0
    if 'Name' not in vessel.keys():
        vessel['Name'] = ''
    if 'Gross Tonnage' not in vessel.keys() or vessel['Gross Tonnage'] == '-':
        vessel['Gross Tonnage'] = 0
    elif 'Gross Tonnage' in vessel.keys() and isinstance(vessel['Gross Tonnage'], str):
        vessel['Gross Tonnage'] = int(vessel['Gross Tonnage'].split()[0])
    if 'Summer DWT' not in vessel.keys() or vessel['Summer DWT'] == '-':
        vessel['Summer DWT'] = 0
    elif 'Summer DWT' in vessel.keys() and isinstance(vessel['Summer DWT'], str):
        vessel['Summer DWT'] = int(vessel['Summer DWT'].split()[0])
    if 'Year Built' not in vessel.keys() or vessel['Year Built'] == '-':
        vessel['Year Built'] = 0
    return (int(vessel['MMSI']),
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


def _insertelem(elem, mmsi, imo):
    # prepare sql code for inserting vessel info
    insert_sqlfile = os.path.join(sqlpath, 'insert_webdata_marinetraffic.sql')
    with open(insert_sqlfile, 'r') as f:
        insert_sql = f.read()

    vessel = {}
    for info in elem.text.split('\n'):
        _updateinfo(info, vessel)
    if len(vessel.keys()) < 11:
        return
    insertrow = _getrow(vessel)

    print(vessel)

    with trafficDB as conn:
        conn.execute(insert_sql, insertrow)
        if vessel['MMSI'] != mmsi:
            conn.execute(err404, (str(mmsi), str(imo)))
        if vessel['IMO'] != imo:
            vessel['IMO'] = int(imo)
            insertrow = _getrow(vessel)
            conn.execute(insert_sql, insertrow)


def _vinfo(track, conn):
    track['static'] = set(track['static']).union({'marinetraffic_info'})
    res = conn.execute(
            'select * from webdata_marinetraffic where mmsi = ?',
            [track['mmsi']],
            ).fetchall()
    if len(res) >= 1:
        for r in res:
            if (r['error404'] == 0 and r['imo'] > 0
                    and r['vesseltype_generic'] is not None):
                track['marinetraffic_info'] = dict(r)
                break
            track['marinetraffic_info'] = dict(r)
    else:
        track['marinetraffic_info'] = {
                'mmsi': track['mmsi'],
                'imo': track['imo'],
                'name': track['vessel_name'] if 'vessel_name' in track.keys() and track['vessel_name'] is not None else '',
                'vesseltype_generic': None,
                'vesseltype_detailed': None,
                'callsign': None,
                'flag': None,
                'gross_tonnage': None,
                'summer_dwt': None,
                'length_breadth': None,
                'year_built': None,
                'home_port': None,
                'error404': 1,
                }
    if track['marinetraffic_info']['name'] is None or track['marinetraffic_info']['name'] == 0:
        track['marinetraffic_info']['name'] = track['vessel_name']
    return track


def vessel_info(tracks):
    with trafficDB as conn:
        for track in tracks:
            yield _vinfo(track, conn)


class VesselInfo():

    def __init__(self, proxy=None):
        self.filename = 'marinetraffic.db'
        self.driver = None
        self.baseurl = 'https://www.marinetraffic.com/'
        self.proxy = proxy

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if self.driver is not None:
            self.driver.close()
            self.driver.quit()

    def _getinfo(self, url, searchmmsi, searchimo):
        if self.driver is None:
            self.driver = Scraper(proxy=self.proxy).driver

        print(url, end='\t')
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 15).until(_loaded)
        except TimeoutException:
            print(f'timed out, skipping {searchmmsi=} {searchimo=}')

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

            # if timeout occurs, mark as error 404
            with trafficDB as conn:
                conn.execute(err404, (str(searchmmsi), str(searchimo)))
            return
        except Exception as err:
            self.driver.close()
            self.driver.quit()
            raise err

        if 'asset_type' in self.driver.current_url:
            print('recursing...')

            urls = []
            for elem in self.driver.find_elements(
                    By.CLASS_NAME,
                    value='ag-cell-content-link',
                    ):
                urls.append(elem.get_attribute('href'))

            for url in urls:
                self._getinfo(url, searchmmsi, searchimo)

            # recursion break condition
            with trafficDB as conn:
                #conn.execute(err404, (searchmmsi, searchimo))
                conn.execute(err404, (str(searchmmsi), str(searchimo)))

        elif 'hc-en' in self.driver.current_url:
            raise RuntimeError('bad url??')

        elif self.driver.title[0:3] == '404':
            print(f'404 error! {searchmmsi=} {searchimo=}')
            with trafficDB as conn:
                #conn.execute(err404, (searchmmsi, searchimo))
                conn.execute(err404, (str(searchmmsi), str(searchimo)))

        value = 'vesselDetails_vesselInfoSection'
        for elem in self.driver.find_elements(value=value):
            _ = _insertelem(elem, searchmmsi, searchimo)

    def vessel_info_callback(self, mmsis, imos, retry_404=False):
        # only check unique mmsis and matching imo
        mmsis, midx = np.unique(mmsis, return_index=True)
        imos = [i if i is not None else 0 for i in imos[midx]]
        mmsis = np.array(mmsis, dtype=int)
        imos = np.array(imos, dtype=int)
        assert mmsis.size == imos.size

        # create a new info table if it doesnt exist yet
        createtable_sqlfile = os.path.join(
                sqlpath,
                'createtable_webdata_marinetraffic.sql',
                )
        with (trafficDB as conn, open(createtable_sqlfile, 'r') as f):
            createtable_sql = f.read()
            conn.execute(createtable_sql)

        # check existing
        qrymmsis = ','.join(map(str, mmsis))
        sqlcount = 'SELECT CAST(mmsi AS INT), CAST(imo as INT)\n'
        sqlcount += f'FROM webdata_marinetraffic WHERE mmsi IN ({qrymmsis})\n'
        if retry_404:
            sqlcount += 'AND error404 != 1\n'
        sqlcount += 'ORDER BY mmsi'
        with trafficDB as conn:
            existing = conn.execute(sqlcount).fetchall()

        # skip existing mmsis
        for m, i in existing:
            idx_m = mmsis == m
            idx_i = imos == i
            skip = np.logical_and(idx_m, idx_i)
            if np.sum(skip) == 0:
                continue
            mmsis = mmsis[~skip]
            imos = imos[~skip]

        if mmsis.size == 0:
            return

        for mmsi, imo in zip(mmsis, imos):
            if not 200000000 <= mmsi <= 780000000:
                continue
            suffix = f'/imo:{imo}' if imo > 0 else ''
            url = f'{self.baseurl}en/ais/details/ships/mmsi:{mmsi}{suffix}'
            self._getinfo(url, mmsi, imo)

        return
