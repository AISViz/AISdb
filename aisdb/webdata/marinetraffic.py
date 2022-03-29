''' scrape vessel information such as deadweight tonnage from marinetraffic.com '''

import os
import warnings

import numpy as np
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

from aisdb import data_dir, sqlpath
from aisdb.webdata.scraper import Scraper
import sqlite3

warnings.filterwarnings("error")


trafficDBpath = os.path.join(data_dir, 'marinetraffic.db')
trafficDB = sqlite3.Connection(trafficDBpath)


err404 = 'INSERT OR IGNORE INTO webdata_marinetraffic(mmsi, imo, error404) '
err404 += 'VALUES (CAST(? as INT), CAST(? as INT), 1)'

# prepare sql code for inserting vessel info
insert_sqlfile = os.path.join(sqlpath, 'insert_webdata_marinetraffic.sql')
with open(insert_sqlfile, 'r') as f:
    insert_sql = f.read()


def _loaded(drv: WebDriver) -> bool:
    asset_type = 'asset_type' in drv.current_url
    e404 = '404' == drv.title[0:3]
    exists = drv.find_elements(
            by='id',
            value='vesselDetails_voyageInfoSection',
            )
    return (exists or e404 or asset_type)


def _updateinfo(info: str, vessel: dict) -> None:
    match info.split(': '):

        case ['MMSI', '-']:
            vessel['mmsi'] = 0

        case ['MMSI', mmsi]:
            vessel['mmsi'] = int(mmsi)

        case ['IMO', '-']:
            vessel['imo'] = 0

        case ['IMO', imo]:
            vessel['imo'] = int(imo)

        case ['Name', name]:
            vessel['name'] = name

        case ['Vessel Type - Generic', vtype1]:
            vessel['vesseltype_generic'] = vtype1

        case ['Vessel Type - Detailed', vtype2]:
            vessel['vesseltype_detailed'] = vtype2

        case ['Call Sign', callsign]:
            vessel['callsign'] = callsign

        case ['Flag', flag]:
            vessel['flag'] = flag

        case ['Gross Tonnage', '-']:
            vessel['gross_tonnage'] = 0

        case ['Gross Tonnage', gt]:
            vessel['gross_tonnage'] = int(gt)

        case ['Summer DWT', '-']:
            vessel['summer_dwt'] = 0

        case ['Summer DWT', dwt]:
            vessel['summer_dwt'] = int(dwt.split()[0])

        case['Length Overall x Breadth Extreme', lxw]:
            vessel['length_breadth'] = lxw

        case['Year Built', '-']:
            vessel['year_built'] = 0

        case['Year Built', year]:
            vessel['year_built'] = int(year)

        case['Home Port', home]:
            vessel['home_port'] = home


def _getrow(vessel: dict) -> tuple:
    if 'mmsi' not in vessel.keys():
        vessel['mmsi'] = 0
    if 'imo' not in vessel.keys():
        vessel['imo'] = 0
    assert isinstance(vessel["mmsi"], int), f'not an int: {type(vessel["mmsi"])} {vessel["mmsi"]=}'
    assert isinstance(vessel["imo"], int), f'not an int: {type(vessel["imo"])} {vessel["imo"]=}'
    return (vessel['mmsi'],
            vessel['imo'],
            vessel['vesseltype_generic'],
            vessel['vesseltype_detailed'],
            vessel['callsign'],
            vessel['flag'],
            vessel['gross_tonnage'],
            vessel['summer_dwt'],
            vessel['length_breadth'],
            vessel['year_built'],
            vessel['home_port'],
            )


def _insertelem(elem, mmsi, imo):
    vessel = {}
    for info in elem.text.split('\n'):
        _updateinfo(info, vessel)
    if len(vessel.keys()) < 11:
        return
    insertrow = _getrow(vessel)

    print(vessel)

    with trafficDB as conn:
        conn.execute(insert_sql, insertrow)
        if vessel['mmsi'] != mmsi:
            conn.execute(err404, (str(mmsi), str(imo)))
        if vessel['imo'] != imo:
            vessel['imo'] = int(imo)
            insertrow = _getrow(vessel)
            conn.execute(insert_sql, insertrow)


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
            WebDriverWait(self.driver, 30).until(_loaded)
        except TimeoutException:
            print(f'timed out, skipping {searchmmsi=} {searchimo=}')

            # validate IMO
            if searchimo != 0:
                checksum = str(
                        np.sum(
                            np.array(list(map(int, list(str(searchimo)[:-1])))) *
                            np.array([7, 6, 5, 4, 3, 2])))[-1]
            else:
                checksum = '0'

            # if timeout occurs and MMSI or IMO appears invalid,
            # mark as error 404
            if (searchmmsi < 200000000
                    or searchmmsi > 780000000
                    or checksum != str(searchimo)[-1]):
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

    def vessel_info_callback(self, mmsis, imos):
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

        # skip existing
        qrymmsis = ','.join(map(str, mmsis))
        sqlcount = 'SELECT CAST(mmsi AS INT), CAST(imo as INT) '
        sqlcount += f'FROM webdata_marinetraffic WHERE mmsi IN ({qrymmsis})'
        sqlcount += 'ORDER BY mmsi'

        with trafficDB as conn:
            existing = conn.execute(sqlcount).fetchall()

        for m, i in existing:
            idx_m = mmsis == m
            idx_i = imos == i
            skip = np.logical_and(idx_m, idx_i)
            if sum(skip) == 0:
                continue
            mmsis = mmsis[~skip]
            imos = imos[~skip]

        if mmsis.size == 0:
            return

        for mmsi, imo in zip(mmsis, imos):
            suffix = f'/imo:{imo}' if imo > 0 else ''
            url = f'{self.baseurl}en/ais/details/ships/mmsi:{mmsi}{suffix}'
            self._getinfo(url, mmsi, imo)

        return
