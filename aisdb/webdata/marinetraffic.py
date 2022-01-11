''' scrape vessel information such as deadweight tonnage from marinetraffic.com '''

import os

from common import *
from index import index
from webdata.scraper import *


class scrape_tonnage():

    def __init__(self):
        self.filename = 'marinetraffic.db'
        self.driver = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if self.driver is not None:
            self.driver.close()
            self.driver.quit()

    def tonnage_callback(self, mmsi, imo=0, **_):
        if self.driver is None:
            self.driver = init_webdriver()

        loaded = lambda drv: 'asset_type' in drv.current_url or '404' == drv.title[
            0:3] or drv.find_elements_by_id('vesselDetails_voyageInfoSection')

        if imo == 0:
            url = f'https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}'
        else:
            url = f'https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}/imo:{imo}'
        print(url, end='\t')
        self.driver.get(url)

        WebDriverWait(self.driver, 15).until(loaded)

        if 'asset_type' in self.driver.current_url:
            for elem in self.driver.find_elements_by_partial_link_text(""):
                if (url := elem.get_attribute('href')) == None: continue
                elif 'vessel:' in url:
                    print(
                        f'multiple entries found for {mmsi=} {imo=} ! fetching {url}'
                    )
                    self.driver.get(url)
                    WebDriverWait(self.driver, 15).until(loaded)
                    break

        elif self.driver.title[0:3] == '404':
            print(f'404 error! {mmsi=} {imo=}')
            return 0

        exists = self.driver.find_elements_by_id(
            'vesselDetails_vesselInfoSection')
        if exists:
            elem = exists[0].find_element_by_id('summerDwt')
            #elem.location_once_scrolled_into_view
            print(mmsi, elem.text)
            return elem.text.split(' ')[2]
        else:
            print(0)
            return 0

    def get_tonnage_mmsi_imo(self, mmsi, imo):
        #if not 201000000 <= mmsi < 776000000: return 0
        if not 1000000 <= imo < 9999999: imo = 0

        with index(bins=False,
                   store=True,
                   storagedir=data_dir,
                   filename=self.filename) as web:
            tonnage = web(callback=self.tonnage_callback,
                          mmsi=mmsi,
                          imo=imo,
                          seed='dwt marinetraffic.com')[0]

        if tonnage == '-': return 0

        return int(tonnage)

    def exit(self):
        self.driver.close()


def api_shipsearch_bymmsi(mmsi):
    '''
    https://services.marinetraffic.com/api/shipsearch/YOUR-API-KEY/mmsi:value/protocol:value
    '''
    url = f'https://services.marinetraffic.com/api/shipsearch/{YOUR_API_KEY}/mmsi:{mmsi}/protocol:json'
    requests.get(url)
    pass


'''
import pickle
# load cookies
cookiefile = os.path.join(os.path.dirname(__file__), 'webdata.cookie')
if os.path.isfile(cookiefile):
    for cookie in pickle.load(open(cookiefile, 'rb')): driver.add_cookie(cookie)
    driver.refresh()


# save cookies
pickle.dump(driver.get_cookies(), open(cookiefile, 'wb'))

mmsi = 566970000
imo = 9604110

get_tonnage_mmsi_imo(mmsi, imo)
'''
