import os
import time

from webdata.scraper import *
from hashindex.index import index


driver = init_webdriver()

def tonnage_callback(*, mmsi, imo, **_):
    loaded = lambda drv: 'asset_type' in drv.current_url or '404' == drv.title[0:3] or drv.find_elements_by_id('vesselDetails_voyageInfoSection')

    driver.get(f'https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}/imo:{imo}')
    WebDriverWait(driver, 10).until(loaded)

    if 'asset_type' in driver.current_url:
        for elem in driver.find_elements_by_partial_link_text(""):
            if (url := elem.get_attribute('href')) == None: continue
            elif 'vessel:' in url: 
                print(f'multiple entries found for {mmsi=} {imo=} ! fetching {url}')
                driver.get(url)
                WebDriverWait(driver, 10).until(loaded)
                break

    elif driver.title[0:3] == '404':
        print(f'404 error! {mmsi=} {imo=}')
        return 0

    exists = driver.find_elements_by_id('vesselDetails_vesselInfoSection')
    if exists: 
        elem = exists[0].find_element_by_id('summerDwt')
        #elem.location_once_scrolled_into_view
        return elem.text.split(' ')[2]
    else: 
        return 0


def get_tonnage_mmsi_imo(mmsi, imo, storagedir=os.getcwd(), filename='tonnage.db'):
    with index(bins=False, store=True, storagedir=storagedir, filename=filename) as web:
        tonnage = web(callback=tonnage_callback, mmsi=mmsi, imo=imo, seed='marinetraffic.com')[0]
    if tonnage == '-': return 0
    return int(tonnage)


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
