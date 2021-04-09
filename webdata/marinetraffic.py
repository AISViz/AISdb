import os
import pickle

import numpy as np

from index.index import index
from webdata.scraper import init_webdriver


driver = init_webdriver()

def tonnage_callback(*, mmsi, imo, **_):
    driver.get(f'https://www.marinetraffic.com/en/ais/details/ships/mmsi:{mmsi}/imo:{imo}')
    WebDriverWait(driver, 5).until(presence_of_element_located((By.ID, 'vesselDetails_voyageInfoSection')))
    elem = driver.find_element_by_id('vesselDetails_vesselInfoSection').find_element_by_id('summerDwt')
    #elem.location_once_scrolled_into_view
    return int(elem.text.split(' ')[2])


def get_tonnage_mmsi_imo(mmsi, imo):
    with index(bins=False, store=True, filename='tonnage.db') as web:
        tonnage = web(callback=tonnage_callback, mmsi=mmsi, imo=imo, seed='marinetraffic.com')[0]
    return tonnage


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
