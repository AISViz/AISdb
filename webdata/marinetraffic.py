import os
import pickle

import numpy as np
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, SessionNotCreatedException
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium.webdriver.common.by import By

from index.index import index
from webdata import install_dep


# configs
headless = True
(opt := Options()).headless = headless
opt.set_preference('permissions.default.image', 2)  # dont load images for faster loading
opt.set_preference('extensions.contentblocker.enabled', True)
opt.set_preference('media.autoplay.default', 1)
opt.set_preference('media.autoplay.allow-muted', False)
opt.set_preference('media.autoplay.block-event.enabled', True)
opt.set_preference('media.autoplay.block-webaudio', True)
opt.set_preference('services.sync.prefs.sync.media.autoplay.default', False)
opt.set_preference('ui.context_menus.after_mouseup', False)
opt.set_preference('privacy.sanitize.sanitizeOnShutdown', True)
driverpath = 'webdriver' if os.name != 'nt' else 'geckodriver.exe'


# init webdriver
if os.path.isfile(path := '/usr/lib/firefox/firefox') or (path := shutil.which('firefox')):  
    driver = webdriver.Firefox(firefox_binary=FirefoxBinary(path), executable_path=os.path.join(os.path.dirname(__file__), driverpath), options=opt)
else: 
    driver = webdriver.Firefox(executable_path=os.path.join(os.path.dirname(__file__), driverpath), options=opt)
driver.set_window_size(9999,9999) if headless else driver.maximize_window()


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
