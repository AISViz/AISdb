''' webscraper using selenium, firefox, and mozilla geckodriver '''

import os
import shutil
import requests
from bs4 import BeautifulSoup
import time
from random import randint

def firefox_driver():
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options
    from selenium.webdriver.firefox.service import Service
    from webdriver_manager.firefox import GeckoDriverManager
    opt = webdriver.FirefoxOptions()
    # opt.headless = True if not os.environ.get('DEBUG') else False
    opt.set_preference('permissions.default.image', 2)
    opt.set_preference('extensions.contentblocker.enabled', True)
    opt.set_preference('media.autoplay.default', 2)
    opt.set_preference('media.autoplay.allow-muted', False)
    opt.set_preference('media.autoplay.block-event.enabled', True)
    opt.set_preference('media.autoplay.block-webaudio', True)
    opt.set_preference('services.sync.prefs.sync.media.autoplay.default',False)
    opt.set_preference('ui.context_menus.after_mouseup', False)
    opt.set_preference('privacy.sanitize.sanitizeOnShutdown', True)
    opt.set_preference('dom.disable_beforeunload', True)
    if not os.environ.get('DEBUG') and not os.environ.get('HEADLESS') == '0':
        opt.add_argument('-headless')

    driver = webdriver.Firefox(options=opt)
    return driver


def _scraper():
    ''' selenium web scraper ``selenium.webdriver``

        to open a browser window while debugging, export DEBUG=1
    '''
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options
    from selenium.webdriver.firefox.service import Service
    from webdriver_manager.firefox import GeckoDriverManager
    # from selenium.webdriver.chrome.options import Options
    # from selenium.webdriver.chrome.service import Service
    # from webdriver_manager.chrome import ChromeDriverManager
    # assert shutil.which('firefox') is not None, f'Firefox is required for this feature. {shutil.which("firefox")=}'

    # # configs
    # opt = Options()
    # #opt.headless = True if not os.environ.get('DEBUG') else False
    # opt.set_preference('permissions.default.image', 2)
    # opt.set_preference('extensions.contentblocker.enabled', True)
    # opt.set_preference('media.autoplay.default', 2)
    # opt.set_preference('media.autoplay.allow-muted', False)
    # opt.set_preference('media.autoplay.block-event.enabled', True)
    # opt.set_preference('media.autoplay.block-webaudio', True)
    # opt.set_preference('services.sync.prefs.sync.media.autoplay.default',
    #                    False)
    # opt.set_preference('ui.context_menus.after_mouseup', False)
    # opt.set_preference('privacy.sanitize.sanitizeOnShutdown', True)
    # opt.set_preference('dom.disable_beforeunload', True)
    # if not os.environ.get('DEBUG') and not os.environ.get('HEADLESS') == '0':
    #     opt.add_argument('-headless')
    # """ chrome args
    # opt.add_argument('--headless')
    # opt.add_argument(f'user-data-dir={data_dir}')
    # opt.add_argument('permissions.default.image=2')
    # opt.add_argument('extensions.contentblocker.enabled=True')
    # opt.add_argument('media.autoplay.default=2')
    # opt.add_argument('media.autoplay.allow-muted=False')
    # opt.add_argument('media.autoplay.block-event.enabled=True')
    # opt.add_argument('media.autoplay.block-webaudio=True')
    # opt.add_argument(
    #     'services.sync.prefs.sync.media.autoplay.default=False')
    # opt.add_argument('ui.context_menus.after_mouseup=False')
    # opt.add_argument('privacy.sanitize.sanitizeOnShutdown=True')
    # opt.add_argument('dom.disable_beforeunload=True')
    # """
    #
    # driver = webdriver.Firefox(
    #     service=Service(executable_path=GeckoDriverManager().install()),
    #     options=opt)

    driver = firefox_driver()

    if os.environ.get('DEBUG'):
        driver.maximize_window()
    else:
        driver.set_window_size(9999, 9999)

    return driver


B_URL = "https://www.vesselfinder.com/"

def search_metadata_vesselfinder(mmsi):
    '''
    scrape vessel metadata from vesselfinder

    args:
        search_metadata_with_mmsi(MMSI <Str>)

    returns:
        dictionary of information
    '''
    request_url = B_URL+"vessels?name={0}".format(mmsi)
    r_dict = {}

    # getting response text webpage
    hdr = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(request_url, headers=hdr)

    soup = BeautifulSoup(response.content, 'html.parser')

    tb_data_element = soup.find('div', attrs={"class": "sli"})
    try:
        boat_inf_url = tb_data_element.parent.get("href")

        response = requests.get(B_URL+boat_inf_url, headers=hdr)
        web_vessel_soup = BeautifulSoup(response.content, 'html.parser')
        table_rows = web_vessel_soup.find("h2", text="Vessel Particulars").parent.find_all("tr")

        for tr in table_rows:
            key = tr.find_all("td")[0].text
            value = tr.find_all("td")[1].text
            r_dict[key] = value
    except:
        print("no metadata mmsi -> {0}".format(mmsi))


    try:
        tbdata_element = web_vessel_soup.find('table', attrs={"class": "aparams"}).find_all("tr")
        for trow in tbdata_element:
            key = trow.find_all("td")[0].text
            value = trow.find_all("td")[1].text
            r_dict[key] = value
    except:
        print("no information mmsi -> {0}".format(mmsi))

    return r_dict


def search_metadata_marinetraffic(mmsi):
    '''
       scrape vessel metadata from marinetraffic

       args:
           search_metadata_with_mmsi(MMSI <Str>)

       returns:
           dictionary of information
       '''
    data2 = {}
    try:
        url_str = "https://www.marinetraffic.com/en/global_search/search?term={0}".format(mmsi)
        headers = {
                "accept": "application/json",
                "accept-encoding": "gzip, deflate",
                "user-agent": "Mozilla/5.0",
                "x-requested-with": "XMLHttpRequest"
            }

        response = requests.get(url_str, headers=headers)
        response.raise_for_status()

        data = response.json()

        ship_id = data['results'][0]['id']
        time.sleep(randint(1, 3))
        data2 = get_ship_metadata(ship_id)
    except:
        print("no information mmsi -> {0}".format(mmsi))

    return data2


def get_ship_metadata(ship_id):
        url = "https://www.marinetraffic.com/vesselDetails/vesselInfo/shipid:{}".format(ship_id)
        headers = {
            "accept": "application/json",
            "accept-encoding": "gzip, deflate",
            "user-agent": "Mozilla/5.0",
            "x-requested-with": "XMLHttpRequest"
        }
        json_A = {}
        json_B = {}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            json_A = response.json()
        except:
            a = 10 # print("request failed: ", url)
        time.sleep(randint(1,3))
        try:
            url2 = "https://www.marinetraffic.com/vesselDetails/latestPosition/shipid:{0}".format(ship_id)
            response2 = requests.get(url2, headers=headers)
            response2.raise_for_status()
            json_B = response2.json()
        except:
            a = 10 # print("request failed: ", url2)

        json_A.update(json_B)

        return json_A