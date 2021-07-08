import os


'''
__file__ = 'webdata/install_dep.py'
'''
if not os.path.isfile(os.path.join(os.path.dirname(__file__), 'webdriver')) and not os.path.isfile(os.path.join(os.path.dirname(__file__), 'webdriver.exe')):
    import requests
    import tarfile
    import zipfile
    import stat
    print('downloading webdrivers...')

    if   os.name == 'posix':  url = 'https://github.com/mozilla/geckodriver/releases/download/v0.29.0/geckodriver-v0.29.0-linux64.tar.gz'
    elif os.name == 'darwin': url = 'https://github.com/mozilla/geckodriver/releases/download/v0.29.0/geckodriver-v0.29.0-macos.tar.gz'
    elif os.name == 'nt':     url = 'https://github.com/mozilla/geckodriver/releases/download/v0.29.0/geckodriver-v0.29.0-win64.zip'
    else: print('unsupported platform!'); exit()

    req = requests.get(url=url, stream=True) 


    if os.name != 'nt':
        with open(os.path.join(os.path.dirname(__file__), 'drivers.tar.gz'),'wb') as f: list(map(lambda chunk: f.write(chunk), req.iter_content(chunk_size=1024)))
        tar = tarfile.open(os.path.join(os.path.dirname(__file__), 'drivers.tar.gz'), 'r:gz')
        with open(os.path.join(os.path.dirname(__file__), 'webdriver'), 'wb')     as f: f.write(tar.extractfile(tar.getmembers()[0]).read())
        os.remove(os.path.join(os.path.dirname(__file__), 'drivers.tar.gz'))
        os.chmod(os.path.join(os.path.dirname(__file__), 'webdriver'), stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IROTH)
    else:
        with open(os.path.join(os.path.dirname(__file__), 'drivers.zip'),'wb')    as f: list(map(lambda chunk: f.write(chunk), req.iter_content(chunk_size=1024)))
        zipfile.ZipFile(os.path.join(os.path.dirname(__file__), 'drivers.zip')).extractall()
        os.remove(os.path.join(os.path.dirname(__file__), 'drivers.zip'))

    #with open(savefile, 'w') as f: f.write('')
    print('drivers installed!')


os.environ['PATH'] = f'{os.path.dirname(__file__)}:{os.environ.get("PATH")}'

import shutil

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, SessionNotCreatedException
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located
from selenium.webdriver.common.by import By


def init_webdriver():

    # configs
    headless = True
    (opt := Options()).headless = headless
    opt.set_preference('permissions.default.image', 2)
    opt.set_preference('extensions.contentblocker.enabled', True)
    opt.set_preference('media.autoplay.default', 2)
    opt.set_preference('media.autoplay.allow-muted', False)
    opt.set_preference('media.autoplay.block-event.enabled', True)
    opt.set_preference('media.autoplay.block-webaudio', True)
    opt.set_preference('services.sync.prefs.sync.media.autoplay.default', False)
    opt.set_preference('ui.context_menus.after_mouseup', False)
    opt.set_preference('privacy.sanitize.sanitizeOnShutdown', True)
    opt.set_preference('dom.disable_beforeunload', True)
    driverpath = 'webdriver' if os.name != 'nt' else 'geckodriver.exe'

    firefoxpath = '/usr/lib/firefox/firefox' if os.path.ispath('/usr/lib/firefox/firefox') else shutil.which('firefox')

    if os.path.isfile(firefoxpath):  
        driver = webdriver.Firefox(
                firefox_binary=FirefoxBinary(firefoxpath), 
                executable_path=os.path.join(os.path.dirname(__file__), driverpath), 
                options=opt,
                service_log_path=os.path.join(os.path.dirname(__file__), 'geckodriver.log'),
            )
    else: 
        driver = webdriver.Firefox(
                executable_path=os.path.join(os.path.dirname(__file__), driverpath), 
                options=opt,
            )
    driver.set_window_size(9999,9999) if headless else driver.maximize_window()
    return driver

