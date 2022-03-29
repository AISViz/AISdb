''' webscraper using selenium, firefox, and mozilla geckodriver '''

import os

from aisdb.common import data_dir
from aisdb.webdata import _init_configs

import shutil

from selenium.webdriver.firefox import webdriver
from selenium.webdriver.firefox.options import Options

cfgfile = os.path.join(os.path.expanduser('~'), '.config', 'ais.cfg')

os.environ['PATH'] = f'{data_dir}:{os.environ.get("PATH")}'


class Scraper():

    def __init__(self, proxy=None):
        '''
            args:
                proxy (string):
                    Optional. String addressing IP and port, e.g.
                    "127.0.0.1:8080"
        '''

        firefoxpath = '/usr/lib/firefox/firefox' if os.path.isfile(
                '/usr/lib/firefox/firefox') else shutil.which('firefox')
        # firefoxpath = shutil.which('firefox')

        if firefoxpath is None:
            raise RuntimeError(
                    'firefox must be installed to use this feature!')

        _init_configs()

        headless = True
        match os.environ.get('HEADLESS', True):
            case '0' | 'False' | 'false':
                headless = False

        # configs
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
        # driverpath = 'webdriver' if os.name != 'nt' else 'geckodriver.exe'

        service_args = []
        if proxy is not None:
            host, port = proxy.split(':')
            service_args = ['--host', host, '--port', port]

        self.driver = webdriver.WebDriver(
                options=opt,
                keep_alive=True,
                service_args=service_args,
                )

        if headless:
            self.driver.set_window_size(9999, 9999)
        else:
            self.driver.maximize_window()
