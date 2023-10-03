''' webscraper using selenium, firefox, and mozilla geckodriver '''

import os
import shutil


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

    # configs
    opt = Options()
    #opt.headless = True if not os.environ.get('DEBUG') else False
    opt.set_preference('permissions.default.image', 2)
    opt.set_preference('extensions.contentblocker.enabled', True)
    opt.set_preference('media.autoplay.default', 2)
    opt.set_preference('media.autoplay.allow-muted', False)
    opt.set_preference('media.autoplay.block-event.enabled', True)
    opt.set_preference('media.autoplay.block-webaudio', True)
    opt.set_preference('services.sync.prefs.sync.media.autoplay.default',
                       False)
    opt.set_preference('ui.context_menus.after_mouseup', False)
    opt.set_preference('privacy.sanitize.sanitizeOnShutdown', True)
    opt.set_preference('dom.disable_beforeunload', True)
    if not os.environ.get('DEBUG') and not os.environ.get('HEADLESS') == '0':
        opt.add_argument('-headless')
    """ chrome args
    opt.add_argument('--headless')
    opt.add_argument(f'user-data-dir={data_dir}')
    opt.add_argument('permissions.default.image=2')
    opt.add_argument('extensions.contentblocker.enabled=True')
    opt.add_argument('media.autoplay.default=2')
    opt.add_argument('media.autoplay.allow-muted=False')
    opt.add_argument('media.autoplay.block-event.enabled=True')
    opt.add_argument('media.autoplay.block-webaudio=True')
    opt.add_argument(
        'services.sync.prefs.sync.media.autoplay.default=False')
    opt.add_argument('ui.context_menus.after_mouseup=False')
    opt.add_argument('privacy.sanitize.sanitizeOnShutdown=True')
    opt.add_argument('dom.disable_beforeunload=True')
    """

    driver = webdriver.Firefox(
        service=Service(executable_path=GeckoDriverManager().install()),
        options=opt)

    if os.environ.get('DEBUG'):
        driver.maximize_window()
    else:
        driver.set_window_size(9999, 9999)

    return driver
