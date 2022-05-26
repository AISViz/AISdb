import os
import configparser

cfgfile = os.path.join(os.path.expanduser('~'), '.config', 'ais.cfg')
#baseurl = 'https://github.com/mozilla/geckodriver/releases/download/v0.30.0/'
srcurl = 'https://firefox-ci-tc.services.mozilla.com/api/queue/v1/task/KVijQsMQQiKl1_Ada0CNog/runs/0/artifacts/public/build/geckodriver.tar.gz'


def _init_configs(data_dir):
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)
    if os.path.isfile(cfgfile):
        cfg = configparser.ConfigParser()
        with open(cfgfile, 'r') as f:
            cfg.read_string('[DEFAULT]\n' + f.read())
        settings = dict(cfg['DEFAULT'])
        data_dir = settings['data_dir'] if 'data_dir' in settings.keys(
        ) else data_dir

    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)

    if not os.path.isfile(os.path.join(
            data_dir, 'webdriver')) and not os.path.isfile(
                os.path.join(data_dir, 'webdriver.exe')):
        import requests
        import tarfile
        import zipfile
        import stat
        print('downloading webdrivers...')
        '''
        if os.name == 'posix':
            url = baseurl + 'geckodriver-v0.30.0-linux64.tar.gz'
        elif os.name == 'darwin':
            url = baseurl + 'geckodriver-v0.30.0-macos.tar.gz'
        elif os.name == 'nt':
            url = baseurl + 'geckodriver-v0.30.0-win64.zip'
        else:
            print('unsupported platform!')
            exit()
        '''
        url = srcurl

        req = requests.get(url=url, stream=True)

        if os.name != 'nt':
            with open(os.path.join(data_dir, 'drivers.tar.gz'), 'wb') as f:
                list(
                    map(lambda chunk: f.write(chunk),
                        req.iter_content(chunk_size=1024)))
            tar = tarfile.open(os.path.join(data_dir, 'drivers.tar.gz'),
                               'r:gz')
            with open(os.path.join(data_dir, 'webdriver'), 'wb') as f:
                f.write(tar.extractfile(tar.getmembers()[0]).read())
            os.remove(os.path.join(data_dir, 'drivers.tar.gz'))
            os.chmod(
                os.path.join(data_dir, 'webdriver'), stat.S_IXUSR
                | stat.S_IXGRP | stat.S_IXOTH | stat.S_IRUSR | stat.S_IROTH)
        else:
            with open(os.path.join(data_dir, 'drivers.zip'), 'wb') as f:
                list(
                    map(lambda chunk: f.write(chunk),
                        req.iter_content(chunk_size=1024)))
            zipfile.ZipFile(os.path.join(data_dir, 'drivers.zip')).extractall()
            os.remove(os.path.join(data_dir, 'drivers.zip'))

        print('drivers installed!')
