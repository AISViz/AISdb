
import os
import sys
import subprocess
subprocess.run(f'{sys.executable} -m pip install numpy requests selenium'.split())
import requests
import tarfile
import zipfile
import stat

print('downloading webdrivers...')

'''
__file__ = 'webdata/install_dep.py'
'''
if not os.path.isfile(os.path.join(os.path.dirname(__file__), 'webdriver')) and not os.path.isfile(os.path.join(os.path.dirname(__file__), 'webdriver.exe')):
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

