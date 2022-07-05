from os.path import getsize
from zipfile import BadZipFile

import aisdb

# create databases from zipped data files in unzipped directory
zipfiles = aisdb.proc_util.glob_files(
    dirpath='/run/media/matt/My Passport/',
    ext='.zip',
)
unzip_dir = '/vol1/AIS_decompressed_meopar/'

# database output path
# '{}' will be replaced with the calendar year, e.g. 2022
basepath = '/home/matt/meridian_{}.db'

for zipfilelocation in zipfiles:

    # set data source
    # will be inserted as 'source' column in primary key
    if 'terr' in zipfilelocation:
        source = 'exactEarth_terrestrial'
    else:
        source = 'exactEarth'

    # unzip file into unzip_dir and handle errors
    print(f'unzipping {zipfilelocation}...')
    try:
        aisdb.proc_util._fast_unzip(zipf=zipfilelocation, dirname=unzip_dir)
    except BadZipFile:
        print(f'{zipfilelocation} err: bad zip file')
        continue
    except Exception as err:
        print(f'{zipfilelocation} err: {err.with_traceback(None)}')
        continue

    # iterate through unzipped files
    for filelocation in (aisdb.proc_util.glob_files(unzip_dir, ext='.nm4') +
                         aisdb.proc_util.glob_files(unzip_dir, ext='.csv')):

        # if file is empty, skip it
        if getsize(filelocation) == 0:
            continue

        # check approximate date of data within file
        try:
            filedate = aisdb.proc_util.getfiledate(filelocation)
        except Exception as err:
            print(f'couldn\'t get file date! {filelocation}\n'
                  f'{err.with_traceback(None)}')
            continue
        if filedate is False:
            print(f'bad file date {filelocation}')
            continue

        # insert into SQL database
        dbpath = basepath.replace('{}', str(filedate.year))
        aisdb.decode_msgs(
            filepaths=[filelocation],
            dbpath=dbpath,
            source=source,
        )

        # zero the unzipped data file
        with open(filelocation, 'wb') as f:
            _ = f.write(b'')
