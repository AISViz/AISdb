import os
import zipfile
from multiprocessing import Pool
from functools import partial


def _fast_unzip(zipf, dirname='.'):
    exists = set(sorted(os.listdir(dirname)))
    with zipfile.ZipFile(zipf, 'r') as zip_ref:
        contents = set(zip_ref.namelist())
        members = list(contents - exists)
        zip_ref.extractall(path=dirname, members=members)


def fast_unzip(zipfilenames, dirname='.', processes=12):
    fcn = partial(_fast_unzip, dirname=dirname)
    with Pool(processes) as p:
        p.imap_unordered(fcn, zipfilenames)
        p.close()
        p.join()


def binarysearch(arr, search):
    ''' fast indexing of ordered arrays '''
    low, high = 0, len(arr)-1
    while (low <= high):
        mid = int((low + high) / 2)
        if arr[mid] == search or mid == 0 or mid == len(arr)-1:
            return mid
        elif (arr[mid] >= search):
            high = mid -1 
        else:
            low = mid +1
    return mid


#def tmpdir(dbpath):
#    path, dbfile = dbpath.rsplit(os.path.sep, 1)
#    tmpdirpath = os.path.join(path, 'tmp_parsing')
#    if not os.path.isdir(tmpdirpath):
#        os.mkdir(tmpdirpath)
#    return tmpdirpath

