from aisdb import rawdata_dir, data_dir, output_dir
from aisdb.proc_util import fast_unzip, glob_files, _fast_unzip


files = glob_files(rawdata_dir, ext='.zip')

fast_unzip(files, dirname=output_dir, processes=4)

_fast_unzip(files[0], dirname=output_dir)
