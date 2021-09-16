from proc_util import fast_unzip

zips = [os.path.join(rawdata_dir,r) for r in os.listdir(rawdata_dir) if r.lower().rfind('nm4') >= 0 and r.lower().rfind('zip') >= 0 ]

fast_unzip(zips, dirname=rawdata_dir, processes=min(len(zips), 32, os.cpu_count()//2))

