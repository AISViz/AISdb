import os
import zipfile

import requests
from tqdm import tqdm
import rasterio


class Gebco():

    def __init__(self, dbpath):
        self.dirname, pathfile = dbpath.rsplit(os.path.sep, 1)

    def fetch_bathymetry_grid(self):
        """ download geotiff zip archive and extract it """

        zipf = os.path.join(self.dirname, "gebco_2020_geotiff.zip")

        # download the file if necessary
        if not os.path.isfile(zipf):
            print('downloading gebco bathymetry (geotiff ~8GB decompressed)... ')
            url = 'https://www.bodc.ac.uk/data/open_download/gebco/gebco_2020/geotiff/'
            with requests.get(url, stream=True) as payload:
                assert payload.status_code == 200, 'error fetching file'
                with open(zipf, 'wb') as f:
                    with tqdm(total=3730631664, desc=zipf, unit='B', unit_scale=True) as t:
                        for chunk in payload.iter_content(chunk_size=8192): 
                            _ = t.update(f.write(chunk))

            # unzip the downloaded file
            with zipfile.ZipFile(zipf, 'r') as zip_ref:
                print('extracting bathymetry data...')
                zip_ref.extractall(path=self.dirname)

        return


    def __enter__(self):
        self.fetch_bathymetry_grid()

        self.rasterfiles = { k : None for k in sorted([f for f in os.listdir(self.dirname) if f[-4:] == '.tif' and 'gebco' in f ]) }

        filebounds = lambda fpath: { f[0]: float(f[1:]) for f in fpath.split('gebco_2020_', 1)[1].rsplit('.tif', 1)[0].split('_') }

        self.rasterfiles = { f : filebounds(f) for f in self.rasterfiles }

        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        for filepath, bounds in self.rasterfiles.items():
            if 'dataset' in bounds.keys():
                bounds['dataset'].close()
                bounds['dataset'].stop()
                del bounds['dataset']
                del bounds['band1']


    def getdepth(self, lon, lat):
        for filepath, bounds in self.rasterfiles.items():
            if bounds['w'] <= lon <=  bounds['e'] and bounds['s'] <= lat <= bounds['n']: 
                if not 'band1' in bounds.keys(): 
                    bounds.update({'dataset': rasterio.open(os.path.join(self.dirname, filepath))})
                    bounds.update({'band1': bounds['dataset'].read(1)})
                ixlon, ixlat = bounds['dataset'].index(lon, lat)
                return bounds['band1'][ixlon-1,ixlat-1]


