import os
import zipfile

import kadlu
import requests
from tqdm import tqdm
import rasterio


class Gebco():

    def fetch_bathymetry_grid(self):
        """ download geotiff zip archive and extract it """

        zipf = os.path.join(kadlu.storage_cfg(), "gebco_2020_geotiff.zip")

        # download the file if necessary
        if not os.path.isfile(zipf):
            logging.info('downloading and decompressing gebco bathymetry (netcdf ~8GB)... ')
            url = 'https://www.bodc.ac.uk/data/open_download/gebco/gebco_2020/geotiff/'
            with requests.get(url, stream=True) as payload_netcdf:
                assert payload_netcdf.status_code == 200, 'error fetching file'
                with open(zipf, 'wb') as f:
                    with tqdm(total=3730631664, desc=zipf, unit='B', unit_scale=True) as t:
                        for chunk in payload_netcdf.iter_content(chunk_size=8192): 
                            _ = t.update(f.write(chunk))

            # unzip the downloaded file
            with zipfile.ZipFile(zipf, 'r') as zip_ref:
                logging.info('extracting bathymetry data...')
                zip_ref.extractall(path=kadlu.storage_cfg())

        return


    def __enter__(self):
        self.fetch_bathymetry_grid()

        self.rasterfiles = { k : None for k in sorted([f for f in os.listdir(kadlu.storage_cfg()) if f[-4:] == '.tif' and 'gebco' in f ]) }

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
                    bounds.update({'dataset': rasterio.open(kadlu.storage_cfg() + filepath)})
                    bounds.update({'band1': bounds['dataset'].read(1)})
                ixlon, ixlat = bounds['dataset'].index(lon, lat)
                return bounds['band1'][ixlon-1,ixlat-1]




