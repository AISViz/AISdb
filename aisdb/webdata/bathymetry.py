''' load bathymetry data from GEBCO raster files '''

import os
import zipfile

from PIL import Image
from tqdm import tqdm
import numpy as np
import requests

from aisdb.webdata.load_raster import pixelindex, load_raster_pixel
from aisdb import data_dir

url = 'https://www.bodc.ac.uk/data/open_download/gebco/gebco_2021/geotiff/'


class Gebco():

    def fetch_bathymetry_grid(self):
        """ download geotiff zip archive and extract it """

        zipf = os.path.join(data_dir, "gebco_2021_geotiff.zip")

        # download the file if necessary
        if not os.path.isfile(zipf):
            print('downloading gebco bathymetry...')
            with requests.get(url, stream=True) as payload:
                assert payload.status_code == 200, 'error fetching file'
                with open(zipf, 'wb') as f:
                    with tqdm(total=4011413504,
                              desc=zipf,
                              unit='B',
                              unit_scale=True) as t:
                        for chunk in payload.iter_content(chunk_size=8192):
                            _ = t.update(f.write(chunk))

            # unzip the downloaded file
            exists = set(sorted(os.listdir(data_dir)))
            with zipfile.ZipFile(zipf, 'r') as zip_ref:
                contents = set(zip_ref.namelist())
                members = list(contents - exists)
                print('extracting bathymetry data...')
                zip_ref.extractall(path=data_dir, members=members)

        return

    def __enter__(self):
        self.fetch_bathymetry_grid()  # download bathymetry rasters if missing
        Image.MAX_IMAGE_PIXELS = 650000000  # suppress DecompressionBombError

        filebounds = lambda fpath: {
            f[0]: float(f[1:])
            for f in fpath.split('gebco_2021_', 1)[1].rsplit('.tif', 1)[0].
            split('_')
        }

        self.rasterfiles = {
            f: filebounds(f)
            for f in {
                k: None
                for k in sorted([
                    f for f in os.listdir(data_dir)
                    if f[-4:] == '.tif' and 'gebco' in f
                ])
            }
        }

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for filepath, bounds in self.rasterfiles.items():
            if 'img' in bounds.keys():
                bounds['img'].close()

    def getdepth(self, lon, lat):
        ''' get grid cell elevation value for given coordinate.
            negative values indicate below sealevel
        '''
        for filepath, bounds in self.rasterfiles.items():
            if bounds['w'] <= lon <= bounds['e'] and bounds[
                    's'] <= lat <= bounds['n']:
                if 'img' not in bounds.keys():
                    bounds.update(
                        {'img': Image.open(os.path.join(data_dir, filepath))})
                return load_raster_pixel(lon, lat, img=bounds['img']) * -1

    def getdepth_cellborders_nonnegative_avg(self, lon, lat):
        ''' get the average depth of surrounding grid cells from the given
            coordinate.
            the absolute value of depths below sea level will be averaged
        '''

        for filepath, bounds in self.rasterfiles.items():
            if bounds['w'] <= lon <= bounds['e'] and bounds[
                    's'] <= lat <= bounds['n']:
                if 'img' not in bounds.keys():
                    bounds.update(
                        {'img': Image.open(os.path.join(data_dir, filepath))})

                ixlon, ixlat = pixelindex(lon, lat, bounds['img'])
                depths = np.array([
                    bounds['img'].getpixel((xlon, xlat))
                    for xlon in range(ixlon - 1, ixlon + 2)
                    for xlat in range(ixlat - 1, ixlat + 2)
                    if (xlon != ixlon and xlat != ixlat) and (
                        0 <= xlon <= bounds['img'].size[0]) and (
                            0 <= xlat <= bounds['img'].size[1])
                ])

                return np.average(depths * -1)
