''' load bathymetry data from GEBCO raster files '''

import os
import zipfile
import time
import hashlib

import numpy as np

from aisdb.webdata.load_raster import RasterFile

from tqdm import tqdm
import requests

url = 'https://www.bodc.ac.uk/data/open_download/gebco/gebco_2022/geotiff/'


def _filebounds(fpath):
    return {
        f[0]: float(f[1:])
        for f in fpath.split('gebco_2022_', 1)[1].rsplit('.tif', 1)[0].split(
            '_')
    }


class Gebco():

    def __init__(self, data_dir):
        '''
            args:
                data_dir (string)
                    folder where rasters should be stored
        '''
        self.data_dir = data_dir
        # self.griddata = os.path.join(self.data_dir, 'griddata.db')
        assert os.path.isdir(data_dir)
        self.__enter__()

    def __enter__(self):
        self.rasterfiles = {
            f: _filebounds(f)
            for f in {
                k: None
                for k in sorted([
                    f for f in os.listdir(self.data_dir)
                    if f[-4:] == '.tif' and 'gebco_2022' in f
                ])
            }
        }

        # download bathymetry rasters if missing
        self.fetch_bathymetry_grid()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_all()

    def fetch_bathymetry_grid(self):  # pragma: no cover
        """ download geotiff zip archive and extract it """

        zipf = os.path.join(self.data_dir, "gebco_2022_geotiff.zip")

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
            with open(zipf, 'rb') as z:
                sha256sum = hashlib.sha256(z.read()).hexdigest()
            print('verifying checksum...')
            assert sha256sum == '5ade15083909fcd6003409df678bdc6537b8691df996f8d806b48de962470cc3',\
                    'checksum failed!'

            with zipfile.ZipFile(zipf, 'r') as zip_ref:
                members = list(
                    set(zip_ref.namelist()) -
                    set(sorted(os.listdir(self.data_dir))))
                print('extracting bathymetry data...')
                zip_ref.extractall(path=self.data_dir, members=members)

        # zzz
        time.sleep(5)
        return

    def _filter_raster_bounds(self, pair, track):
        filename, bounds = pair
        if 'raster' in bounds.keys():
            return False
        in_lon = np.logical_and(bounds['w'] <= track['lon'],
                                track['lon'] <= bounds['e'])
        in_lat = np.logical_and(bounds['s'] <= track['lat'],
                                track['lat'] <= bounds['n'])
        if sum(np.logical_and(in_lon, in_lat)) == 0:
            return False
        bounds['raster'] = RasterFile(
            imgpath=os.path.join(self.data_dir, filename))
        return True

    def _check_in_bounds(self, track):
        for lon, lat in zip(track['lon'], track['lat']):
            tracer = False
            for key, bounds in self.rasterfiles.items():
                if (bounds['w'] < lon < bounds['e']
                        and bounds['s'] < lat < bounds['n']):
                    tracer = True
                    if 'raster' not in bounds.keys():
                        bounds['raster'] = RasterFile(
                            imgpath=os.path.join(self.data_dir, key))
                    yield key
                    break
            assert tracer
        return

    def _close_all(self):
        for filepath, bounds in self.rasterfiles.items():
            if 'raster' in bounds.keys():
                bounds['raster'].img.close()

    def merge_tracks(self, tracks):
        for track in tracks:
            track['dynamic'] = set(track['dynamic']).union(
                set(['depth_metres']))

            raster_keys = np.array(list(self._check_in_bounds(track)),
                                   dtype=object)
            assert len(raster_keys) == len(track['time'])
            track['depth_metres'] = np.array([
                self.rasterfiles[raster_keys[i]]
                ['raster']._get_coordinate_value(
                    track['lon'][i],
                    track['lat'][i],
                ) for i in range(len(track['time']))
            ])

            yield track
        self._close_all()
