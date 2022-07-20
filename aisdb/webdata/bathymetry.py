''' load bathymetry data from GEBCO raster files '''

import os
import zipfile
import time
import hashlib

from PIL import Image
from tqdm import tqdm
import requests
import rasterio

from aisdb.webdata.load_raster import pixelindex_rasterio, load_raster_pixel
from aisdb.proc_util import fast_unzip

Image.MAX_IMAGE_PIXELS = 650000000  # suppress DecompressionBombError

url = 'https://www.bodc.ac.uk/data/open_download/gebco/gebco_2022/geotiff/'

_filebounds = lambda fpath: {
    f[0]: float(f[1:])
    for f in fpath.split('gebco_2022_', 1)[1].rsplit('.tif', 1)[0].split('_')
}


class Gebco():

    def __init__(self, data_dir):
        '''
            args:
                data_dir (string)
                    folder where rasters should be stored
        '''
        self.rasterfiles = None
        self.data_dir = data_dir
        self.griddata = os.path.join(self.data_dir, 'griddata.db')
        if not os.path.isdir(data_dir):
            os.mkdir(data_dir)
        self.__enter__()

    def __enter__(self):
        #Image.MAX_IMAGE_PIXELS = 650000000  # suppress DecompressionBombError

        if self.rasterfiles is not None:
            return self

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
        self.fetch_bathymetry_grid()  # download bathymetry rasters if missing

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for filepath, bounds in self.rasterfiles.items():
            if 'img' in bounds.keys():
                bounds['img'].close()

    def fetch_bathymetry_grid(self):
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

            fast_unzip([zipf], os.path.dirname(zipf), 1)

        # zzz
        time.sleep(5)
        return
        """

        if os.path.isfile(self.griddata):
            return

        print('creating bathymetry SQL database from raster files...\n'
              f'this will create a ~200gb file at {self.griddata}')

        rasterfiles = sorted([
            os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir)
            if f[-4:] == '.tif' and 'gebco_2022' in f
        ])
        Image.MAX_IMAGE_PIXELS = 650000000  # suppress DecompressionBombError
        insert_stmt = ('INSERT INTO gebco_2022(x0, x1, y0, y1, depth_metres) '
                       'VALUES (?,?,?,?,?)')

        with sqlite3.Connection(self.griddata) as db:
            with open(os.path.join(sqlpath, 'createtable_griddata.sql'),
                      'r') as qrytxt:
                _ = db.execute(qrytxt.read())
            for r in rasterfiles:
                #if r not in (
                #        '/RAID0/ais/gebco_2022_n90.0_s0.0_w-90.0_e0.0.tif',
                #        '/RAID0/ais/gebco_2022_n90.0_s0.0_w-180.0_e-90.0.tif'):
                #    continue
                im = Image.open(r)

                # GDAL tags
                if 33922 in im.tag.tagdata.keys():
                    i, j, k, x, y, z = im.tag_v2[33922]  # ModelTiepointTag
                    dx, dy, dz = im.tag_v2[33550]  # ModelPixelScaleTag
                    lat = np.arange(y, y + (dy * im.size[1]), dy)[::-1] - 90
                    if np.sum(lat > 91):
                        lat -= 90

                # NASA JPL tags
                elif 34264 in im.tag.tagdata.keys():
                    dx, _, _, x, _, dy, _, y, _, _, dz, z, _, _, _, _ = im.tag_v2[
                        34264]  # ModelTransformationTag
                    lat = np.arange(y, y + (dy * im.size[1]), dy)

                else:
                    raise ValueError(f'unknown metadata tag encoding in {r}')

                lon = np.arange(x, x + (dx * im.size[0]), dx)

                xsize = len(lon)
                #val = im.getdata()
                im.close()
                xspacing = lon[1] - lon[0]
                yspacing = lat[1] - lat[0]

                for x, i in zip(lon, range(xsize)):
                    rowlon0 = (x for _ in range(xsize))
                    rowlon1 = (x + xspacing for _ in range(xsize))
                    rowlat = np.append(lat[0:xsize],
                                       [lat[xsize - 1] + yspacing])
                    rowlat0 = rowlat[:-1] if rowlat[0] <= rowlat[
                        1] else rowlat[1:]
                    rowlat1 = rowlat[
                        1:] if rowlat[0] <= rowlat[1] else rowlat[:-1]
                    rowval = (self.getdepth(x, y)
                              for x, y in zip(rowlon0, rowlat0))
                    insertrows = zip(rowlon0, rowlon1, rowlat0, rowlat1,
                                     rowval)

                    _ = db.executemany(insert_stmt, insertrows)
                print(f'completed {r}')

        return
    """

    def getdepth_manual(self, lon, lat):
        ''' get grid cell elevation value for given coordinate.
            negative values indicate below sealevel
        '''
        for filepath, bounds in self.rasterfiles.items():
            if bounds['w'] <= lon <= bounds['e'] and bounds[
                    's'] <= lat <= bounds['n']:
                if 'img' not in bounds.keys():
                    bounds['img'] = Image.open(
                        os.path.join(self.data_dir, filepath))
                return load_raster_pixel(lon, lat, img=bounds['img'])
        raise ValueError(f'given lon, lat not in files! {lon=} {lat=}')

    def getdepth(self, lon, lat):
        ''' get grid cell elevation value for given coordinate.
            negative values indicate below sealevel
        '''
        for filepath, bounds in self.rasterfiles.items():
            if bounds['w'] <= lon <= bounds['e'] and bounds[
                    's'] <= lat <= bounds['n']:
                if 'img' not in bounds.keys():
                    #imgfile = { 'img': Image.open(os.path.join(self.data_dir, filepath)) }
                    raster = rasterio.open(
                        os.path.join(self.data_dir, filepath))
                    rasterdata = {'img': raster, 'band1': raster.read(1)}
                    bounds.update(rasterdata)
                #return load_raster_pixel(lon, lat, img=bounds['img']) * -1
                return pixelindex_rasterio(lon,
                                           lat,
                                           dataset=bounds['img'],
                                           band1=bounds['band1'])
        raise ValueError(f'given lon, lat not in files! {lon=} {lat=}')

    """
    def getdepth_cellborders_nonnegative_avg(self, lon, lat):
        ''' get the average depth of surrounding grid cells from the given
            coordinate.
            the absolute value of depths below sea level will be averaged
        '''

        for filepath, bounds in self.rasterfiles.items():
            if bounds['w'] <= lon <= bounds['e'] and bounds[
                    's'] <= lat <= bounds['n']:
                if 'img' not in bounds.keys():
                    bounds.update({
                        'img':
                        Image.open(os.path.join(self.data_dir, filepath))
                    })

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
    """
