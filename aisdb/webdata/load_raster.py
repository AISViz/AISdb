import os

import numpy as np
from PIL import Image
try:
    import rasterio
    use_rasterio = True
except Exception:
    use_rasterio = False

from aisdb.proc_util import binarysearch
from aisdb.aisdb import binarysearch_vector

Image.MAX_IMAGE_PIXELS = 650000000  # suppress DecompressionBombError warning


class _RasterFile_generic():

    def __enter__(self):
        assert hasattr(self, 'img')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ''' close raster files upon exit from context '''
        self.img.close()

    def merge_tracks(self, tracks, new_track_key: str):
        for track in tracks:
            track['dynamic'] = set(track['dynamic']).union(set([new_track_key
                                                                ]))
            track[new_track_key] = self._track_coordinate_values(track)
            yield track


class RasterFile(_RasterFile_generic):

    def _get_img_grids(self, im):
        if 33922 in im.tag.tagdata.keys():
            # GDAL tags
            i, j, k, x, y, z = im.tag_v2[33922]  # ModelTiepointTag
            dx, dy, dz = im.tag_v2[33550]  # ModelPixelScaleTag
            lat = np.arange(y + dy, y + (dy * im.size[1]) + dy, dy)[::-1] - 90
            if np.sum(lat > 91):
                lat -= 90

        elif 34264 in im.tag.tagdata.keys():  # pragma: no cover
            # NASA JPL tags
            dx, _, _, x, _, dy, _, y, _, _, dz, z, _, _, _, _ = im.tag_v2[
                    34264]  # ModelTransformationTag
            lat = np.arange(y + dy, y + (dy * im.size[1]) + dy, dy)

        else:
            raise ValueError('error: unknown metadata tag encoding')

        lon = np.arange(x + dx, x + (dx * im.size[0]) + dx, dx)

        return lon, lat

    def __init__(self, imgpath):
        self.imgpath = imgpath
        assert not hasattr(self, 'img')
        assert os.path.isfile(
                self.imgpath), f'raster file {self.imgpath} not found!'
        self.img = Image.open(self.imgpath)
        self.xy = self._get_img_grids(self.img)

    def _get_coordinate_values(self, track, rng=None):
        if rng is None:
            rng = range(len(track['time']))
        idx_lons = np.array(binarysearch_vector(self.xy[0], track['lon'][rng]))
        idx_lats = np.array(binarysearch_vector(self.xy[1], track['lat'][rng]))
        return np.array(list(map( self.img.getpixel, zip(idx_lons, idx_lats),)))

    def _track_coordinate_values(self, track, *, rng: range = None):
        return self._get_coordinate_values(track, rng=rng)

if use_rasterio:

    class RasterFile_Rasterio(_RasterFile_generic):

        def __init__(self, imgpath):
            self.imgpath = imgpath
            assert not hasattr(self, 'img')
            assert os.path.isfile(
                    self.imgpath), f'raster file {self.imgpath} not found!'
            self.img = rasterio.open(self.imgpath)
            self.band1 = self.img.read(1)

        def _get_coordinate_value(self, lon, lat):
            ''' retrieve value of the specified coordinates '''
            x, y = self.img.index(lon, lat)
            return self.band1[x, y]

        def _track_coordinate_values(self, track, *, rng: range = None):
            if rng is None:  # pragma: no cover
                rng = range(len(track['time']))

            return np.array([
                self._get_coordinate_value(x, y)
                for x, y in zip(track['lon'][rng], track['lat'][rng])
                ])
