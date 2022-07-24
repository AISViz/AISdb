import os

import numpy as np
from PIL import Image
import rasterio

from aisdb.proc_util import binarysearch

Image.MAX_IMAGE_PIXELS = 650000000  # suppress DecompressionBombError warning


def _get_img_grids(im):
    # GDAL tags
    if 33922 in im.tag.tagdata.keys():
        i, j, k, x, y, z = im.tag_v2[33922]  # ModelTiepointTag
        dx, dy, dz = im.tag_v2[33550]  # ModelPixelScaleTag
        lat = np.arange(y, y + (dy * im.size[1]), dy)[::-1] - 90
        if np.sum(lat > 91):
            lat -= 90
    # NASA JPL tags
    elif 34264 in im.tag.tagdata.keys():  # pragma: no cover
        dx, _, _, x, _, dy, _, y, _, _, dz, z, _, _, _, _ = im.tag_v2[
            34264]  # ModelTransformationTag
        lat = np.arange(y, y + (dy * im.size[1]), dy)

    else:
        raise ValueError('error: unknown metadata tag encoding')

    lon = np.arange(x, x + (dx * im.size[0]), dx)

    return lon, lat


def pixelindex(x1, y1, lon, lat):
    ''' convert WGS84 coordinates to raster grid index

        image tag spec:
        http://duff.ess.washington.edu/data/raster/drg/docs/geotiff.txt

        args:
            x1 (float)
                longitude coordinate
            y1 (float)
                latitude coordinate
            lon (np.ndarray)
                coordinate grid X values
            lat (np.ndarray)
                coordinate grid Y values

        returns:
            (x, y) array indices
    '''

    idx_lon = binarysearch(lon, x1)
    idx_lat = binarysearch(lat, y1)

    return idx_lon, idx_lat


class RasterFile():

    def __init__(self, imgpath):
        self.imgpath = imgpath
        assert not hasattr(self, 'img')
        assert os.path.isfile(
            self.imgpath), f'raster file {self.imgpath} not found!'
        self.img = Image.open(self.imgpath)
        self.xy = _get_img_grids(self.img)

    def __enter__(self):
        ''' load rasters into memory '''
        assert hasattr(self, 'img')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ''' close raster files upon exit from context '''
        self.img.close()

    def _get_coordinate_value(self, lon, lat):
        return self.img.getpixel(pixelindex(lon, lat, *self.xy))

    def _merge_tracks(self, tracks, new_track_key: str):
        for track in tracks:
            track[new_track_key] = np.array([
                self._get_coordinate_value(x, y)
                for x, y in zip(track['lon'], track['lat'])
            ])
            track['dynamic'] = set(track['dynamic']).union(set([new_track_key
                                                                ]))
            yield track


class RasterFile_Rasterio(RasterFile):

    def _get_coordinate_value(self, lon, lat):
        x, y = self.img.index(lon, lat)
        return self.band1[x, y]

    def __init__(self, imgpath):
        self.imgpath = imgpath
        assert not hasattr(self, 'img')
        assert os.path.isfile(
            self.imgpath), f'raster file {self.imgpath} not found!'
        self.img = rasterio.open(self.imgpath)
        self.band1 = self.img.read(1)
