import numpy as np
from PIL import Image

from aisdb.proc_util import binarysearch
from aisdb import data_dir


def pixelindex(x1, y1, im):
    ''' convert WGS84 coordinates to raster grid index

        image tag spec:
        http://duff.ess.washington.edu/data/raster/drg/docs/geotiff.txt

        args:
            x1: float
                longitude coordinate
            y1: float
                latitude coordinate
            img: pillow PIL.Image file
                can be supplied instead of a filepath

        returns:
            (x, y) array indices
    '''

    # GDAL tags
    if 33922 in im.tag.tagdata.keys():
        i, j, k, x, y, z = im.tag_v2[33922]  # ModelTiepointTag
        dx, dy, dz = im.tag_v2[33550]  # ModelPixelScaleTag
        lat = np.arange(y, y + (dy * im.size[1]), dy)[::-1] - 90
        if sum(lat > 91): lat -= 90

    # NASA JPL tags
    elif 34264 in im.tag.tagdata.keys():
        dx, _, _, x, _, dy, _, y, _, _, dz, z, _, _, _, _ = im.tag_v2[
            34264]  # ModelTransformationTag
        lat = np.arange(y, y + (dy * im.size[1]), dy)

    else:
        assert False, f'error {filepath}: unknown metadata tag encoding'

    lon = np.arange(x, x + (dx * im.size[0]), dx)

    idx_lon = binarysearch(lon, x1)
    idx_lat = binarysearch(lat, y1, descending=True)

    return idx_lon, idx_lat


def load_raster_pixel(x1, y1, filepath=None, img=None):
    """ load pixel data from raster file

        args:
            filepath: string
                path to rasterfile
            x1: float
                longitude coordinate
            y1: float
                latitude coordinate
            filepath: string
                raster image location
            img: pillow PIL.Image file
                can be supplied instead of a filepath.
                image will not be closed after loading data when
                using this option

        returns:
            pixel value
    """
    if (not filepath and not img) or (filepath and img):
        assert False, 'must pass filepath or PIL.Image'

    Image.MAX_IMAGE_PIXELS = 650000000  # suppress DecompressionBombError warning

    if filepath:
        im = Image.open(filepath)
    else:
        im = img

    px = im.getpixel(pixelindex(x1, y1, im))

    if filepath:
        im.close()

    return px
