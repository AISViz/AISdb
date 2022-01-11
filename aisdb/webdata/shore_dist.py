''' collect shore distance at a given coordinates using Global Fishing Watch raster files.

    the raster data used here can be downloaded from:

    .. code-block::

        https://globalfishingwatch.org/data-download/datasets/public-distance-from-shore-v1
        https://globalfishingwatch.org/data-download/datasets/public-distance-from-port-v1

    once downloaded, place the unzipped geotiff files in `data_dir`
'''

import os

from PIL import Image

from aisdb import data_dir
from aisdb.webdata.load_raster import load_raster_pixel


class shore_dist_gfw():

    def __enter__(self,
                  shorerasterfile='distance-from-shore.tif',
                  portrasterfile='distance-from-port-v20201104.tiff'):
        ''' load rasters into memory '''
        # suppress DecompressionBombError warning
        Image.MAX_IMAGE_PIXELS = 650000000

        shorerasterpath = os.path.join(data_dir, shorerasterfile)
        assert os.path.isfile(
            shorerasterpath
        ), ' raster file not found! see docstring for download URL'
        self.shoreimg = Image.open(shorerasterpath)

        portrasterpath = os.path.join(data_dir, portrasterfile)
        assert os.path.isfile(
            portrasterpath
        ), ' raster file not found! see docstring for download URL'
        self.portimg = Image.open(portrasterpath)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        ''' close raster files upon exit from context '''
        self.shoreimg.close()
        self.portimg.close()

    def getdist(self, lon, lat):
        ''' get approximate shore distance for coordinates (kilometres) '''
        return load_raster_pixel(lon, lat, img=self.shoreimg)

    def getportdist(self, lon, lat):
        ''' get approximate port distance for coordinates (kilometres) '''
        return load_raster_pixel(lon, lat, img=self.portimg)


'''
with shore_dist_gfw() as sdist:
    sdist.getdist(lon=-63.3, lat=44.5)
    sdist.getportdist(lon=-63.3, lat=44.5)
'''
