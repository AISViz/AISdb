''' collect shore distance at a given coordinates using Global Fishing Watch raster files.

    the raster data used here can be downloaded from:
    https://globalfishingwatch.org/data-download/datasets/public-distance-from-shore-v1
    https://globalfishingwatch.org/data-download/datasets/public-distance-from-port-v1

    once downloaded, place the unzipped geotiff files in `data_dir`
'''

import os

import requests
import tqdm
import rasterio 

from common import *

class shore_dist_gfw():
    '''
    '''

    # load raster into memory
    def __enter__(self, shorerasterfile=f'distance-from-shore.tif', portrasterfile='distance-from-port-v20201104.tiff'):
        shorerasterpath = f'{data_dir}{os.path.sep}{shorerasterfile}'
        assert os.path.isfile(shorerasterpath)
        self.shoredata = rasterio.open(shorerasterpath)
        self.shoreband1 = self.shoredata.read(1)

        portrasterpath = f'{data_dir}{os.path.sep}{portrasterfile}'
        assert os.path.isfile(portrasterpath)
        self.portdata = rasterio.open(portrasterpath)
        self.portband1 = self.portdata.read(1)

        return self

    # cleanup resources on exit
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shoredata.close()
        self.shoredata.stop()
        del self.shoredata
        del self.shoreband1
        self.portdata.close()
        self.portdata.stop()
        del self.portdata
        del self.portband1

    # get approximate shore distance for coordinates (kilometres)
    def getdist(self, lon, lat):
        ixlon, ixlat = self.shoredata.index(lon, lat)
        return self.shoreband1[ixlon,ixlat]

    def getportdist(self, lon, lat):
        ixlon, ixlat = self.portdata.index(lon, lat)
        return self.portband1[ixlon,ixlat]

