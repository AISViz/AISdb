import os
import rasterio 

from common import *

class shore_dist_gfw():
    '''
    the raster data file used here can be downloaded from:
    https://globalfishingwatch.org/data-download/datasets/public-distance-from-port-v1

    TODO: copy file download script from gebco.py
    '''

    # load raster into memory
    def __enter__(self, rasterfile=f'distance-from-port-v20201104.tiff'):
        rasterpath = f'{data_dir}{os.path.sep}{rasterfile}'
        assert os.path.isfile(rasterpath)
        self.dataset = rasterio.open(rasterpath)
        self.band1 = self.dataset.read(1)
        return self

    # cleanup resources on exit
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dataset.close()
        self.dataset.stop()
        del self.dataset
        del self.band1

    # get approximate shore distance for coordinates (kilometres)
    def getdist(self, lon, lat):
        ixlon, ixlat = self.dataset.index(lon, lat)
        return self.band1[ixlon,ixlat]

