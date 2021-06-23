import os
import rasterio 


class shore_dist_gfw():
    '''
    the raster data file used here can be downloaded from:
    https://globalfishingwatch.org/data-download/datasets/public-distance-from-port-v1
    '''

    def __init__(self, rasterfile='input/distance-from-port-v20201104.tiff'):
        assert os.path.isfile(rasterfile)
        self.dataset = rasterio.open(raster)
        self.band1 = self.dataset.read(1)


    def getdist(lon, lat):
        ixlon, ixlat = self.dataset.index(lon, lat)
        return self.band1[ixlon,ixlat]

