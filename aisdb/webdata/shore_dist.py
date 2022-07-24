''' collect shore distance at a given coordinates using Global Fishing Watch raster files.

    the raster data used here can be downloaded from:

    .. code-block::

        https://globalfishingwatch.org/data-download/datasets/public-distance-from-shore-v1
        https://globalfishingwatch.org/data-download/datasets/public-distance-from-port-v1

    once downloaded, place the unzipped geotiff files in `data_dir`
'''

from aisdb.webdata.load_raster import RasterFile


class ShoreDist(RasterFile):

    def get_distance(self, tracks):
        return self._merge_tracks(tracks, new_track_key='km_from_shore')


class PortDist(RasterFile):

    def get_distance(self, tracks):
        return self._merge_tracks(tracks, new_track_key='km_from_port')
