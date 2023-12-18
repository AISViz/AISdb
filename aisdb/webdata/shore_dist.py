''' Collect shore/port distances at given coordinates using NASA and Global
    Fishing Watch raster files.
    A (free) login is required to download Global Fishing Watch rasters,
    so these files must be manually downloaded and extracted into ``data_dir``.

    Raster data can be downloaded from:

    .. code-block::

        https://oceancolor.gsfc.nasa.gov/docs/distfromcoast/GMT_intermediate_coast_distance_01d.zip
        https://globalfishingwatch.org/data-download/datasets/public-distance-from-shore-v1
        https://globalfishingwatch.org/data-download/datasets/public-distance-from-port-v1

    once downloaded, place the unzipped geotiff files in `data_dir`
'''

import os
import zipfile

import requests
from tqdm import tqdm

from aisdb.webdata.load_raster import RasterFile

def download_unzip(data_url, data_dir, bytesize=0):
    assert os.path.isdir(data_dir), f'not a directory: {data_dir=}'
    zipf = os.path.join(data_dir, data_url.rsplit('/',1)[1])
    if not os.path.isfile(zipf):
        try:
            with requests.get(data_url, stream=True) as payload:
                assert payload.status_code == 200, 'error fetching file'
                with open(zipf, 'wb') as f:
                    with tqdm(total=bytesize,
                              desc=zipf,
                              unit='B',
                              unit_scale=True) as t:
                        for chunk in payload.iter_content(chunk_size=8192):
                            _ = t.update(f.write(chunk))
        except Exception as err:
            os.remove(zipf)
            raise err
    with zipfile.ZipFile(zipf, 'r') as zip_ref:
        members = list( fpath for fpath in
                       set(zip_ref.namelist()) -
                       set(sorted(os.listdir(data_dir))) if '.tif' in fpath)
        if len(members) > 0:
            zip_ref.extractall(path=data_dir, members=members)

class ShoreDist(RasterFile):

    data_url = "https://oceancolor.gsfc.nasa.gov/docs/distfromcoast/GMT_intermediate_coast_distance_01d.zip"

    def __init__(self, data_dir, tif_filename='GMT_intermediate_coast_distance_01d.tif'):
        download_unzip(self.data_url, data_dir, bytesize=657280)
        imgpath = os.path.join(data_dir, tif_filename)
        #imgpath = os.path.join(data_dir, 'distance-from-shore.tif')
        assert os.path.isfile(imgpath)
        super().__init__(imgpath)

    def get_distance(self, tracks):
        '''

        args:

        .. example:

        >>> y1, x1 = 48.271185186388735, -61.10595523571155
        >>>  # creating a sample track
        >>> tracks_short = [ dict(
        ...    lon=np.array([x1]), lat=np.array([y1]),
        ...     time=[0], dynamic=set(['time']), ) ]

        >>> with ShoreDist(data_dir="./testdata/") as sdist:
        >>> # getting distance from shore for each point in the track
        >>>     for track in sdist.get_distance(tracks_short):
        >>>         assert 'km_from_shore' in track.keys()
        >>>         assert 'km_from_shore' in track['dynamic']
        >>>         print(track['km_from_shore'])
        '''
        assert hasattr(self, 'imgpath')
        return self.merge_tracks(tracks, new_track_key='km_from_shore')


class PortDist(RasterFile):

    def get_distance(self, tracks):
        return self.merge_tracks(tracks, new_track_key='km_from_port')
