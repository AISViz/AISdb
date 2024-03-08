"""
This module calculates distances from the shore, coast, and the nearest port for given coordinates using raster
files from NASA and Global Fishing Watch. These calculations are valuable for various analyses, including
environmental impact studies, navigation safety assessments, and maritime activities monitoring.

To use this module, the necessary raster files must be first manually downloaded and placed in the specified
`data_dir` due to the requirement of a (free) login for accessing Global Fishing Watch rasters. This pre-step
ensures that the module operates smoothly without needing interactive authentication during runtime.

You can download the required raster data from the following sources:

- For distance from the coast calculations:
    https://oceancolor.gsfc.nasa.gov/docs/distfromcoast/GMT_intermediate_coast_distance_01d.zip

- For distance from shore and port calculations (Global Fishing Watch):
    - Distance from the shore:
        https://globalfishingwatch.org/data-download/datasets/public-distance-from-shore-v1
    - Distance from the nearest port:
        https://globalfishingwatch.org/data-download/datasets/public-distance-from-port-v1

After downloading, please extract the zip files and place the resulting geotiff files into the `data_dir`
you specified. This setup enables the module to directly access and utilize these raster files for
distance calculations. This documentation and the accompanying module aim to facilitate easy and
efficient access to marine geographical data for research and operational purposes.
"""
import os
import zipfile

import py7zr
import requests
from tqdm import tqdm

from aisdb.webdata.load_raster import RasterFile


def download_unzip(data_url, data_dir, bytesize=0, timeout=(10, 30)):
    """
    Download and unzip a file from a given URL to a specified directory.

    :param data_url: The URL of the file to download.
    :param data_dir: The directory in which to store the downloaded file.
    :param bytesize: The expected size of the file in bytes (optional, default=0).
    :param timeout: The timeout values for the connection and response in seconds (optional, default=(10, 30)).
    """
    assert os.path.isdir(data_dir), f"Not a directory: data_dir={data_dir}"
    filename = os.path.basename(data_url)
    zip_file = os.path.join(data_dir, filename)
    # Initialize a session for connection pooling
    session = requests.Session()

    # Check if the file exists and has the correct size
    if os.path.isfile(zip_file) and (bytesize == 0 or os.path.getsize(zip_file) == bytesize):
        print(f"File already exists: {zip_file}")
        return

    try:
        # Perform a HEAD request to check for file metadata.
        with session.head(data_url, timeout=timeout) as response:
            assert response.status_code == 200, "Error fetching file metadata"
            if 'Content-Length' in response.headers:
                content_length = int(response.headers['Content-Length'])
                # Update bytesize if it wasn't provided or differs.
                if bytesize != content_length:
                    bytesize = content_length
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return

    try:
        with session.get(data_url, stream=True, timeout=timeout) as payload:
            payload.raise_for_status()  # Will raise an HTTPError for bad responses.
            with open(zip_file, 'wb') as f, tqdm(total=bytesize, desc=filename, unit="B", unit_scale=True) as t:
                for chunk in payload.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        t.update(f.write(chunk))
    except requests.RequestException as err:
        os.remove(zip_file)
        print(f"Download failed: {err}")

    if zip_file.endswith('.zip'):
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            members = list(
                fpath for fpath in set(zip_ref.namelist()) - set(sorted(os.listdir(data_dir))) if '.tif' in fpath)
            if len(members) > 0:
                zip_ref.extractall(path=data_dir, members=members)
    elif zip_file.endswith('.7z'):
        with py7zr.SevenZipFile(zip_file, mode='r') as zip_ref:
            members = list(
                fpath for fpath in set(zip_ref.getnames()) - set(sorted(os.listdir(data_dir))) if '.tif' in fpath)
            if len(members) > 0:
                zip_ref.extract(targets=members, path=data_dir)
    else:
        assert False, "File type not supported (use .zip or .7z)."


class ShoreDist(RasterFile):
    # This is self-stored data to easy the deployment process
    data_url = "http://bigdata5.research.cs.dal.ca/raster-shore.7z"

    def __init__(self, data_dir, tif_filename='distance-from-shore.tif'):
        download_unzip(self.data_url, data_dir, bytesize=39911958)
        img_path = os.path.join(data_dir, tif_filename)
        assert os.path.isfile(img_path)
        super().__init__(img_path)

    def get_distance(self, tracks):
        """
        This method calculates the distance of each track in the given tracks list from the shore. It first checks if
        the 'imgpath' attribute exists in the class object calling the method. Then, it calls the 'merge_tracks' method,
        passing the tracks list and specifying the new track key as 'km_from_shore'. The 'merge_tracks' method merges
        all the tracks into a single track object and adds the new track key with the calculated distance.

        :param tracks: A list of track objects
        :return: A merged track object with a new track key 'km_from_shore'
        :example:
        >>> y1, x1 = 48.271185186388735, -61.10595523571155
        >>>  # creating a sample track
        >>> tracks_short = [ dict(
        ...    lon=np.array([x1]), lat=np.array([y1]),
        ...     time=[0], dynamic={'time'}, ) ]

        >>> with ShoreDist(data_dir="./testdata/") as sdist:
        >>> # getting distance from shore for each point in the track
        >>>     for track in sdist.get_distance(tracks_short):
        >>>         assert 'km_from_shore' in track.keys()
        >>>         assert 'km_from_shore' in track['dynamic']
        >>>         print(track['km_from_shore'])
        """
        assert hasattr(self, 'imgpath')
        return self.merge_tracks(tracks, new_track_key='km_from_shore')


class PortDist(RasterFile):
    # This is self-stored data to ease the deployment process
    data_url = "http://bigdata5.research.cs.dal.ca/raster-ports.7z"

    def __init__(self, data_dir, tif_filename='distance-from-port-v20201104.tiff'):
        download_unzip(self.data_url, data_dir, bytesize=1263005549)
        img_path = os.path.join(data_dir, tif_filename)
        assert os.path.isfile(img_path)
        super().__init__(img_path)

    def get_distance(self, tracks):
        """
        This method calculates the distance of each track in the given track list from the nearest port. It first checks
        if the 'imgpath' attribute exists in the class object calling the method. Then, it calls the 'merge_tracks'
        method, passing the tracks list and specifying the new track key as 'km_from_port'. The 'merge_tracks' method
        merges all the tracks into a single track object and adds the new track key with the calculated distance.

        :param tracks: A list of track objects
        :return: A merged track object with a new track key 'km_from_port'
        :example:
        >>> y1, x1 = 48.271185186388735, -61.10595523571155
        >>>  # Creating a sample track
        >>> tracks_short = [dict(
        ...    lon=np.array([x1]), lat=np.array([y1]),
        ...     time=[0], dynamic={'time'}, )]

        >>> with PortDist(data_dir="./testdata/") as pdist:
        >>> # Getting distance from the nearest port for each point in the track
        >>>     for track in pdist.get_distance(tracks_short):
        >>>         assert 'km_from_port' in track.keys()
        >>>         assert 'km_from_port' in track['dynamic']
        >>>         print(track['km_from_port'])
        """
        assert hasattr(self, 'imgpath')
        return self.merge_tracks(tracks, new_track_key='km_from_port')


class CoastDist(RasterFile):
    # This is self-stored data to ease the deployment process
    data_url = "http://bigdata5.research.cs.dal.ca/raster-coast.7z"

    def __init__(self, data_dir, tif_filename='GMT_intermediate_coast_distance_01d.tif'):
        download_unzip(self.data_url, data_dir, bytesize=58802115)
        img_path = os.path.join(data_dir, tif_filename)
        assert os.path.isfile(img_path)
        super().__init__(img_path)

    def get_distance(self, tracks):
        """
        This method calculates the distance of each track in the given track list from the nearest coast. It checks
        if the 'imgpath' attribute exists in the class object calling the method. Then, it calls the 'merge_tracks'
        method, passing the tracks list and specifying the new track key as 'km_from_coast'. The 'merge_tracks' method
        merges all the tracks into a single track object and adds the new track key with the calculated distance.

        :param tracks: A list of track objects
        :return: A merged track object with a new track key 'km_from_coast'
        :example:
        >>> y1, x1 = 48.271185186388735, -61.10595523571155
        >>>  # Creating a sample track
        >>> tracks_short = [dict(
        ...    lon=np.array([x1]), lat=np.array([y1]),
        ...     time=[0], dynamic={'time'}, )]

        >>> with CoastDist(data_dir="./testdata/") as cdist:
        >>> # Getting distance from the nearest coast for each point in the track
        >>>     for track in cdist.get_distance(tracks_short):
        >>>         assert 'km_from_coast' in track.keys()
        >>>         assert 'km_from_coast' in track['dynamic']
        >>>         print(track['km_from_coast'])
        """
        assert hasattr(self, 'imgpath')
        return self.merge_tracks(tracks, new_track_key='km_from_coast')
