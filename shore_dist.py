import os
import rasterio 

class shore_dist_gfw():
    '''
    the raster data file used here can be downloaded from:
    https://globalfishingwatch.org/data-download/datasets/public-distance-from-port-v1

    TODO: copy file download script from gebco.py
    '''
    def fetch_shoredist_raster(self):
        """ download geotiff zip archive and extract it """

        zipf = os.path.join(kadlu.storage_cfg(), "gfw_shoredist.zip")

        # download the file if necessary
        if not os.path.isfile(zipf):
            logging.info('downloading and decompressing gebco bathymetry (netcdf ~8GB)... ')
            url = 'https://www.bodc.ac.uk/data/open_download/gebco/gebco_2020/geotiff/'
            with requests.get(url, stream=True) as payload_netcdf:
                assert payload_netcdf.status_code == 200, 'error fetching file'
                with open(zipf, 'wb') as f:
                    with tqdm(total=3730631664, desc=zipf, unit='B', unit_scale=True) as t:
                        for chunk in payload_netcdf.iter_content(chunk_size=8192): 
                            _ = t.update(f.write(chunk))

            # unzip the downloaded file
            with zipfile.ZipFile(zipf, 'r') as zip_ref:
                logging.info('extracting bathymetry data...')
                zip_ref.extractall(path=kadlu.storage_cfg())

        return

    def __init__(self, dbpath):
        self.dirname, pathfile = dbpath.rsplit(os.path.sep, 1)

    # load raster into memory
    def __enter__(self, rasterfile=f'distance-from-port-v20201104.tiff'):
        rasterpath = f'{self.dirname}{os.path.sep}{rasterfile}'
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

