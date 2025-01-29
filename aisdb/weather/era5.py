import datetime
import os
import tempfile
import xarray as xr
import numpy as np
from aisdb.database.decoder import fast_unzip


def dt_to_iso8601(timestamp):
    dt = datetime.datetime.fromtimestamp(timestamp, datetime.UTC)
    # Format datetime object to ISO 8601 with nanoseconds (compatible with ERA-5 timestamp)
    iso_format = dt.strftime('%Y-%m-%dT%H:%M:%S.%f') + '000'

    return iso_format
    
    
def get_monthly_range(start: np.uint32, end: np.uint32) -> list:
    """
    Generates a list of 'yyyy-mm' values representing each month between the start and end dates (inclusive).

    Args:
        start (np.uint32): The start date
        end (np.uint32): The end date

    Returns:
        list: List of strings in the format 'yyyy-mm' for each month between start and end.
    """
    months = []
    current = start

    while current <= end:
        # Format the current date as 'yyyy-mm'
        months.append(current.strftime('%Y-%m'))
        
        # Move to the next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return months

class ClimateDataStore:
    """
    Load weather data from a server and process it.
    Call WeatherDataFromServer.Close() to release resources after use.
    """
    def __init__(self,short_names: list, start:datetime.datetime,end: datetime.datetime, weather_data_path: str):
        # Validate parameter_names
        if not isinstance(short_names, list) or not all(isinstance(name, str) for name in short_names):
            raise ValueError("short_names should be a list of strings.")
        
        # validate weather_data_path
        if weather_data_path =="":
            raise ValueError("WEATHER_DATA_PATH is not specified.")
        
        for short_name in short_names:
            if short_name not in available_short_names:
                raise ValueError(f"Invalid short name: {short_name}")
            
        self.start = start
        self.end = end
        self.months = get_monthly_range(start, end)

        print(f"months: {self.months}")
        
        self.short_names = short_names
        self.weather_data_path = weather_data_path

        self.weather_ds = self._load_weather_data()
           
    def _load_weather_data(self):
        weather_dataset_instances = []

        # Create a temporary directory for extraction
        tmp_dir = tempfile.mkdtemp()
        zipped_grib_files = []
        
        for month in self.months:
            file_name = f"{self.weather_data_path}/{month}.grib"

            # Unzip the GRIB file
            zip_path = f"{file_name}.zip"
            zipped_grib_files.append(zip_path)

        fast_unzip(zipped_grib_files, tmp_dir) # TODO: optimize to do in the same loop

        for month in self.months:
            grib_file_path = f"{tmp_dir}/{month}.grib"
            
            # Load the weather dataset from the extracted GRIB file
            weather_ds = xr.open_dataset(
                grib_file_path,
                engine="cfgrib",
                backend_kwargs={
                    'filter_by_keys': {
                        'shortName': self.short_names,
                    }
                }
            )

            weather_dataset_instances.append(weather_ds)

        return xr.concat(weather_dataset_instances, dim='time')

    def extract_weather(self, latitude, longitude, time) -> dict:
        """
        Get the value of a weather variable at a specific latitude, longitude, and time.

        Args:
            lat (float): Latitude of the location.
            lon (float): Longitude of the location.
            time (int): Time in yyyy-mm-dd.

        Returns:
            float: Value of the variable at the given location and time.
        """
        # Convert time to iso format
        dt = dt_to_iso8601(time)

        # Select the variable based on the short name -- example short_name '10u' has data_variable 'u10' which corresponds to 10-meter U-component wind velocity
        ds_variables = list(self.weather_ds.data_vars)

        # Initialize an empty dictionary to store values for each variable
        values = {}

        # Loop through each variable (e.g., wind, wave)
        for var in ds_variables:
            # Extract the value at the specified lat, lon, and time for each variable
            values[var] = self.weather_ds[var].sel(latitude=latitude, longitude=longitude, time=dt, method='nearest').values

        return values
        
    def extract_weather_multiple_points(self, latitudes, longitudes, timestamps) -> dict:
        dt = [dt_to_iso8601(t) for t in timestamps]
        
        # Initialize a dictionary to store weather data for each short name (weather variable)
        weather_data_dict = {short_name: [] for short_name in self.short_names}
        
        # Create xarray DataArrays for latitudes, longitudes, and times
        latitudes_da = xr.DataArray(latitudes, dims="points", name="latitude")
        longitudes_da = xr.DataArray(longitudes, dims="points", name="longitude")
        times_da = xr.DataArray(dt, dims="points", name="time")
        
        # Use xarray's multi-dimensional selection to get values for each variable
        for var in self.weather_ds.data_vars:
            # Extract values using xarray's .sel() for multi-dimensional indexing
            # We need to ensure that xarray knows the lat, lon, and time coordinates are aligned
            data = self.weather_ds[var].sel(
                latitude=latitudes_da, 
                longitude=longitudes_da, 
                time=times_da, 
                method="nearest"
            )
            
            # Convert xarray DataArray to numpy array and store it
            # The shape of the data will be (len(latitudes),) if latitudes, longitudes, and times are 1D
            weather_data_dict[var] = data.values  # Extract values as numpy array
        
        return weather_data_dict

    def close(self):
        """
        Close the weather dataset.
        """
        self.weather_ds.close()

available_short_names = {'tcc': True, 'inss': True, 'tciw': True, 'pev': True, 'lgws': True, 'ewss': True, 'mntpr': True, 'wss': True, 
                         'w': True, 'mvtpm': True, 'msnswrfcs': True, 'viiwd': True, 'vst': True, 'cdir': True, 'mwp1': True, 
                         'p1ww': True, 'msmr': True, 'iews': True, 'dwww': True, 'ptype': True, 'e': True, 'mwp2': True, 'mpww': True, 
                         '2dfd': True, 'mttlwrcs': True, '2t': True, 'tsrc': True, 'msdrswrfcs': True, 'vitoee': True, 'tcrw': True, 
                         'clwc': True, 'vithe': True, 'cvl': True, 'aluvp': True, 'istl2': True, '100v': True, 'ssr': True, 
                         'tauoc': True, 'isor': True, 'mx2': True, 'swvl4': True, 'uvb': True, 'sshf': True, 'zust': True, 
                         'msdwuvrf': True, 'pv': True, 'lai_lv': True, 'mdmf': True, 'viozn': True, 'vit': True, 'hcc': True, 
                         'phiaw': True, 'msnlwrf': True, 'anor': True, 'sdor': True, 'sst': True, 'vitoen': True, 'tsr': True, 
                         'vithed': True, 'mpts': True, 'mvimd': True, 'cin': True, 'sro': True, 'str': True, 'sf': True, 'src': True, 
                         'mtdch': True, 'mttpm': True, 'ssrd': True, 'dndzn': True, 'vikee': True, 'mutpm': True, 'stl3': True, 
                         'mtnswrfcs': True, 'mlspr': True, 'viozd': True, 'swh': True, 'mcsr': True, 'tclw': True, 'arrc': True, 
                         'lict': True, 'tplt': True, 'mmtss': True, 'sd': True, 'crwc': True, 'o3': True, 'viiwn': True, 'wdw': True, 
                         'mxtpr': True, 'ishf': True, 'vimat': True, 'ci': True, 'bld': True, 'hmax': True, 't': True, 'vike': True, 
                         'ssrc': True, 'wsk': True, 'slor': True, 'nsss': True, 'mslhf': True, 'mtnswrf': True, 'viwvd': True, 
                         'pres': True, 'smlt': True, 'flsr': True, 'msl': True, 'cswc': True, 'tco3': True, 'etadot': True, 
                         'mumf': True, 'q': True, 'mudr': True, 'ttr': True, 'p2ww': True, 'istl3': True, 'mwd': True, 'msshf': True, 
                         'mdww': True, 'viman': True, 'bfi': True, 'swh3': True, 'msqs': True, 'r': True, 'cvh': True, '10u': True, 
                         'lmlt': True, 'lshf': True, 'vign': True, 'lssfr': True, 'mcpr': True, 'wstar': True, 'alnid': True, 
                         '10fg': True, 'pt': True, 'strd': True, 'ssrdc': True, 'lsf': True, 'msdrswrf': True, 'swvl2': True, 
                         'ttrc': True, 'vitoe': True, 'crr': True, 'cl': True, 'lcc': True, 'viwvn': True, 'vitoed': True, 'swh1': True, 
                         'mp2': True, 'v': True, 'mp1': True, 'mqtpm': True, '100u': True, 'z': True, 'deg0l': True, 'asn': True, 
                         'strc': True, 'vithen': True, 'vilwn': True, 'rhoao': True, 'mtpf': True, 'fsr': True, 'mdts': True, 'sp': True, 
                         'slt': True, 'totalx': True, 'vimae': True, 'tcwv': True, 'mcc': True, 'vipie': True, 'swh2': True, 
                         'slhf': True, 'mx2t': True, 'mddr': True, 'stl1': True, 'lblt': True, 'ie': True, 'msnlwrfcs': True, 
                         'pp1d': True, 'dwi': True, 'awh': True, 'rsn': True, 'vo': True, 'mgwd': True, 'phioc': True, 'mttlwr': True, 
                         'vithee': True, 'mser': True, 'metss': True, 'fdir': True, 'ciwc': True, 'tvl': True, 'lai_hv': True, 
                         'swvl1': True, 'u10n': True, 'msdwlwrf': True, 'fal': True, 'dl': True, 'cp': True, 'mer': True, 'mwp': True, 
                         'wsp': True, 'strdc': True, 'viwve': True, 'viken': True, 'dctb': True, 'vimad': True, '2d': True, 'lmld': True, 
                         'vige': True, 'vigd': True, 'lsm': True, '10v': True, 'p1ps': True, 'mttswrcs': True, 'tmax': True, 'vimd': True, 
                         'd': True, 'mbld': True, 'cc': True, 'sdfor': True, 'csf': True, 'dwps': True, 'mtpr': True, 'chnk': True, 
                         'vilwd': True, 'mn2t': True, 'ust': True, 'mntss': True, 'tsn': True, 'msdwswrfcs': True, 'ssro': True, 
                         'viked': True, 'cape': True, 'mttswr': True, 'ro': True, 'tp': True, 'vilwe': True, 'istl1': True, 'msror': True, 
                         'dndza': True, 'cbh': True, 'ilspf': True, 'vipile': True, 'tcsw': True, 'mssror': True, 'mngwss': True, 
                         'mtnlwrfcs': True, 'mwd1': True, 'msr': True, 'mror': True, 'mwp3': True, 'tplb': True, 'ltlt': True, 
                         'mont': True, 'aluvd': True, 'v10n': True, 'mtnlwrf': True, 'stl2': True, 'mwd2': True, 'tcw': True, 'mper': True, 
                         'blh': True, 'msnswrf': True, 'p2ps': True, 'magss': True, 'msdwswrf': True, 'swvl3': True, 'mlspf': True, 
                         'mwd3': True, 'shts': True, 'msdwlwrfcs': True, 'mlssr': True, 'lsp': True, 'tvh': True, 'lsrr': True, 
                         'stl4': True, 'i10fg': True, 'vioze': True, 'viiwe': True, 'kx': True, 'istl4': True, 'mgws': True, 'cdww': True, 
                         '10si': True, 'tcslw': True, 'viec': True, 'megwss': True, 'tisr': True, 'mtdwswrf': True, 'alnip': True, 
                         'es': True, 'wind': True, 'u': True, 'lspf': True, 'wmb': True, 'acwh': True, 'lnsp': True, 'vima': True, 
                         'licd': True, 'shww': True, 'csfr': True, 'skt': True, 'gwd': True}
