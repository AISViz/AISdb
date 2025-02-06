import cdsapi
import os
from datetime import datetime, timedelta


class FetchClimateData:
    AVAILABLE_PARAMETERS = [
        "variable","day","time","month","temperature_and_pressure", "wind", "mean_rates", 
        "radiation_and_heat", "clouds", "year", "area",
        "lakes", "evaporation_and_runoff", "precipitation_and_rain", "snow", "soil",
        "vertical_integrals", "vegetation", "ocean_waves", "other"
    ]
    MONTH_MAPPING = {
        "January": "01", "February": "02", "March": "03", "April": "04", "May": "05", "June": "06",
        "July": "07", "August": "08", "September": "09", "October": "10", "November": "11", "December": "12"
    }

    def __init__(self, dataset: str = None, params_requested: dict = None, output_path: str = None):
        """
        Initialize the FetchClimateData class.
        
        :param dataset: The dataset name 
        :param params_requested: A dictionary containing request parameters.
        :param output_path: Path where the downloaded file should be saved.
        """
        if dataset is None:
            raise ValueError("Dataset must be provided.")
        if params_requested is None or not isinstance(params_requested, dict):
            raise ValueError("Request parameters must be a non-empty dictionary.")
        
        required_keys = {"variable", "start_time", "end_time", "area"}
        if not required_keys.issubset(params_requested.keys()):
            raise ValueError(f"Request must contain the following keys: {required_keys}")
        
        if "product_type" in params_requested and params_requested["product_type"] != "reanalysis":
            raise ValueError("Product type must always be 'reanalysis'.")
        params_requested["product_type"] = "reanalysis"
        
        if "format" in params_requested and params_requested["format"] != "grib":
            raise ValueError("Data format must always be 'grib'.")
        params_requested["format"] = "grib"
        
        if "download_format" in params_requested and params_requested["download_format"] != "unarchived":
            raise ValueError("Download format must always be 'unarchived'.")
        params_requested["download_format"] = "unarchived"
        
        for param in params_requested.keys():
            if param not in self.AVAILABLE_PARAMETERS and param not in {"start_time", "end_time", "area", "product_type", "format", "download_format"}:
                raise ValueError(f"Invalid parameter '{param}'. Choose from {self.AVAILABLE_PARAMETERS}")
        
        # Validate area coordinates (North, West, South, East)
        area = params_requested.get("area", [])
        if len(area) != 4 or not (
            -90 <= area[0] <= 90 and -180 <= area[1] <= 180 and -90 <= area[2] <= 90 and -180 <= area[3] <= 180
        ):
            raise ValueError("Invalid geographical area. Format: [North, West, South, East] within valid lat/lon bounds.")
        
        try:
            start_time = datetime.strptime(request["start_time"], "%Y-%m-%d %H:%M")
            end_time = datetime.strptime(request["end_time"], "%Y-%m-%d %H:%M")
        except ValueError:
            raise ValueError("Time must be in format 'YYYY-MM-DD HH:MM'.")
        
        if start_time >= end_time:
            raise ValueError("Start time must be earlier than end time.")
        
        if start_time.year < 1940 or end_time.year > 20205:
            raise ValueError("Year must be between 1940 and 20205.")
        
        # Generate time intervals
        time_intervals = []
        current_time = start_time
        while current_time <= end_time:
            time_intervals.append(current_time.strftime("%H:%M"))
            current_time += timedelta(hours=1)
        
        params_requested["year"] = [str(start_time.year)]
        month_name = start_time.strftime("%B").lower()
        params_requested["month"] = [self.MONTH_MAPPING[month_name]]
        params_requested["day"] = [str(start_time.day).zfill(2)]
        params_requested["time"] = time_intervals

        self.dataset = dataset
        self.params_requested = params_requested
        self.output_path = output_path if output_path else "data.grib"
        try:
            self.client = cdsapi.Client()
            print("API Key found")
        except Exception as e:
            print(f"Error while fetching API Key: {e}")
    
    def fetch_data(self):
        """
        Fetch climate data from the Copernicus Climate Data Store and save it to the specified location.
        """
        
        try:
            result = self.client.retrieve(self.dataset, self.params_requested)
            result.download(self.output_path)
            print(f"Data successfully downloaded to {self.output_path}")
        except Exception as e:
            print(f"Error while fetching data: {e}")

    