import cdsapi
import os
from datetime import datetime, timedelta


class FetchClimateData:
    AVAILABLE_PARAMETERS = [
        "variable","day","start_time","end_time","month","temperature_and_pressure", "wind", "mean_rates", 
        "radiation_and_heat", "clouds", "year", "area",
        "lakes", "evaporation_and_runoff", "precipitation_and_rain", "snow", "soil",
        "vertical_integrals", "vegetation", "ocean_waves", "other"
    ]

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
        
        params_requested["product_type"] = "reanalysis"
        params_requested["format"] = "grib"
        params_requested["download_format"] = "unarchived"
        
        for param in params_requested.keys():
            if param not in self.AVAILABLE_PARAMETERS and param not in {"start_time", "end_time", "area", "product_type", "format", "download_format"}:
                raise ValueError(f"Invalid parameter '{param}'. Choose from {self.AVAILABLE_PARAMETERS}")
        
        area = params_requested.get("area", [])
        if len(area) != 4 or not (
            -90 <= area[0] <= 90 and -180 <= area[1] <= 180 and -90 <= area[2] <= 90 and -180 <= area[3] <= 180
        ):
            raise ValueError("Invalid geographical area. Format: [North, West, South, East] within valid lat/lon bounds.")
        
        try:
            start_time = params_requested["start_time"]
            end_time = params_requested["end_time"]
        except ValueError:
            raise ValueError("Time must be in format 'YYYY-MM-DD HH:MM'.")
        
        if start_time >= end_time:
            raise ValueError("Start time must be earlier than end time.")
        
        if start_time.year < 1940 or end_time.year > 2025:
            raise ValueError("Year must be between 1940 and 2025.")
        
        # Convert variable to correct format
        params_requested["variable"] = [params_requested["variable"].replace(" ", "_")]
        
        # Generate list of years, months, and days
        current_date = start_time
        years, months, days = set(), set(), set()
        while current_date <= end_time:
            years.add(str(current_date.year))
            months.add(str(current_date.month).zfill(2))
            days.add(str(current_date.day).zfill(2))
            current_date += timedelta(days=1)
        
        params_requested["year"] = sorted(list(years))
        params_requested["month"] = sorted(list(months))
        params_requested["day"] = sorted(list(days))
        
        # Generate list of hourly timestamps
        time_intervals = []
        current_time = start_time
        while current_time <= end_time:
            time_intervals.append(current_time.strftime("%H:%M"))
            current_time += timedelta(hours=1)
        
        params_requested["time"] = time_intervals

        self.dataset = dataset
        self.params_requested = params_requested
        self.output_path = output_path if output_path else "data.grib"
        try:
            self.client = cdsapi.Client()
            print("API Key found")
        except Exception as e:
            print(f"Error while fetching API Key: {e}")
        print(f"Requested data API format: {params_requested}")    
    
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

    