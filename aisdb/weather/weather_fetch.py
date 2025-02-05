import cdsapi
import os

class FetchClimateData:
    AVAILABLE_PARAMETERS = [
        "variable","day","time","month","temperature_and_pressure", "wind", "mean_rates", 
        "radiation_and_heat", "clouds", "year", "area",
        "lakes", "evaporation_and_runoff", "precipitation_and_rain", "snow", "soil",
        "vertical_integrals", "vegetation", "ocean_waves", "other"
    ]

    def __init__(self, dataset: str = None, request: dict = None, output_path: str = None):
        """
        Initialize the FetchClimateData class.
        
        :param dataset: The dataset name (e.g., "reanalysis-era5-single-levels").
        :param request: A dictionary containing request parameters.
        :param output_path: Path where the downloaded file should be saved.
        """
        if dataset is None:
            raise ValueError("Dataset must be provided.")
        if request is None or not isinstance(request, dict):
            raise ValueError("Request parameters must be a non-empty dictionary.")
        
        required_keys = {"variable", "year", "month", "day", "time"}
        if not required_keys.issubset(request.keys()):
            raise ValueError(f"Request must contain the following keys: {required_keys}")
        
        if "product_type" in request and request["product_type"] != "reanalysis":
            raise ValueError("Product type must always be 'reanalysis'.")
        request["product_type"] = "reanalysis"
        
        if "format" in request and request["format"] != "grib":
            raise ValueError("Data format must always be 'grib'.")
        request["format"] = "grib"
        
        if "download_format" in request and request["download_format"] != "unarchived":
            raise ValueError("Download format must always be 'unarchived'.")
        request["download_format"] = "unarchived"
        
        if "variable" in request and request["variable"] not in self.AVAILABLE_PARAMETERS:
            raise ValueError(f"Invalid variable. Choose from {self.AVAILABLE_PARAMETERS}")
        
        # Validate area coordinates (North, West, South, East)
        area = request.get("area", [])
        if len(area) != 4 or not (
            -90 <= area[0] <= 90 and -180 <= area[1] <= 180 and -90 <= area[2] <= 90 and -180 <= area[3] <= 180
        ):
            raise ValueError("Invalid geographical area. Format: [North, West, South, East] within valid lat/lon bounds.")
        
        year = int(request["year"])
        if year < 1940 or year > 2025:
            raise ValueError("Year must be between 1940 and 20205.")
        
        day = int(request["day"])
        if day < 1 or day > 31:
            raise ValueError("Day must be between 01 and 31.")
        
        for t in request["time"]:
            if not ("00:00" <= t <= "23:59"):
                raise ValueError("Time must be in 24-hour format (HH:MM).")
        
        self.dataset = dataset
        self.request = request
        self.output_path = output_path if output_path else "data.grib"
    
    def fetch_data(self):
        """
        Fetch climate data from the Copernicus Climate Data Store and save it to the specified location.
        """
        try:
            client = cdsapi.Client()
            result = client.retrieve(self.dataset, self.request)
            result.download(self.output_path)
            print(f"Data successfully downloaded to {self.output_path}")
        except Exception as e:
            print(f"Error while fetching data: {e}")