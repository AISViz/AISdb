import cdsapi
from datetime import datetime, timedelta

DEFAULT_PARAMS = {
    "product_type": ["reanalysis"],
    "data_format": "grib",
    "download_format": "unarchived",
    "variable": "",
    "area": [],
}

AVAILABLE_PARAMETERS = {
    "variable": True,
    "start_time":True,
    "end_time":True,
    "day": True,
    "month": True,
    "temperature_and_pressure": True,
    "wind": True,
    "mean_rates": True,
    "radiation_and_heat": True,
    "clouds": True,
    "year": True,
    "area": True,
    "lakes": True,
    "evaporation_and_runoff": True,
    "precipitation_and_rain": True,
    "snow": True,
    "soil": True,
    "vertical_integrals": True,
    "vegetation": True,
    "ocean_waves": True,
    "other": True,
}


class FetchClimateData:
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

        if "start_time" not in params_requested or "end_time" not in params_requested:
            raise ValueError("Both 'start_time' and 'end_time' must be provided in params_requested.")

        start_time = params_requested["start_time"]
        end_time = params_requested["end_time"]

        if isinstance(start_time, str):
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M")
        if isinstance(end_time, str):
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M")

        if start_time >= end_time:
            raise ValueError("Start time must be earlier than end time.")

        self.params_requested = {**DEFAULT_PARAMS, **params_requested}

        # Validate parameters
        for param in self.params_requested.keys():
            if param not in AVAILABLE_PARAMETERS and param not in DEFAULT_PARAMS:
                raise ValueError(f"Invalid parameter '{param}'. Choose from {list(AVAILABLE_PARAMETERS.keys())}")

        area = self.params_requested.get("area", [])
        if len(area) != 4 or not (
            -90 <= area[0] <= 90 and -180 <= area[1] <= 180 and -90 <= area[2] <= 90 and -180 <= area[3] <= 180
        ):
            raise ValueError("Invalid geographical area. Format: [North, West, South, East] within valid lat/lon bounds.")

        if isinstance(self.params_requested["variable"], list):
            self.params_requested["variable"] = " ".join(self.params_requested["variable"])

        self.params_requested["variable"] = [
            self.params_requested["variable"].replace(" ", "_").replace("-", "_")
        ]


        current_date = start_time
        years, months, days = set(), set(), set()

        while current_date <= end_time:
            years.add(str(current_date.year))
            months.add(str(current_date.month).zfill(2))
            days.add(str(current_date.day).zfill(2))
            current_date += timedelta(days=1)

        self.params_requested["year"] = sorted(list(years))
        self.params_requested["month"] = sorted(list(months))
        self.params_requested["day"] = sorted(list(days))

        time_intervals = []

        if start_time.date() == end_time.date():
            # If within the same day, only include relevant hours
            current_time = start_time
            while current_time <= end_time:
                time_intervals.append(current_time.strftime("%H:%M"))
                current_time += timedelta(hours=1)
        else:
            # If spanning multiple days, include all 24 hours
            time_intervals = [f"{hour:02d}:00" for hour in range(24)]

        self.params_requested["time"] = time_intervals

        # Now remove start_time and end_time from params_requested
        self.params_requested.pop("start_time", None)
        self.params_requested.pop("end_time", None)

        self.dataset = dataset
        self.output_path = output_path if output_path else "data.grib"

        try:
            self.client = cdsapi.Client()
            print("API Key found")
        except Exception as e:
            print(f"Error while fetching API Key: {e}")

        print(f"Requested data API format: {self.params_requested}")

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
