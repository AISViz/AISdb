import cdsapi
from datetime import datetime, timedelta
from aisdb.weather.utils import SHORT_NAMES_TO_VARIABLES

DEFAULT_PARAMS = {
    "product_type": ["reanalysis"],
    "data_format": "grib",
    "download_format": "unarchived",
}

class ClimateDataStore:
    def __init__(self, dataset: str = None, short_names: list = None, start_time: datetime = None, end_time: datetime = None, area: list = None):
        """
        Initialize the FetchClimateData class.

        :param dataset: The dataset name
        :param params_requested: A dictionary containing request parameters.
        """
        if dataset is None:
            raise ValueError("Dataset must be provided.")

        if isinstance(start_time, str):
            start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M")
        if isinstance(end_time, str):
            end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M")
        if start_time >= end_time:
            raise ValueError("Start time must be earlier than end time.")

        self.params_requested = {**DEFAULT_PARAMS}
        self.dataset = dataset
        
        area = self.params_requested.get("area", [])
        if len(area) != 4 or not (
            -90 <= area[0] <= 90 and -180 <= area[1] <= 180 and -90 <= area[2] <= 90 and -180 <= area[3] <= 180
        ):
            raise ValueError("Invalid geographical area. Format: [North, West, South, East] within valid lat/lon bounds.")
 
        self.params_requested["variable"] = self._get_variable_for_shortName(short_names)

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
            time_intervals = [f"{hour:02d}:00" for hour in range(24)]

        self.params_requested["time"] = time_intervals
        self.params_requested.pop("start_time", None)
        self.params_requested.pop("end_time", None)

        try:
            self.client = cdsapi.Client()
        except Exception as e:
            print(f"Error while establishing connection with cdsapi: {e}")

    def download_grib_file(self, output_path = str):
        """
        Fetch climate data from the Copernicus Climate Data Store and save it to the specified location.
        """
        try:
            result = self.client.retrieve(self.dataset, self.params_requested)
            result.download(output_path)
            print(f"Data successfully downloaded to {output_path}")
        except Exception as e:
            print(f"Error while fetching weather data: {e}")

    def _get_variable_for_shortName(self, short_names: list) -> list:
        variables = []
        for short_name in short_names:
                value =  SHORT_NAMES_TO_VARIABLES[short_name]
                if value == "":
                    raise ValueError(f"Invalid shortName: {short_name}.")
                
                variables.append(value)
        return variables