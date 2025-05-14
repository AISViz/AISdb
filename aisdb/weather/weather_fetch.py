import cdsapi
import os
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from aisdb.weather.utils import SHORT_NAMES_TO_VARIABLES
import logging

DEFAULT_PARAMS = {
    "product_type": ["reanalysis"],
    "data_format": "grib",
    "download_format": "unarchived",
}

class ClimateDataStore:
    def __init__(self, dataset: str = None, short_names: list = None, start_time: datetime = None, end_time: datetime = None, area: list = None):
        """
        Initialize the ClimateDataStore class.

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
        
        xmin, xmax, ymin, ymax = area 
        if not (-90 <= ymin <= 90 and -90 <= ymax <= 90 and -180 <= xmin <= 180 and -180 <= xmax <= 180):
            raise ValueError("Invalid geographical bounds. Longitude: [-180, 180], Latitude: [-90, 90]")

        self.params_requested["area"] = [ymax, xmin, ymin, xmax]
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
        
        self.logger = logging.getLogger("ClimateDataStore")


    def download_grib_file(self, output_folder):
        """
        Fetch climate data from the Copernicus Climate Data Store and save it to separate monthly files.
        """
        try:
            start_date = datetime.strptime(
                f"{self.params_requested['year'][0]}-{self.params_requested['month'][0]}-{self.params_requested['day'][0]}",
                "%Y-%m-%d"
            )
            end_date = datetime.strptime(
                f"{self.params_requested['year'][-1]}-{self.params_requested['month'][-1]}-{self.params_requested['day'][-1]}",
                "%Y-%m-%d"
            )

            current_date = start_date

            while current_date <= end_date:
                month_str = current_date.strftime("%Y-%m")

                month_start = current_date.replace(day=1)
                last_day = calendar.monthrange(month_start.year, month_start.month)[1]
                self.logger.info(f"Processing {month_start.year}-{month_start.month}: Last day is {last_day}")

                month_end = datetime(month_start.year, month_start.month, last_day)
                if month_end > end_date:
                    month_end = end_date

                days_list = [str(i).zfill(2) for i in range(1, (month_end.day if month_end.month == end_date.month and month_end.year == end_date.year else last_day) + 1)]

                original_params = self.params_requested.copy()

                self.params_requested["year"] = [month_start.strftime("%Y")]
                self.params_requested["month"] = [month_start.strftime("%m")]
                self.params_requested["day"] = days_list

                self.logger.info(f"Final request parameters: {self.params_requested}")

                try:
                    result = self.client.retrieve(self.dataset, self.params_requested)
                    file_name = f"{month_str}.grib"
                    current_directory = os.getcwd()
                    new_output_folder = os.path.join(current_directory, output_folder)
                    if not os.path.exists(new_output_folder):
                        os.makedirs(new_output_folder)
                    output_path = os.path.join(new_output_folder, file_name)
                    result.download(output_path)
                    print(f"Data for {month_str} successfully downloaded to {output_path}")
                except Exception as e:
                    print(f"Error while fetching weather data: {e}")

                self.params_requested = original_params

                current_date = month_start + relativedelta(months=1)

        except Exception as e:
            self.logger.error(f"Error while fetching weather data: {e}")

    def _get_variable_for_shortName(self, short_names: list) -> list:
        variables = []
        for short_name in short_names:
                value =  SHORT_NAMES_TO_VARIABLES[short_name]
                if value == "":
                    raise ValueError(f"Invalid shortName: {short_name}.")
                
                variables.append(value)
        return variables