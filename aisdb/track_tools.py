import numpy as np
import matplotlib.pyplot as plt
from geopy.distance import geodesic
from joblib import Parallel, delayed
from tqdm import tqdm

def _compute_track(track):
    """ Process a single track for evaluation of the results. """
    sog_values = track.get('sog', [])
    cog_values = track.get('cog', [])
    longitudes = track.get('lon', [])
    latitudes = track.get('lat', [])
    times = track.get('time', [])

    cog_values = np.mod(cog_values, 360)
    distance_between_points = []
    speeds_between_points = []
    time_differences = []

    for i in range(len(latitudes) - 1):
        # Extract latitude, longitude, and time for consecutive AIS points
        lat2, lon2, time2 = latitudes[i + 1], longitudes[i + 1], times[i + 1]
        lat1, lon1, time1 = latitudes[i], longitudes[i], times[i]

        # Calculate the distance between points
        point1, point2 = (lat1, lon1), (lat2, lon2)
        distance_meters = geodesic(point1, point2).meters
        distance_between_points.append(distance_meters)

        # Calculate the time difference in seconds
        time_diff_seconds = time2 - time1
        time_differences.append(time_diff_seconds / 60.0)  # in minutes

        # Calculate the time difference in hours for speed calculation
        time_diff_hours = time_diff_seconds / 3600.0

        # Calculate speed in knots (nautical miles per hour)
        distance_nm = distance_meters / 1852.0  # Convert meters to nautical miles
        speed_knots = distance_nm / time_diff_hours if time_diff_hours > 0 else 0  # zero for inertia
        speeds_between_points.append(speed_knots)

    # Calculate positional deltas for COG with circular adjustment
    cog_array = np.array(cog_values, dtype=np.float64)
    cog_diff = np.abs(np.diff(cog_array) % 360)
    cog_diff = np.minimum(cog_diff, 360 - cog_diff)

    return distance_between_points, speeds_between_points, sog_values, cog_values, cog_diff, time_differences


def _visualize_computation(results: list):
    all_speeds = []  # Calculated speeds between consecutive points
    all_distances = []  # Calculated distances between consecutive points
    all_sog_values = []  # Speed Over Ground values from sog key
    all_cog_values = []  # Course Over Ground values from cog key
    all_cog_differences = []  # COG differences between consecutive messages
    all_time_differences = []  # Time differences between consecutive messages

    # Collect all the data from the results
    for distances, speeds, sog_values, cog_values, cog_difference, time_differences in results:
        all_time_differences.extend(time_differences)
        all_cog_differences.extend(cog_difference)
        all_sog_values.extend(sog_values)
        all_cog_values.extend(cog_values)
        all_distances.extend(distances)
        all_speeds.extend(speeds)

    plt.figure(figsize=(12, 24))

    # 0. Histogram of Calculated Distances Between Consecutive Points
    plt.subplot(6, 1, 1)
    plt.hist(all_distances, bins=50, color='yellow', edgecolor='black', alpha=0.7)
    plt.title('Calculated Distances Between Consecutive Points')
    plt.xlabel('Distance (meters)')
    plt.ylabel('Frequency')
    plt.xscale('symlog')
    plt.yscale('log')
    plt.grid(True)
    plt.xlim(0)

    # 1. Histogram of Time Differences Between Consecutive Messages
    plt.subplot(6, 1, 2)
    plt.hist(all_time_differences, bins=50, color='purple', edgecolor='black', alpha=0.7)
    plt.title('Time Differences Between Consecutive Messages')
    plt.xlabel('Delta Time (minutes)')
    plt.ylabel('Frequency')
    plt.xscale('symlog')
    plt.yscale('log')
    plt.grid(True)
    plt.xlim(0)

    # 2. Histogram of Calculated Speeds Between Consecutive Points
    plt.subplot(6, 1, 3)
    plt.hist(all_speeds, bins=50, color='blue', edgecolor='black', alpha=0.7)
    plt.title('Instantly Calculated Speeds Between Points')
    plt.xlabel('Speed (knots)')
    plt.ylabel('Frequency')
    plt.xscale('symlog')
    plt.yscale('log')
    plt.grid(True)
    plt.xlim(0)

    # 3. Histogram of Speed Over Ground (SOG) Values
    plt.subplot(6, 1, 4)
    plt.hist(all_sog_values, bins=50, color='red', edgecolor='black', alpha=0.7)
    plt.title('Speed Over Ground (SOG) Distribution')
    plt.xlabel('Speed (knots)')
    plt.ylabel('Frequency')
    plt.yscale('log')
    plt.grid(True)
    plt.xlim(0)

    # 4. Histogram of Course Over Ground (COG) Values
    plt.subplot(6, 1, 5)
    plt.hist(all_cog_values, bins=50, color='cyan', edgecolor='black', alpha=0.7)
    plt.title('Course Over Ground (COG) Distribution')
    plt.xlabel('COG (degrees)')
    plt.ylabel('Frequency')
    plt.yscale('log')
    plt.grid(True)
    plt.xlim(0)

    # 5. Histogram of Course Over Ground (COG) Values
    plt.subplot(6, 1, 6)
    plt.hist(all_cog_differences, bins=50, color='pink', edgecolor='black', alpha=1.0)
    plt.title('COG Differences Between Consecutive Messages')
    plt.xlabel('Delta COG (degrees)')
    plt.ylabel('Frequency')
    plt.yscale('log')
    plt.grid(True)
    plt.xlim(0)

    plt.tight_layout()
    plt.show()


def TrackCompute(tracks: iter, visualize: bool = False) -> list:
    """
    This function processes a list of AIS tracks and, if `visualize` is True,
    it generates and displays plots for each track's histogram.

    :param tracks: An iterable of AIS tracks.
    :param visualize: If True, generates plots for each track.
    :return: A list of processed track histograms.
    """
    ais_tracks = []  # in-mermory track location
    ais_tracks.extend(tracks)

    results = Parallel(n_jobs=-1)(
        delayed(_compute_track)(track)
        for track in tqdm(ais_tracks)
    )
    if visualize:
        _visualize_computation(results)

    return results