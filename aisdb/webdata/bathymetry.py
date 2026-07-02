"""load bathymetric data from GEBCO raster files"""

import os
import subprocess
import warnings
from functools import reduce

import numpy as np
import py7zr
import requests
from tqdm import tqdm

from aisdb.gis import shiftcoord
from aisdb.webdata.load_raster import RasterFile

# GEBCO 2022 grid split into two archives to fit release asset limits
urls = (
    "https://github.com/AISViz/AISdb/releases/download/data-v1/raster-bathy-1.7z",
    "https://github.com/AISViz/AISdb/releases/download/data-v1/raster-bathy-2.7z",
)


def _filebounds(fpath):
    return {
        f[0]: float(f[1:])
        for f in fpath.split("gebco_2022_", 1)[1].rsplit(".tif", 1)[0].split("_")
    }


def _segment_bounds(raster_keys: np.ndarray) -> np.ndarray:
    """boundaries of runs of equal consecutive raster keys.

    returns an index array `b` such that for each i,
    raster_keys[b[i]:b[i+1]] is a maximal run of one repeated key.
    """
    change_idx = np.where(raster_keys[:-1] != raster_keys[1:])[0] + 1
    return np.concatenate(([0], change_idx, [len(raster_keys)]))


class Gebco:
    def __init__(self, data_dir):
        """
        args:
            data_dir (string)
                folder where rasters should be stored
        """
        self.data_dir = data_dir
        assert os.path.isdir(data_dir)

        # download bathymetry rasters if missing, then index available files
        self.fetch_bathymetry_grid()
        self.rasterfiles = {
            f: _filebounds(f)
            for f in sorted(
                f
                for f in os.listdir(self.data_dir)
                if f.endswith(".tif") and "gebco_2022" in f
            )
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_all()

    def fetch_bathymetry_grid(self):  # pragma: no cover
        """download geotiff zip archive and extract it"""

        if any(
            f.endswith(".tif") and "gebco_2022" in f for f in os.listdir(self.data_dir)
        ):
            return  # bathymetry data already present

        for url in urls:
            zipf = os.path.join(self.data_dir, url.rsplit("/", 1)[-1])
            try:
                if not os.path.isfile(zipf):
                    print("Downloading bathymetric data...")
                    with requests.get(url, stream=True) as payload:
                        assert payload.status_code == 200, "error fetching file"
                        total = int(payload.headers.get("content-length", 0)) or None
                        with open(zipf, "wb") as f:
                            with tqdm(
                                total=total, desc=zipf, unit="B", unit_scale=True
                            ) as t:
                                for chunk in payload.iter_content(chunk_size=8192):
                                    _ = t.update(f.write(chunk))

                print("Extracting bathymetric data...")
                try:
                    subprocess.run(
                        ["7z", "x", zipf, f"-o{self.data_dir}", "-y"], check=True
                    )
                except (FileNotFoundError, subprocess.CalledProcessError):
                    print("System 7z not found, falling back to py7zr (slower)...")
                    with py7zr.SevenZipFile(zipf, mode="r") as zip_ref:
                        zip_ref.extractall(path=self.data_dir)

            except (Exception, KeyboardInterrupt):
                # remove the partial archive so the next attempt starts clean
                if os.path.isfile(zipf):
                    os.remove(zipf)
                raise

            print(f"Removing {zipf} to save space...")
            if os.path.exists(zipf):
                os.remove(zipf)

        return

    def _load_raster(self, key):
        self.rasterfiles[key]["raster"] = RasterFile(
            imgpath=os.path.join(self.data_dir, key)
        )

    def _check_in_bounds(self, track):
        for lon, lat in zip(track["lon"], track["lat"]):
            if not (-180 <= lon <= 180) or not (-90 <= lat <= 90):  # pragma: no cover
                warnings.warn("coordinates out of range!")
                lon = shiftcoord([lon])[0]
                lat = shiftcoord([lat], rng=90)[0]

            if os.environ.get("DEBUG"):
                tracer = False
            for key, bounds in self.rasterfiles.items():
                if (
                    bounds["w"] <= lon <= bounds["e"]
                    and bounds["s"] <= lat <= bounds["n"]
                ):
                    tracer = True
                    if "raster" not in bounds.keys():
                        self._load_raster(key)
                    yield key
                    break
            if os.environ.get("DEBUG") and not tracer:
                print(f"{lon = } {lat = }")
                assert tracer
        return

    def _close_all(self):
        for filepath, bounds in self.rasterfiles.items():
            if "raster" in bounds.keys():
                bounds["raster"].img.close()

    def merge_tracks(self, tracks):
        """append `depth_metres` column to track dictionaries"""
        for track in tracks:
            # mapping of filepaths to the corresponding boundary region
            raster_keys = np.array(list(self._check_in_bounds(track)), dtype=object)

            # ensure that each vector time slice has a value
            if len(raster_keys) != len(track["time"]):
                raise ValueError("no rasters found for track")
            bathy_segments = _segment_bounds(raster_keys)
            track["depth_metres"] = (
                reduce(
                    np.append,
                    [
                        self.rasterfiles[raster_keys[bathy_segments[i]]][
                            "raster"
                        ]._track_coordinate_values(
                            track,
                            rng=range(bathy_segments[i], bathy_segments[i + 1]),
                        )
                        for i in range(len(bathy_segments) - 1)
                    ],
                )
                * -1
            )

            track["dynamic"] = set(track["dynamic"]).union({"depth_metres"})
            yield track
