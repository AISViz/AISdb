import h3
import numpy as np
import contextily as cx
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Polygon


class Discretizer:
    def __init__(self, resolution):
        """
        Initialize the H3Indexer with a specified resolution.
        
        :param resolution: H3 hex resolution level (integer)
        """
        self.resolution = resolution

    def get_h3_index(self, lat, lon):
        """
        Get the H3 index for a single latitude and longitude point.
        
        :param lat: Latitude of the point
        :param lon: Longitude of the point
        :return: H3 index as a string
        """
        return h3.latlng_to_cell(lat, lon, self.resolution)
    
    def get_polygon_from_cells(self, cells, tight=True):
        """
        Get the polygon for a list of H3 cells.
        
        :param cells: List of H3 cell indices
        :param tight: If True, return a tightly fitting polygon around the cells, otherwise return the cell boundary
        :return: Polygon as a Shapely geometry object
        """
        return h3.cells_to_h3shape(cells,  tight=tight)

    def yield_tracks_discretized_by_indexes(self, tracks):
        """
        Get H3 indices for a generator of tracks and yield updated tracks.
        
        :param tracks: Generator yielding dictionaries with 'lat' and 'lon' arrays
        :yield: Each track dictionary with an added 'h3_index' list of H3 indices
        """
        for track in tracks:
            longitudes = np.array(track['lon'])
            latitudes = np.array(track['lat'])
            track['h3_index'] = [self.get_h3_index(lat, lon) for lat, lon in zip(latitudes, longitudes)]
            yield track

    def get_hexagon_area_at_latitude(self,lat):
        """ 
        Generate a single hexagon at a specific latitude and calculate its area. 
        """
        hex_boundary = self.get_polygon_from_cells([self.get_h3_index(lat, 0)], tight=False)
        gdf_hex = gpd.GeoDataFrame({'geometry': [hex_boundary]}, crs='EPSG:4326')
        gdf_hex = gdf_hex.to_crs(epsg=32619)  # Convert to UTM for accurate area calculation
        return gdf_hex.geometry.area.iloc[0] / 1000000  # Convert to square kilometers
    
    def describe(self,plot=True):
        """
        Generate and display the relationship between latitude and hexagon area, 
        and resolution and hexagon edge length, with plots and printed output.
        
        Returns
        -------
        None
        """
        latitudes = [-90, -60, -30, 0, 30, 60, 90]
        
        # Calculate the areas of hexagons at different latitudes
        areas = [self.get_hexagon_area_at_latitude(lat) for lat in latitudes]

        if plot:
            # Plot the areas to visualize
            plt.figure(figsize=(10, 5))
            plt.plot(latitudes, areas, marker='o', color='b', linestyle='-', markersize=8, label="Hexagon Area")
            
            # Label and formatting
            plt.ylabel(r'Area $\left(km^2\right)$')
            plt.title(f"Changes in Latitude vs. Area for Resolution: {self.resolution}")
            plt.xlabel(r'Latitude (in degrees)')
            plt.xticks(latitudes)
            plt.xlim(min(latitudes) - 10, max(latitudes) + 10)
            plt.ylim(min(areas) - 10, max(areas) + 10)
            plt.grid(True, which='both', linestyle='--', linewidth=0.5)  # Add gridlines for better clarity
            plt.tight_layout()    # Adjust layout to make sure everything fits
            plt.legend(loc='upper right')  # Add a legend for clarity
            
            # Show the plot
            plt.show()

        print(f"\n[Changes in Latitude vs. Area for Resolution: {self.resolution}]", end="\n")
        for lat, area in zip(latitudes, areas):
            print(f"Latitude {lat} (deg): Hexagon area = {area:.2f} (km2)")

        print("\n[Changes in Resolution vs. Area - [0-15]]", end="\n")
        for h3_resolution in range(0, 16):
            # Calculate the edge length of a hexagon at the given resolution
            edge_length_km = h3.average_hexagon_edge_length(h3_resolution, unit='km')
            print(f"Resolution {h3_resolution} has {edge_length_km:.9f} (km) per edge.")
        
        # Summary of key concepts
        print("\n[Summary of Key Concepts]\n")
        print("- **Variation in Hexagon Areas:** The variation in hexagon areas calculated at different latitudes is primarily due to these projection distortions. Hexagons near the equator (0° latitude) appear larger in area compared to those near the poles. This is a known effect when using certain map projections and area calculations.\n")
        print("- **Resolution Definition:** In the H3 system, the resolution defines the size of the hexagons. A lower resolution number corresponds to larger hexagons, while a higher resolution number corresponds to smaller hexagons.\n")
        print("- **Edge Length Reduction:** As the resolution increases, the edge length of each hexagon decreases. This allows for more detailed spatial analysis, as smaller hexagons can capture finer geographic details.\n")
        print("- **Hierarchical Structure:** Each hexagon at a given resolution is subdivided into smaller hexagons at the next higher resolution. Specifically, each hexagon is divided into approximately seven smaller hexagons, leading to a reduction in edge length by a factor related to the square root of this subdivision.\n")