import h3
import numpy as np
import contextily as cx
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.ops import unary_union
import networkx as nx
import os
import time
import cuspatial
from tqdm import tqdm
from shapely.prepared import prep
import pandas as pd


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

    def plot_discretized_tracks(self, tracks, shapefile_path=None, ax=None):
        """
        Plot the hexagonal cells for each track ensuring compliance with the given shapefile.
        """
        # Collect all unique H3 cells from the track
        cells = set()
        for track in tracks:
            cells.update(track['h3_index'])  # Collect all unique cells from the track 

        if len(cells) == 0:
            raise ValueError("No value h3_index found in the tracks.")

        # If a shapefile path is given, ensure hexagons comply with the shapefile
        if shapefile_path:
            # Load the shapefile containing valid hexagons (that we need to comply with)
            valid_hexagons = gpd.read_file(shapefile_path)
            
            # Filter cells based on whether they are within the valid hexagons in the shapefile
            # Convert the H3 cells to geometries and filter based on intersection with the shapefile polygons
            valid_cells = set()
            for cell in cells:
                h3_geom = h3.cell_to_boundary(cell)  # Convert H3 cell to polygon
                h3_polygon = Polygon([(lat, lon) for lon, lat in h3_geom])
                if valid_hexagons.geometry.intersects(h3_polygon).any():  # Check if the cell intersects any valid hexagon
                    valid_cells.add(cell)
            cells = valid_cells

            if len(cells) == 0:
                raise ValueError("No valid H3 cells found within the shapefile.")
        
        _plot_cells(cells, ax=ax)

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


    def generate_filtered_hexagons_from_shapefile(self, shapefile_path, output_path_and_file_name="1_Hexagons.shp", to_device='cpu', batch_size=1000):
        start_time = time.time()  # Start overall timer
        print("Starting the hexagon generation process. There are 20 steps in total...")

        # Step 1: Read the shapefile and ensure it's in EPSG:4326
        print("Step 1: Reading and reprojecting shapefile...")
        step_start = time.time()
        gdf = gpd.read_file(shapefile_path).to_crs(epsg=4326)
        print(f"Time for reading and reprojecting shapefile: {time.time() - step_start:.2f} seconds")

        # Step 2: Filter for valid polygon geometries only
        print("Step 2: Filtering valid polygon geometries...")
        step_start = time.time()
        gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
        
        if not gdf.is_valid.all():
            gdf = gdf[gdf.is_valid]
            print("Warning: Some invalid geometries were found and removed.")
        
        if gdf.empty:
            raise ValueError("No valid polygon geometries found in the shapefile.")
        print(f"Time for filtering valid geometries: {time.time() - step_start:.2f} seconds")

        # Step 3: Merge all polygons into a single polygon
        print("Step 3: Merging polygons into a single polygon...")
        step_start = time.time()
        polygon = unary_union(gdf.geometry)
        print(f"Time for merging polygons: {time.time() - step_start:.2f} seconds")

        # Step 4: Generate hexagon cells within the bounds of the polygon
        print("Step 4: Generating hexagon cells within the bounds of the polygon...")
        step_start = time.time()
        cells = self._generate_cells_within_bounds(polygon)
        print(f"Time for generating hexagon cells: {time.time() - step_start:.2f} seconds")

        # Step 5: Generate hexagon polygons
        print("Step 5: Generating hexagon polygons...")
        step_start = time.time()
        hexagon_polygons = [
            (cell, Polygon([(lat, lon) for lon, lat in h3.cell_to_boundary(cell)])) for cell in cells
        ]
        print(f"Time for generating hexagon polygons: {time.time() - step_start:.2f} seconds")

        # Step 6: Create GeoDataFrame for hexagons
        print("Step 6: Creating GeoDataFrame for hexagons...")
        step_start = time.time()
        gdf_hex = gpd.GeoDataFrame(hexagon_polygons, columns=['hex_id', 'geometry'], crs='EPSG:4326')
        print(f"Time for creating GeoDataFrame: {time.time() - step_start:.2f} seconds")

        # Step 7: Filter hexagons that intersect with the polygon using GPU with progress bar
        print(f"Step 7: Filtering hexagons that intersect with the polygon using {to_device}...")
        step_start = time.time()
        total = len(gdf_hex)
        mask_list = []

        if to_device not in ['CPU', 'CUDA']:
            raise ValueError("Unsupported device type. Choose 'CPU' or 'CUDA'.")

        # Define the common progress bar
        with tqdm(total=total, desc="Filtering hexagons", unit="hexagon") as pbar:
            for start_idx in range(0, total, batch_size):
                end_idx = min(start_idx + batch_size, total)
                batch = gdf_hex.iloc[start_idx:end_idx]

                if to_device == 'cpu':
                    # Vectorized intersection test for CPU
                    mask_batch = batch.intersects(polygon)
                    mask_list.append(mask_batch)
                
                elif to_device == 'cuda':
                    # GPU processing using cuspatial for CUDA
                    batch_hex_geo = cuspatial.GeoSeries(batch.geometry)
                    polygons1_batch = cuspatial.GeoSeries([polygon] * len(batch))
                    distances = cuspatial.pairwise_polygon_distance(polygons1_batch, batch_hex_geo)
                    intersecting_mask_batch = (distances == 0).to_pandas()
                    mask_list.append(intersecting_mask_batch)

                pbar.update(len(batch))

        # Combine the boolean masks from all batches
        mask_full = pd.concat(mask_list)
        gdf_water_hex = gdf_hex[mask_full]

        print(f"Time for filtering intersecting hexagons: {time.time() - step_start:.2f} seconds")


        # Step 8: Project the data to UTM once for efficiency
        print("Step 8: Projecting to UTM...")
        step_start = time.time()
        gdf_water_hex_utm = gdf_water_hex.to_crs(epsg=32619)
        gdf_water_hex_utm['area_km2'] = gdf_water_hex_utm.geometry.area / 1000000
        print(f"Time for projecting to UTM: {time.time() - step_start:.2f} seconds")

        # Step 9: Filter hexagons that are fully within the polygon using prepared geometry
        print("Step 9: Filtering hexagons fully within the polygon using prepared geometry...")
        step_start = time.time()
        prepared_polygon = prep(polygon)
        mask = gdf_hex.geometry.apply(lambda x: prepared_polygon.contains(x))
        gdf_fully_within_hex = gdf_hex[mask]
        print(f"Time for filtering fully contained hexagons: {time.time() - step_start:.2f} seconds")

        # Step 10: Project fully contained hexagons into UTM and calculate area
        print("Step 10: Projecting fully contained hexagons and calculating area...")
        step_start = time.time()
        gdf_fully_within_hex_utm = gdf_fully_within_hex.to_crs(epsg=32619)
        gdf_fully_within_hex_utm['area_km2'] = gdf_fully_within_hex_utm.geometry.area / 1000000
        print(f"Time for projecting and calculating area: {time.time() - step_start:.2f} seconds")

        # Step 11: Create a unified polygon for the Gulf in UTM
        print("Step 11: Creating a unified Gulf polygon in UTM...")
        step_start = time.time()
        gulf_polygon_utm = unary_union(gdf.to_crs(epsg=32619).geometry)
        print(f"Time for creating unified Gulf polygon: {time.time() - step_start:.2f} seconds")

        # Step 12: Calculate overlap area in UTM using spatial join for efficiency
        print("Step 12: Calculating overlap area...")
        step_start = time.time()
        gdf_water_hex_utm['overlap_area_km2'] = gdf_water_hex_utm.geometry.apply(
            lambda x: x.intersection(gulf_polygon_utm).area / 1e6 if not x.is_empty else 0
        )
        print(f"Time for calculating overlap area: {time.time() - step_start:.2f} seconds")

        # Step 13: Calculate percentage overlap
        print("Step 13: Calculating percentage overlap...")
        step_start = time.time()
        gdf_water_hex_utm['percentage_overlap'] = (gdf_water_hex_utm['overlap_area_km2'] / gdf_water_hex_utm['area_km2']) * 100
        print(f"Time for calculating percentage overlap: {time.time() - step_start:.2f} seconds")

        # Step 14: Filter hexagons with significant overlap (more than 70%)
        print("Step 14: Filtering hexagons with significant overlap...")
        step_start = time.time()
        gdf_significant_overlap = gdf_water_hex_utm[gdf_water_hex_utm['percentage_overlap'] > 70]
        print(f"Time for filtering significant overlap hexagons: {time.time() - step_start:.2f} seconds")

        # Step 15: Build the graph (only for hexagons with significant overlap)
        print("Step 15: Building graph for significant overlap hexagons...")
        step_start = time.time()
        G = nx.Graph()

        for hex_id, polygon in zip(gdf_significant_overlap['hex_id'], gdf_significant_overlap['geometry']):
            G.add_node(hex_id, geometry=polygon)

        for hex_id in G.nodes:
            neighbors = h3.grid_ring(hex_id, 1)
            G.add_edges_from((hex_id, neighbor) for neighbor in neighbors if neighbor in G.nodes)
        print(f"Time for building graph: {time.time() - step_start:.2f} seconds")

        # Step 16: Find the largest connected component in the graph
        print("Step 16: Finding largest connected component...")
        step_start = time.time()
        largest_cc = max(nx.connected_components(G), key=len)
        largest_cc_hex_ids = list(largest_cc)
        print(f"Time for finding largest connected component: {time.time() - step_start:.2f} seconds")

        # Step 17: Filter the GeoDataFrame to keep only the largest connected component
        print("Step 17: Filtering GeoDataFrame for the largest connected component...")
        step_start = time.time()
        gdf_largest_cc = gdf_significant_overlap[gdf_significant_overlap['hex_id'].isin(largest_cc_hex_ids)]
        print(f"Time for filtering largest connected component: {time.time() - step_start:.2f} seconds")

        # Step 18: Reproject to EPSG:4326 for consistency
        print("Step 18: Reprojecting to EPSG:4326...")
        step_start = time.time()
        gdf_largest_cc = gdf_largest_cc.to_crs(epsg=4326)
        print(f"Time for reprojecting to EPSG:4326: {time.time() - step_start:.2f} seconds")

        # Step 19: Ensure the output directory exists
        print("Step 19: Ensuring output directory exists...")
        step_start = time.time()
        output_dir = os.path.dirname(output_path_and_file_name)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        print(f"Time for ensuring output directory: {time.time() - step_start:.2f} seconds")

        # Step 20: Save the filtered GeoDataFrame as a shapefile
        print("Step 20: Saving the filtered GeoDataFrame as shapefile...")
        step_start = time.time()
        gdf_largest_cc.to_file(output_path_and_file_name, driver='ESRI Shapefile')
        print(f"Time for saving shapefile: {time.time() - step_start:.2f} seconds")

        # Overall time
        print(f"Total time for hexagon generation: {time.time() - start_time:.2f} seconds")
        
        return gdf_largest_cc
        
    
    def _generate_cells_within_bounds(self, polygon, deg=0.01):
        """ 
        Generate hexagons covering the bounding box. 
        """
        min_x, min_y, max_x, max_y = polygon.bounds
        lat_range = np.arange(min_y, max_y, deg)
        lon_range = np.arange(min_x, max_x, deg)

        cells = set()
        for lat in lat_range:
            for lon in lon_range:
                hex_id = self.get_h3_index(lat, lon)
                cells.add(hex_id)

        return cells

def _plot_df(df, column=None, ax=None):
    "Plot based on the `geometry` column of a GeoPandas dataframe"
    df = df.copy()
    df = df.to_crs(epsg=3857)  # web mercator

    if ax is None:
        _, ax = plt.subplots(figsize=(8,8))
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)

    df.plot(
        ax=ax,
        alpha=0.5, edgecolor='k',
        column=column, categorical=True,
        legend=True, legend_kwds={'loc': 'upper left'},
    )
    cx.add_basemap(ax, crs=df.crs, source=cx.providers.CartoDB.Positron)


def _plot_shape(shape, ax=None):
    df = gpd.GeoDataFrame({'geometry': [shape]}, crs='EPSG:4326')
    _plot_df(df, ax=ax)


def _plot_cells(cells, ax=None):
    shape = h3.cells_to_h3shape(cells)
    _plot_shape(shape, ax=ax)


def _plot_shape_and_cells(shape, res=9):
    fig, axs = plt.subplots(1,2, figsize=(10,5), sharex=True, sharey=True)
    _plot_shape(shape, ax=axs[0])
    _plot_cells(h3.h3shape_to_cells(shape, res), ax=axs[1])
    fig.tight_layout()