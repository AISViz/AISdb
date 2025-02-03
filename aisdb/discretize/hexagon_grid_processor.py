import h3  #uses v3.7.4
import numpy as np
import networkx as nx
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from shapely.ops import unary_union
from shapely.geometry import Point, Polygon, MultiPolygon

class HexagonGridProcessor:
    def __init__(self, shapefile_path, resolution):
        if not isinstance(shapefile_path, str):
            raise TypeError("shapefile_path must be a string")
        if not isinstance(resolution, int):
            raise TypeError("resolution must be an integer")
        if resolution < 0:  # Or define a reasonable minimum
            raise ValueError("resolution must be a non-negative integer")
        
        self.shapefile_path = shapefile_path
        self.resolution = resolution
        self.gdf_gulf = None
        self.gulf_polygon = None
        self.gdf_hex = None
        self.gdf_fully_within_hex = None
        self.gdf_significant_overlap = None
        self.gdf_largest_cc = None

        self.load_shapefile()

    def load_shapefile(self):
        self.gdf_gulf = gpd.read_file(self.shapefile_path)
        self.gdf_gulf = self.gdf_gulf.to_crs(epsg=4326)
        self.gulf_polygon = unary_union(self.gdf_gulf.geometry)

    def generate_hexagons(self):
        min_x, min_y, max_x, max_y = self.gulf_polygon.bounds
        lat_range = np.arange(min_y, max_y, 0.01)
        lon_range = np.arange(min_x, max_x, 0.01)
        hexagons = set()
        for lat in lat_range:
            for lon in lon_range:
                hex_id = h3.geo_to_h3(lat, lon, self.resolution)
                hexagons.add(hex_id)
        
        hexagon_polygons = [(hex_id, Polygon(h3.h3_to_geo_boundary(hex_id, geo_json=True))) for hex_id in hexagons]
        self.gdf_hex = gpd.GeoDataFrame(hexagon_polygons, columns=['hex_id', 'geometry'], crs='EPSG:4326')

    def filter_fully_within(self):
        self.gdf_hex['is_fully_within'] = self.gdf_hex['geometry'].apply(lambda x: self.gulf_polygon.contains(x))
        self.gdf_fully_within_hex = self.gdf_hex[self.gdf_hex['is_fully_within']].drop(columns='is_fully_within')

    def filter_significant_overlap(self):
        gdf_gulf_utm = self.gdf_gulf.to_crs(epsg=32619)
        self.gdf_hex = self.gdf_hex.to_crs(epsg=32619)
        gulf_polygon_utm = unary_union(gdf_gulf_utm.geometry)
        self.gdf_hex['area_km2'] = self.gdf_hex['geometry'].area / 1e6
        self.gdf_hex['overlap_area_km2'] = self.gdf_hex['geometry'].apply(lambda x: x.intersection(gulf_polygon_utm).area / 1e6)
        self.gdf_hex['percentage_overlap'] = (self.gdf_hex['overlap_area_km2'] / self.gdf_hex['area_km2']) * 100
        self.gdf_significant_overlap = self.gdf_hex[self.gdf_hex['percentage_overlap'] > 70]

    def filter_largest_connected_component(self):
        G = nx.Graph()
        for hex_id, polygon in zip(self.gdf_significant_overlap['hex_id'], self.gdf_significant_overlap['geometry']):
            G.add_node(hex_id, geometry=polygon)
        for hex_id in G.nodes:
            neighbors = h3.hex_ring(hex_id, 1)
            for neighbor in neighbors:
                if neighbor in G.nodes:
                    G.add_edge(hex_id, neighbor)
        largest_cc = max(nx.connected_components(G), key=len)
        largest_cc_hex_ids = list(largest_cc)
        self.gdf_largest_cc = self.gdf_significant_overlap[self.gdf_significant_overlap['hex_id'].isin(largest_cc_hex_ids)]

    def save_outputs(self,output_file_path="."):
        self.gdf_fully_within_hex.to_file(f"{output_file_path}/2_Hexagons.shp", driver='ESRI Shapefile')
        self.gdf_significant_overlap.to_file(f"{output_file_path}/3_Hexagons.shp", driver='ESRI Shapefile')
        self.gdf_largest_cc.to_file(f"{output_file_path}/4_Hexagons.shp", driver='ESRI Shapefile')

    def plot_histogram(self,save_image = False,image_path=None):
        plt.figure(figsize=(6, 4))
        plt.hist(self.gdf_largest_cc.to_crs(epsg=32619)['area_km2'], bins=30, edgecolor='k', alpha=0.7)
        plt.xlabel(r'Area $\left(km^2\right)$')
        plt.ylabel(r'Frequency')
        plt.grid(False)
        plt.ylim(0, 350)
        plt.tight_layout()
        if save_image:
            if image_path is None:
                image_path = "."
            plt.savefig(f"{image_path}/hexagon_final_hist.pdf", format='pdf', bbox_inches='tight')
            plt.savefig(f"{image_path}/hexagon_final_hist.svg", format='svg', bbox_inches='tight')
        plt.show()

    def process(self):
        self.generate_hexagons()
        self.filter_fully_within()
        self.filter_significant_overlap()
        self.filter_largest_connected_component()
        self.save_outputs()

# Usage Example
# processor = HexagonGridProcessor('inputs/gulf.shp', 6)
# processor.process()
