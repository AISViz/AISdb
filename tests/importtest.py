import os
from functools import partial 
from datetime import timedelta

#from ais import *
from ais.gis import Domain, ZoneGeomFromTxt
from ais.track_gen import trackgen, segment_tracks_timesplits, segment_tracks_dbscan, fence_tracks, concat_tracks
from ais.network_graph import serialize_network_edge
from ais.merge_data import merge_tracks_hullgeom, merge_tracks_shoredist, merge_tracks_bathymetry


shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
domain = Domain('east', zonegeoms)

timesplit = partial(segment_tracks_timesplits,  maxdelta=timedelta(hours=2))
distsplit = partial(segment_tracks_dbscan,      max_cluster_dist_km=50)
geofenced = partial(fence_tracks,               domain=domain)
serialize = partial(serialize_network_edge,     domain=domain)


def run_parallel(piped):
    yield serialize(merge_tracks_bathymetry(merge_tracks_shoredist(merge_tracks_hullgeom(geofenced(distsplit(timesplit(trackgen([piped]))))))))

