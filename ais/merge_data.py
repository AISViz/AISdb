from multiprocessing import Queue, Pool

import numpy as np

from common import *
from track_gen import trackgen
from gebco import Gebco
from wsa import wsa
from shore_dist import shore_dist_gfw
#from webdata.marinetraffic import scrape_tonnage


def merge_layers(trackgen):
    ''' generator function to merge AIS row data with shore distance, bathymetry, and geometry databases

        args:
            trackgen: generator function 
                yields track dictionary objects

        yields:
            sets of rows grouped by MMSI sorted by time with additional columns appended

    """ # set start method in main script
        import os; from multiprocessing import set_start_method
        if os.name == 'posix' and __name__ == '__main__': 
            set_start_method('forkserver')
    """
    '''
    from webdata import marinetraffic 

    # read data layers from disk to merge with AIS
    print('aggregating ais, shore distance, bathymetry, vessel geometry...')
    with shore_dist_gfw() as sdist, Gebco() as bathymetry, marinetraffic.scrape_tonnage() as hullgeom:

        #for rows in list(rowgen):
        for track in trackgen:

            # vessel tonnage from marinetraffic.com
            track['deadweight_tonnage'] = hullgeom.get_tonnage_mmsi_imo(track['mmsi'], track['imo'] or 0)

            # wetted surface area - regression on tonnage and ship type
            track['submerged_hull_m^2'] = wsa(track['deadweight_tonnage'], track['ship_type'] or 0)

            # shore distance from cell grid
            track['km_from_shore'] = np.array([sdist.getdist(x, y) for x, y in zip(track['lon'], track['lat']) ])

            # seafloor depth from cell grid
            track['depth_metres'] = np.array([bathymetry.getdepth(x, y) for x,y in zip(track['lon'], track['lat']) ]) * -1

            # seafloor depth from nonnegative border cells
            #track['depth_border_cells_average'] = np.array([bathymetry.getdepth_cellborders_nonnegative_avg(x, y) for x,y in zip(track['lon'], track['lat'])])

            # update indices
            track['static'] = set(track['static']).union(set(['submerged_hull_m^2', 'deadweight_tonnage']))
            #track['dynamic'] = set(track['dynamic']).union(set(['km_from_shore', 'depth_metres', 'depth_border_cells_average']))
            track['dynamic'] = set(track['dynamic']).union(set(['km_from_shore', 'depth_metres']))

            yield track


def queue_generator(q):
    while(True):
        yield q


def queue_iterator(q):
    while not q.empty():
        yield q.get(timeout=10)


def wrapper(q,):
    callback(merge_layers(queue_iterator(q)))

            
def merge_layers_parallel(rowgen, callback, processes=8):
    '''
    rowgen yields a set of rows for each MMSI, sorted by time

    example callback:

        ```
        from functools import partial

        from ais.track_gen import segment_tracks_timesplits
        from ais.clustering import segment_tracks_dbscan
        from ais.network_graph import concat_tracks_no_movement

        timesplit = partial(segment_tracks_timesplits,  maxdelta=timedelta(hours=2))
        distsplit = partial(segment_tracks_dbscan,      max_cluster_dist_km=50)
        concat    = partial(concat_tracks_no_movement,  domain=domain)

        callback = lambda rowgen: concat(distsplit(timesplit(merged)))
        ```
    '''
    q = Queue()

    with Pool(processes=processes) as p:
        #p.imap_unordered(callback, queue_generator(q), chunksize=1)
        p.map(wrapper, queue_generator(q), chunksize=1)
        for rowset in rowgen:
            while q.size() > processes:
                time.sleep(0.1) 
            q.put(rowset, block=True, timeout=10)

        p.close()
        p.join()

