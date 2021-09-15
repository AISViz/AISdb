import numpy as np

from common import *
from track_gen import trackgen
from gebco import Gebco
from wsa import wsa
from shore_dist import shore_dist_gfw
from webdata import marinetraffic 
#from webdata.marinetraffic import scrape_tonnage


def merge_layers(rowgen):
    ''' generator function to merge AIS row data with shore distance, bathymetry, and geometry databases

        args:
            rowgen: generator function 
                yields sets of rows grouped by MMSI sorted by time

        yields:
            sets of rows grouped by MMSI sorted by time with additional columns appended

    """ # set start method in main script
        import os; from multiprocessing import set_start_method
        if os.name == 'posix' and __name__ == '__main__': 
            set_start_method('forkserver')
    """
    '''

    colnames = [ 
            'mmsi', 'time', 'lon', 'lat',
            'cog', 'sog', 'msgtype',
            'imo', 'vessel_name',
            'dim_bow', 'dim_stern', 'dim_port', 'dim_star',
            'ship_type', 'ship_type_txt', 
            'deadweight_tonnage', 'submerged_hull_m^2',
            'km_from_shore', 'depth_metres',
        ]

    # read data layers from disk to merge with AIS
    print('aggregating ais, shore distance, bathymetry, vessel geometry...')
    with shore_dist_gfw() as sdist, Gebco() as bathymetry, marinetraffic.scrape_tonnage() as hullgeom:

        for rows in rowgen:
            xy = rows[:,2:4]
            mmsi_column, imo_column, ship_type_column = 0, 7, 13

            # vessel geometry
            uniqueID = {}
            for r in rows:
                uniqueID.update({f'{r[mmsi_column]}_{r[imo_column]}' : {'m' : r[mmsi_column], 'i' : r[imo_column]}})

            #print('loading marinetraffic vessel data...')
            for uid in uniqueID.values():
                ummsi, uimo = uid.values()
                if uimo != None:
                    uid['dwt'] = hullgeom.get_tonnage_mmsi_imo(ummsi, uimo)
                else:
                    uid['dwt'] = 0

            deadweight_tonnage = np.array([uniqueID[f'{r[mmsi_column]}_{r[imo_column]}']['dwt'] for r in rows ])

            # wetted surface area - regression on tonnage and ship type
            ship_type = np.logical_or(rows[:,ship_type_column], [0 for _ in range(len(rows))])
            submerged_hull = np.array([wsa(d, r) for d,r in zip(deadweight_tonnage,ship_type) ])

            # shore distance from cell grid
            km_from_shore = np.array([sdist.getdist(x, y) for x, y in xy ])

            # seafloor depth from cell grid
            depth = np.array([bathymetry.getdepth(x, y) for x,y in xy ]) * -1

            #yield np.hstack((rows, np.vstack((deadweight_tonnage, submerged_hull, km_from_shore, depth)).T))
            merged_rows = np.hstack((rows, np.vstack((deadweight_tonnage, submerged_hull, km_from_shore, depth)).T))
            yield next(trackgen(merged_rows, colnames=colnames))

