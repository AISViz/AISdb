import os
from datetime import datetime, timedelta
import cProfile

from ais import zones_dir, output_dir, dbpath, tmp_dir
from ais.gis import Domain, ZoneGeomFromTxt, epoch_2_dt
#from ais.proc_util import graph
from ais.proc_util import *
from ais.track_gen import *


# load polygons geometry into Domain object
shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
domain = Domain('east', zonegeoms)
start = datetime(2020,6,1)
end = datetime(2021,10,1)



# pipeline test input config
enable_cprofile =False
fpaths = sorted([os.path.join(tmp_dir, 'db_qry', f) for f in os.listdir(os.path.join(tmp_dir, 'db_qry')) if f[:2] == '__'])
fpath = '/meridian/aisdb/tmp_parsing/db_qry/__316001088'

# pipeline processing config
timesplit = partial(segment_tracks_timesplits,  maxdelta=timedelta(hours=2))
distsplit = partial(segment_tracks_dbscan,      max_cluster_dist_km=50)
geofenced = partial(fence_tracks,               domain=domain)
split_len = partial(concat_tracks,              max_track_length=10000)
filtering = partial(filter_tracks,              filter_callback=lambda track: (
                                                    len(track['time']) <= 2
                                                    or track['hourly_transits_avg'] > 6 
                                                    or set(track['in_zone']) == {'Z0'}
                                                    or np.max(delta_knots(track, np.array(range(len(track['time']))))) > 50
                                                ),
                                                logging_callback=lambda track: (
                                                    track['hourly_transits_avg'] > 6 
                                                    or not (len(track['time']) > 1 
                                                        and np.max(delta_knots(track, np.array(range(len(track['time']))))) > 50) 
                                                ),)


def printfcn(track): 
    print(f'''mmsi={track['mmsi']}\
  start={epoch_2_dt(track['time'][0]) }\
  delta_minutes={int(track['time'][-1]-track['time'][0]): 6}\
  track_length={len(track['time']): 4}\
  {(f'  cluster_label={track["cluster_label"]: 3}'
                            if 'cluster_label' in track.keys() else ''
  )}{('  frequency='    +str(track['hourly_transits_avg']) + 'tx/hr' 
                            if 'hourly_transits_avg' in track.keys() else ''
  )}{('  zoneset='      +str(set(track['in_zone'])) 
                            if 'in_zone' in track.keys() else ''
  )}''')

  #)}{('  frequency='    +str(sum(np.nonzero(track['in_zone'][1:] != track['in_zone'][:-1])[0])  
  #                          /  ((track['time'][-1]-track['time'][0]) or 1 / 60)) 
  #                          if 'in_zone' in track.keys() else ''

def test_fpath_exists():
    assert os.path.isfile(fpath), f'{fpath = }'

def test_can_read_fpath_array():
    for rows in deserialize([fpath]):
        assert isinstance(rows, np.ndarray), f'{rows = }'
        print(rows[0])

def test_generate_track_dictionary():
    for track in trackgen(deserialize([fpath])):
        assert isinstance(track, dict)
        print(track['mmsi'])

def test_segment_tracks_timesplits():
    for track in timesplit(trackgen(deserialize([fpath]))):
        printfcn(track)

def test_segment_tracks_timesplits_dbscan():
    if not enable_cprofile:
        for track in distsplit(timesplit(trackgen(deserialize([fpath])))):
            printfcn(track)
    else:
        cProfile.run("""
            printfcn(track)
        """, sort='tottime')

def test_segment_tracks_timesplits_haversine():
    distsplit_test = partial(segment_tracks_encode_greatcircledistance, distance_meters=50000)
    if not enable_cprofile:
        for track in distsplit_test(timesplit(trackgen(deserialize([fpath])))):
            printfcn(track)
    else:
        cProfile.run("""
        for track in distsplit_test(timesplit(trackgen(deserialize([fpath])))):
            printfcn(track)
        """, sort='tottime')

def test_segment_tracks_timesplits_concat_haversine_concat():
    '''
    track = next(timesplit(trackgen(deserialize([fpath]))))
    track = next(split_len(timesplit(trackgen(deserialize([fpath])))))
    track = next(geofenced(split_len(timesplit(trackgen(deserialize([fpath]))))))
    testgen = distsplit_test([track])
    test = next(testgen)
    printfcn(test)
    '''
    split_len = partial(concat_tracks,              max_track_length=10000)
    distsplit_test = partial(segment_tracks_encode_greatcircledistance, distance_meters=300000)
    if not enable_cprofile:
        for track in split_len(distsplit_test(geofenced(split_len(timesplit(trackgen(deserialize([fpath]))))))):
            printfcn(track)
    else:
        cProfile.run("""
        for track in split_len(distsplit_test(split_len(timesplit(trackgen(deserialize([fpath])))))):
            printfcn(track)
        """, sort='tottime')

def test_geofencing_segment_tracks_timesplits_dbscan():
    if not enable_cprofile:
        for track in geofenced(distsplit(timesplit(trackgen(deserialize([fpath]))))):
            printfcn(track)
    else:
        cProfile.run("""
        for track in geofenced(distsplit(timesplit(trackgen(deserialize([fpath]))))):
            printfcn(track)
        """, sort='tottime')


def test_concat_geofencing_segment_tracks_timesplits_dbscan():
    if not enable_cprofile:
        for track in split_len(distsplit(split_len(geofenced(timesplit(trackgen(deserialize([fpath]))))))):
            printfcn(track)

    else:
        cProfile.run("""
        for track in split_len(distsplit(split_len(geofenced(timesplit(trackgen(deserialize([fpath]))))))):
            printfcn(track)
        """, sort='tottime')

def test_frequencyfilter_concat_geofencing_segment_tracks_timesplits_dbscan():
    if not enable_cprofile:
        for track in filtering(tracks_transit_frequency(split_len(distsplit(split_len(geofenced(timesplit(trackgen(deserialize([fpath]))))))))):
            printfcn(track)
    else:
        cProfile.run("""
        for track in filtering(tracks_transit_frequency(split_len(distsplit(split_len(geofenced(timesplit(trackgen(deserialize([fpath]))))))))):
            printfcn(track)
        """, sort='tottime')

'''
python -m pytest tests/test_track_gen.py -xs --db monitoring.db; 
sqlite3 -line monitoring.db 'select ITEM, KERNEL_TIME, CPU_USAGE, MEM_USAGE FROM  test_metrics ORDER BY ITEM_START_TIME DESC LIMIT 11;'

sqlite3 monitoring.db

sqlite> 
	select ITEM, KERNEL_TIME, CPU_USAGE, MEM_USAGE FROM  test_metrics ORDER BY ITEM_START_TIME DESC LIMIT 11;

test_frequencyfilter_concat_geofencing_segment_tracks_timesplits_dbscan|8.2|0.997883308316579|7523.12890625
test_concat_geofencing_segment_tracks_timesplits_dbscan|7.7|0.998035443409595|7521.45703125
test_geofencing_segment_tracks_timesplits_dbscan|18.41|0.998161077821614|17089.18359375
test_segment_tracks_timesplits_dbscan|15.19|0.999731417796535|17172.54296875
test_segment_tracks_timesplits|0.199999999999999|0.975612950207259|346.2578125
test_generate_track_dictionary|0.23|0.991500440572578|336.67578125
test_can_read_fpath_array|0.220000000000001|0.995004864911067|336.671875
test_fpath_exists|0.0199999999999996|1.20611236678801|91.7421875
test_segment_tracks_timesplits|0.19|0.97752798260189|357.23828125
test_generate_track_dictionary|0.220000000000001|0.992400962008736|336.83203125
test_can_read_fpath_array|0.26|0.979679320669917|336.82421875
'''

#from proc_util import cpu_bound
#list(cpu_bound(fpaths[0], domain=domain))
#graph(fpaths, domain, parallel=0)


