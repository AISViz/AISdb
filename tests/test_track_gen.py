import os
from datetime import datetime, timedelta
import cProfile
from functools import partial

from ais import zones_dir, output_dir, dbpath, tmp_dir
from ais.gis import Domain, ZoneGeomFromTxt, epoch_2_dt
#from ais.proc_util import graph
from ais.proc_util import *
from ais.track_gen import *
from ais.network_graph import aggregate_output


# load polygons geometry into Domain object
shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
domain = Domain('east', zonegeoms)
start = datetime(2020,6,1)
end = datetime(2021,10,1)



# pipeline test input config
enable_cprofile =False
fpaths = sorted([os.path.join(tmp_dir, 'db_qry', f) for f in os.listdir(os.path.join(tmp_dir, 'db_qry')) if f[:2] == '__'])
fpath = os.path.join(tmp_dir, 'db_qry', '__316001088')

# pipeline processing config
timesplit = partial(segment_tracks_timesplits,  maxdelta=timedelta(hours=6))
#distsplit = partial(segment_tracks_dbscan,      max_cluster_dist_km=50)
distsplit = partial(segment_tracks_encode_greatcircledistance, distance_meters=125000, delta_knots_threshold=30)
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
                                                )) 


def printfcn(track): 
    print(f'''mmsi={track['mmsi']}\
  start={epoch_2_dt(track['time'][0]) }\
  delta_minutes={int(track['time'][-1]-track['time'][0]): 6}\
  track_length={len(track['time']): 6}\
  {(f'  cluster_label={track["cluster_label"]: 3}'
                            if 'cluster_label' in track.keys() else ''
  )}{('  frequency='    +str(track['hourly_transits_avg']) + 'tx/hr' 
                            if 'hourly_transits_avg' in track.keys() else ''
  )}{('  zoneset='      +str(set(track['in_zone'])) 
                            if 'in_zone' in track.keys() else ''
  )}''')


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
    timesplit = partial(segment_tracks_timesplits,  maxdelta=timedelta(hours=2))
    for track in timesplit(trackgen(deserialize([fpath]))):
        printfcn(track)

def test_segment_tracks_concat_timesplits():
    max_track_length=10000
    split_len = partial(concat_tracks, max_track_length=max_track_length)
    for track in split_len(timesplit(trackgen(deserialize([fpath])))):
        printfcn(track)
        assert track['time'].size <= max_track_length

def test_segment_tracks_timesplits_concat_haversine():
    '''

    split_len = partial(concat_tracks,              max_track_length=10000)
    distsplit_test = partial(segment_tracks_encode_greatcircledistance, distance_meters=15000, max_timesplit_minutes=120)
    #track = next(timesplit(trackgen(deserialize([fpath]))))
    #track = next(split_len(timesplit(trackgen(deserialize([fpath])))))
    #testinput = geofenced(split_len(timesplit(trackgen(deserialize([fpath])))))
    testinput = split_len(timesplit(trackgen(deserialize([fpath]))))
    #testinput = geofenced(split_len(trackgen(deserialize([fpath]))))
    track = next(testinput)
    track['time'].size

    from ais.track_viz import TrackViz
    from shapely.geometry import LineString
    viz = TrackViz()

    linegeom = LineString(zip(track['lon'], track['lat']))
    viz.add_feature_polyline(linegeom, ident=str(testtrack['mmsi']), color=(255, 0, 0, 128))

    viz.clearfeatures()
    distsplit_test = partial(segment_tracks_encode_greatcircledistance, distance_meters=125000)
    #distsplit_test = partial(segment_tracks_dbscan, max_cluster_dist_km=50)
    n = 0
    testgen = distsplit_test([track])
    for testtrack in testgen:
        if len(testtrack['time']) == 1: continue
        printfcn(testtrack)
        linegeom = LineString(zip(testtrack['lon'], testtrack['lat']))
        viz.add_feature_polyline(linegeom, ident=str(testtrack['mmsi'])+str(n))
        n += 1
        #if n >= 1: break
        


    for i in range(len(segments_idx)-1):
        print(i, segments_idx[i], track['lon'][segments_idx[i]:segments_idx[i+1]], track['lat'][segments_idx[i]:segments_idx[i+1]])
        if i > 20: break

    '''
    split_len = partial(concat_tracks,              max_track_length=10000)
    distsplit_test = partial(segment_tracks_encode_greatcircledistance, distance_meters=125000)
    if not enable_cprofile:
        for track in distsplit_test(split_len(timesplit(trackgen(deserialize([fpath]))))):
            printfcn(track)
    else:
        cProfile.run("""
        for track in distsplit_test(split_len(timesplit(trackgen(deserialize([fpath]))))):
            printfcn(track)
        """, sort='tottime')


def test_segment_tracks_timesplits_concat_dbscan():
    if not enable_cprofile:
        for track in distsplit(split_len(timesplit(trackgen(deserialize([fpath]))))):
            printfcn(track)
    else:
        cProfile.run("""
            printfcn(track)
        """, sort='tottime')

def test_full_pipeline_sequential():
    fpath = os.path.join(output_dir, 'rowgen_year_test2.pickle')
    assert os.path.isfile(fpath)
    graph(fpath, domain, parallel=0)
    filters = [
        lambda rowdict: rowdict['src_zone'] == '000' and rowdict['rcv_zone'] == 'NULL',
        lambda rowdict: rowdict['minutes_spent_in_zone'] == 'NULL' or rowdict['minutes_spent_in_zone'] <= 1,
    ]
    aggregate_output(filename='output_encodedsegments_testsequential.csv', filters=filters, delete=False)

def test_full_pipeline_parallel():
    '''
    sort -k8g,8 -k9g,9 -k1,1 -k15,15 -t',' <(tail -n +2 output_encodedsegments_testparallel.csv) > testoutput.csv && head -n 1 output_encodedsegments_testparallel.csv > sorted.csv && cat testoutput.csv >> sorted.csv && rm testoutput.csv
    '''
    fpath = os.path.join(output_dir, 'rowgen_year_test2.pickle')
    assert os.path.isfile(fpath)
    graph(fpath, domain, parallel=20)
    filters = [
        lambda rowdict: rowdict['src_zone'] == '000' and rowdict['rcv_zone'] == 'NULL',
        #lambda rowdict: rowdict['minutes_spent_in_zone'] == 'NULL' or rowdict['minutes_spent_in_zone'] <= 1,
    ]
    aggregate_output(filename='output_timedelta6hrs_speeddelta30knots.csv', filters=filters, delete=True)


'''

    filters = [
        lambda rowdict: rowdict['src_zone'] == '000' and rowdict['rcv_zone'] == 'NULL',
        lambda rowdict: rowdict['minutes_spent_in_zone'] == 'NULL' or rowdict['minutes_spent_in_zone'] <= 1,
    ]
    aggregate_output(filename='output_encodedsegments.csv', filters=filters, delete=False)


rm monitoring.db; python -m pytest tests/test_track_gen.py -xs -k haversine --db monitoring.db 

sqlite3 -line monitoring.db 'select ITEM, KERNEL_TIME, CPU_USAGE, MEM_USAGE FROM  test_metrics ORDER BY ITEM_START_TIME DESC LIMIT 11;'

       ITEM = test_segment_tracks_timesplits_concat_dbscan
KERNEL_TIME = 2.57
  CPU_USAGE = 0.98128723502848
  MEM_USAGE = 11363.85546875

       ITEM = test_segment_tracks_timesplits_concat_haversine
KERNEL_TIME = 0.08
  CPU_USAGE = 0.986064429464637
  MEM_USAGE = 313.953125

       ITEM = test_segment_tracks_concat_timesplits
KERNEL_TIME = 0.08
  CPU_USAGE = 0.970889591936379
  MEM_USAGE = 300.94140625

       ITEM = test_segment_tracks_timesplits
KERNEL_TIME = 0.07
  CPU_USAGE = 0.956743566713713
  MEM_USAGE = 321.796875

       ITEM = test_generate_track_dictionary
KERNEL_TIME = 0.06
  CPU_USAGE = 0.932201872814649
  MEM_USAGE = 276.02734375

       ITEM = test_can_read_fpath_array
KERNEL_TIME = 0.07
  CPU_USAGE = 1.01452262577264
  MEM_USAGE = 313.390625

       ITEM = test_fpath_exists
KERNEL_TIME = 0.0
  CPU_USAGE = 0.0
  MEM_USAGE = 90.62109375

'''

#from proc_util import cpu_bound
#list(cpu_bound(fpaths[0], domain=domain))
#graph(fpaths, domain, parallel=0)


