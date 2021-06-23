from database import *

aisdb = dbconn(dbpath)
cur, conn = aisdb.cur, aisdb.conn


cur.execute('SELECT mmsi, imo, vessel_name, CAST(time AS INT) FROM ais_201806_msg_5 ORDER BY mmsi, time, imo')
res = np.array(cur.fetchall(), dtype=object)
res[:,-1] = epoch_2_dt(res[:,-1])
res

tracks_idx = np.append(np.append([0], np.nonzero(res[:,0].astype(int)[1:] != res[:,0].astype(int)[:-1])[0]+1), len(res))


observations = []
for i in range(len(tracks_idx)-1):

    if res[tracks_idx[i]][0] > 700000000 or res[tracks_idx[i]][0] < 200000000: continue

    if res[tracks_idx[i]][1] == 0 or res[tracks_idx[i]][1] == '': 
        print(f'no IMO for mmsi {res[tracks_idx[i]][0]}, skipping...')
        continue

    first_observation = res[tracks_idx[i]][-1]
    last_observation = res[tracks_idx[i+1]-1][-1]

    observations.append(list(res[tracks_idx[i]][:-1]) + [first_observation] + [last_observation])


with open(fpath:='output/ballast_request_june2018.csv', 'w') as f:
    f.write('mmsi,imo,vessel_name,start_time,end_time\n')

writecsv(observations, pathname=fpath)

