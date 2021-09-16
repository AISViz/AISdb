import  pickle

import numpy as np

fpath = 'scripts/dfo_project/test_query_october.pickle'
with open (fpath, 'rb') as f:
    rows = pickle.load(f)


maxlen=0
maxmmsi=''
tracks = {track['mmsi'] : track for track in trackgen(rows)}
for track in tracks.values():
    if (m := len(track['lon'])) > maxlen:   
        maxlen = m
        maxmmsi = track['mmsi']
    print(track['mmsi'], m)

testrows = np.array([
        [tracks[maxmmsi]['mmsi'] for _ in range(maxlen)],
        tracks[maxmmsi]['time'],
        tracks[maxmmsi]['lon'],
        tracks[maxmmsi]['lat'],
        tracks[maxmmsi]['cog'],
        tracks[maxmmsi]['sog'],
        [tracks[maxmmsi]['name'] for _ in range(maxlen)],
        [tracks[maxmmsi]['type'] for _ in range(maxlen)],
    ], dtype=object).T


with open('src/test/testdata/longtrack.pickle', 'wb') as f:
    pickle.dump(testrows, f)


