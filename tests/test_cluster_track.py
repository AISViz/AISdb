
from track_viz import TrackViz

assert track_dicts


test_cluster_duplicate_mmsi(track_dicts):


    # count vessels exceeding 50 knots
    n = 0
    for track in track_dicts:
        if flag(track):
            n += 1
            print(f'flagged {track["mmsi"]}\ttotal flagged: {n}')
    print(f'\nflagged {n}/{len(track_dicts)}\t( {n/len(track_dicts) * 100:.2f}% )')
    

    # manually iterate through flagged tracks for testing
    tracks = iter(track_dicts)
    while flag(track := next(tracks)) == False:
        pass


    viz = TrackViz()

    
