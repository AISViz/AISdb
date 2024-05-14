from datetime import timedelta

from aisdb import track_gen, DBQuery, AISProcessorInterface
from datetime import timedelta
from aisdb import DBQuery
from datetime import timedelta

class AISProcessor(AISProcessorInterface):
    def __init__(self, qry, decimate=False, split_time=False, time_bin = 'month'):
        # assert that dbquery is an instance of DBQuery
        assert isinstance(qry, DBQuery), 'dbquery must be an instance of DBQuery'
        assert qry.data['start'] is not None, 'start time must be defined'
        assert qry.data['end'] is not None, 'end time must be defined'

        self.subqry_worker_listt = []

        # if split_time is True, then split the data into months by default, and process each month separately
        if split_time:
            assert time_bin in ['year', 'month', 'day'], 'time_bin must be one of "year", "month", or "day"'

            # get the start and end time
            start = qry.data['start']
            end = qry.data['end']

            # get the time binning interval
            if time_bin == 'year':
                time_bin = timedelta(weeks=52)
            elif time_bin == 'month':
                time_bin = timedelta(weeks=4)
            elif time_bin == 'day':
                time_bin = timedelta(days=1)

            # get the time range
            time_range = [start + i*time_bin for i in range((end-start)//time_bin)]

            # iterate over the time range
            for i in range(len(time_range)-1):
                # get the start and end time for the current time bin
                start_time = time_range[i]
                end_time = time_range[i+1]
                # get the query for the current time bin
                current_qry = qry.copy()
                current_qry.data['start'] = start_time
                current_qry.data['end'] = end_time
                self.subqry_worker_list.append(_AISProcessorWorker(current_qry, decimate))
        else:
            self.subqry_worker_list.append(_AISProcessorWorker(qry, decimate))
    
    def split_timedelta(self, maxdelta=timedelta(weeks=2)):
        ''' partitions tracks where delta time exceeds maxdelta '''
        for subqry in self.subqry_worker_listt:
            subqry.split_timedelta(maxdelta)

    def fence_tracks(self, domain):
        ''' compute points-in-polygons for vessel positions within domain polygons '''
        for subqry in self.subqry_worker_listt:
            subqry.fence_tracks(domain)

    def zone_mask(self, domain):
        ''' compute points-in-polygons for track positions, and filter results to positions within domain '''
        for subqry in self.subqry_worker_listt:
            subqry.zone_mask(domain)

    def print_tracks(self):
        ''' print the tracks '''
        for subqry in self.subqry_worker_listt:
            subqry.print_tracks()

    def save_tracks(self, filename=None):
        ''' save the tracks to a file '''
        for subqry in self.subqry_worker_listt:
            subqry.save_tracks(filename)


class _AISProcessorWorker(AISProcessorInterface):
    ''' private class for processing tracks '''
    def __init__(self, current_qry, decimate):
        self.rowgen = current_qry.gen_qry()
        self.tracks = track_gen.TrackGen(self.rowgen, decimate=True)

    def split_timedelta(self, maxdelta=timedelta(weeks=2)):
        ''' partitions tracks where delta time exceeds maxdelta '''
        track_gen.split_timedelta(self.tracks, maxdelta)

    def fence_tracks(self, domain):
        ''' compute points-in-polygons for vessel positions within domain polygons '''
        track_gen.fence_tracks(self.tracks, domain)

    def zone_mask(self, domain):
        ''' compute points-in-polygons for track positions, and filter results to positions within domain '''
        track_gen.zone_mask(self.tracks, domain)

    def print_tracks(self):
        ''' print the tracks '''
        for track in self.tracks:
            print(track)

    def save_tracks(self, filename=None):
        ''' save the tracks to a file '''
        if filename is None:
            # use the start time as the filename, use the format YYYY-MM-DD.txt
            filename = self.tracks[0]['start'].strftime('%Y-%m-%d.csv')
        with open(filename, 'w') as f:
            for track in self.tracks:
                f.write(str(track) + '\n')
