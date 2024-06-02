import os

from aisdb import track_gen
from aisdb import DBQuery
from datetime import timedelta
from track_gen import AISProcessorInterface

import tempfile


class AISProcessor(AISProcessorInterface):
    def __init__(self, qry, decimate=False, split_time=False, time_bin='month'):
        # assert that dbquery is an instance of DBQuery
        assert isinstance(qry, DBQuery), 'dbquery must be an instance of DBQuery'
        assert qry.data['start'] is not None, 'start time must be defined'
        assert qry.data['end'] is not None, 'end time must be defined'

        self.subqry_worker_list = []

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
            time_range = [start + i * time_bin for i in range((end - start) // time_bin)]

            # iterate over the time range
            for i in range(len(time_range) - 1):
                # get the start and end time for the current time bin
                start_time = time_range[i]
                end_time = time_range[i + 1]
                # get the query for the current time bin
                current_qry = qry.copy()
                current_qry.data['start'] = start_time
                current_qry.data['end'] = end_time
                self.subqry_worker_list.append(_AISProcessorWorker(current_qry, decimate))
        else:
            self.subqry_worker_list.append(_AISProcessorWorker(qry, decimate))

    def split_timedelta(self, maxdelta=timedelta(weeks=2)):
        ''' partitions tracks where delta time exceeds maxdelta '''
        for subqry in self.subqry_worker_list:
            subqry.split_timedelta(maxdelta)

    def fence_tracks(self, domain):
        ''' compute points-in-polygons for vessel positions within domain polygons '''
        for subqry in self.subqry_worker_list:
            subqry.fence_tracks(domain)

    def zone_mask(self, domain):
        ''' compute points-in-polygons for track positions, and filter results to positions within domain '''
        for subqry in self.subqry_worker_list:
            subqry.zone_mask(domain)

    def min_speed_filter(self, minspeed):
        ''' filter tracks by minimum speed '''
        for subqry in self.subqry_worker_list:
            subqry.min_speed_filter(minspeed)

    def print_tracks(self):
        ''' print the tracks '''
        for subqry in self.subqry_worker_list:
            subqry.print_tracks()

    def save_tracks(self, filename=None):
        ''' save the tracks to a file '''
        tmpdir = tempfile.TemporaryDirectory()
        export_dir = os.path.join(tmpdir.name, 'export_data')
        index = 0
        tmp_files = []
        for subqry in self.subqry_worker_list:
            subqry.save_tracks(f'tracks_{index}.txt')
            tmp_files.append(f'tracks_{index}.txt')
            index += 1

        with open(filename, 'w') as f:
            for file in tmp_files:
                with open(os.path.join(export_dir, file), 'r') as f1:
                    f.write(f1.read())
                f.write('\n')

        tmpdir.cleanup()


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

    def min_speed_filter(self, minspeed):
        ''' filter tracks by minimum speed '''
        track_gen.min_speed_filter(self.tracks, minspeed)

    def save_tracks(self, filename):
        ''' save the tracks to a file '''
        # get tmpdir
        tmpdir = tempfile.TemporaryDirectory()
        export_dir = os.path.join(tmpdir.name, 'export_data')
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)

        with open(os.path.join(export_dir, filename), 'w') as f:
            for track in self.tracks:
                f.write(str(track) + '\n')
