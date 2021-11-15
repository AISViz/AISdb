import os
from multiprocessing import Pool#, set_start_method
from functools import partial
from datetime import datetime, timedelta

import numpy as np


'''
res = next(getrows(qryfcn, rows_months, months_str, cols))
gen = explode_month(*next(getrows(qryfcn,rows_months,months_str), cols, csvfile))

'''
"""
if __name__ == '__main__':
    for track in trackgen(rows):
        for rng in segment_rng(track, maxdelta=timedelta(hours=2), minsize=3):
            mask = filtermask(track, rng)
            sog_haversine = compute_knots(track, rng)
            print(f'{track["mmsi"]}  rng: {rng}  sog vs haversine avg diff: '
                  f'{np.average(np.abs(np.append([0],sog_haversine)[mask] - track["sog"][rng][mask]))}')
"""


def getrows(conn, qryfcn, rows_months, months_str, cols):
    '''
    qrows_month, mstr = rows_months[-1], months_str[-1]
    '''
    #callback = lambda alias, ummsi, **_: f'''{alias}.mmsi in ('{"', '".join(map(str, ummsi))}')'''
    callback = lambda alias, ummsi, **_: f'''{alias}.mmsi in ({", ".join(map(str, ummsi))})'''
    for qrows_month, mstr in zip(rows_months, months_str):
        print(f'querying {mstr}...')
        ummsi = np.unique(qrows_month[:,cols['mmsi']])
        qry = qryfcn(mstr, callback, dict(ummsi=ummsi)) + '\nORDER BY mmsi, time'
        cur = conn.cursor()
        cur.execute(qry)
        res = np.array(cur.fetchall())
        #logging.debug(f'{np.unique(res[:,0])}')
        yield dict(res=res, qrows_month=qrows_month, cols=cols, mstr=mstr)


def explode_month(kwargs, csvfile, keepraw=True):
    '''
    kwargs = list(getrows(qryfcn,rows_months, months_str, cols))[-1]
    '''
    qrows_month, res, mstr, cols = kwargs['qrows_month'], kwargs['res'], kwargs['mstr'], kwargs['cols']
    print(f'exploding rowdata from {mstr}...')
    tracks = { t['mmsi'] : t for t in trackgen([res]) }

    raw = np.ndarray(shape=(0, len(cols['keepcols']) + 8), dtype=object)
    out = np.ndarray(shape=(0, len(cols['keepcols']) + 9), dtype=object)
    for qrow in qrows_month:
        if (mmsi := int(qrow[cols['mmsi']])) not in tracks.keys(): continue
        if len(tracks[mmsi]['time']) == 0: continue
        mask = ((tracks[mmsi]['time'] > qrow[cols['start']]) * (tracks[mmsi]['time'] < qrow[cols['end']]))
        n = sum(mask)
        if n == 0: continue
        track = tracks[mmsi].copy()
        for c in ['time', 'lon', 'lat', 'cog', 'sog']: track[c] = track[c][mask] 
        if keepraw: raw = np.vstack((raw, 
            np.vstack((
                    *(np.array([qrow[c] for _ in range(n)]) for c in cols['keepcols']),
                    np.array([track['mmsi'] for _ in range(n)], dtype=np.uint32), # mmsi
                    np.array([track['vessel_name'] for _ in range(n)]), # name 
                    np.array([track['ship_type_txt'] for _ in range(n)]), # type
                    track['time'],
                    track['lon'],
                    track['lat'],
                    track['cog'],
                    track['sog'],
                )).T
            ))
        for rng in segment_rng(track, timedelta(days=7), minsize=1):
            mask2 = filtermask(track, rng, filters=[lambda track, rng: compute_knots(track, rng) < 50])
            n2 = sum(mask2)
            out = np.vstack((out, 
                    np.vstack((
                        *(np.array([qrow[c] for _ in range(n2)]) for c in cols['keepcols']),
                        np.array([track['mmsi'] for _ in range(n2)], dtype=np.uint32), # mmsi
                        np.array([track['vessel_name'] for _ in range(n2)]), # name 
                        np.array([track['ship_type_txt'] for _ in range(n2)]), # type
                        track['time'][rng][mask2],
                        track['lon'][rng][mask2],
                        track['lat'][rng][mask2],
                        track['cog'][rng][mask2],
                        track['sog'][rng][mask2],
                        np.append(compute_knots(track, rng), [0]).astype(np.uint16)[mask2],
                    )).T
                ))
                    
    assert len(out) > 0

    print(f'mmsis found: {len(np.unique(raw[:,3]))} / {len(np.unique(qrows_month[:,3]))}')

    if keepraw: writecsv(raw, csvfile + f'.raw.{mstr}', mode='a',)
    writecsv(out, csvfile + f'.filtered.{mstr}', mode='a',)
    

def explode(conn, qryfcn, qryrows, cols, dateformat='%m/%d/%Y', csvfile='output/test.csv'):
    '''
        crawl database for rows with matching mmsi + time range

        cols = {'mmsi':3, 'start':-2, 'end':-1, 'sort_output':1, 'keepcols':[0,1,2]}
 
        
    '''
    print('building index...')

    # sort by mmsi, time
    qrows = qryrows[qryrows[:,cols['mmsi']].argsort()]
    dt = np.array(list(map(lambda t: datetime.strptime(t, dateformat), qrows[:,cols['start']])))
    qrows = qrows[dt.argsort(kind='mergesort')]
    dt = np.array(list(map(lambda t: datetime.strptime(t, dateformat), qrows[:,cols['start']])))
    dt2 = np.array(list(map(lambda t: datetime.strptime(t, dateformat), qrows[:,cols['end']])))

    # filter timestamps outside data range
    exists = np.logical_or( 
            ((datetime(2011, 1, 1) < dt) & (dt < datetime(datetime.today().year, datetime.today().month, 1))),
            ((datetime(2011, 1, 1) < dt2) & (dt2 < datetime(datetime.today().year, datetime.today().month, 1)))
        )
    qrows = qrows[exists]
    dt = dt[exists]
    dt2 = dt2[exists]

    # filter bad mmsi
    hasmmsi = np.array(list(map(str.isnumeric, qrows[:,cols['mmsi']])))
    qrows = qrows[hasmmsi]
    dt = dt[hasmmsi]
    dt2 = dt2[hasmmsi]
    validmmsi = np.logical_and((m := qrows[:,cols['mmsi']].astype(int)) >= 201000000, m < 776000000)
    qrows = qrows[validmmsi]
    dt = dt[validmmsi]
    dt2 = dt2[validmmsi]
    qrows[:,cols['start']] = dt
    qrows[:,cols['end']] = dt2

    # aggregate by month
    #months_str = dt2monthstr(dt2[0], dt[-1])
    months_str = dt2monthstr(dt[0], dt2[-1])
    months = np.array([datetime.strptime(m, '%Y%m') for m in months_str])
    months = np.append(months, months[-1] + timedelta(days=31))
    rows_months = [ qrows[((dt > m1) & (dt < m2)) | ((dt2 > m1) & (dt2 < m2))] for m1, m2 in zip(months[:-1], months[1:]) ]

    '''
    kwargs = next(getrows(qryfcn,rows_months[-1:],months_str[-1:], cols))
    explode_month(kwargs, csvfile)
    '''

    '''
    with Pool(processes=3) as p:
        p.imap_unordered(partial(explode_month, csvfile=csvfile), getrows(conn, qryfcn,rows_months,months_str, cols))
        p.close()
        p.join()

    '''
    for kwargs in getrows(qryfcn,rows_months, months_str, cols):
        explode_month(kwargs, csvfile)

    #os.system(f"sort -t ',' -k {cols['sort_output']} -o raw_{csvfile} {csvfile}.raw.*")
    os.system(f"sort -t ',' -k {cols['sort_output']} --output=output/raw.csv {csvfile}.raw.*")
    os.system(f"sort -t ',' -k {cols['sort_output']} --output=output/filtered.csv {csvfile}.filtered.*")
    #os.system(f'rm {csvfile}.raw*')
    #os.system(f'rm {csvfile}.filtered*')

    print(f'omitted rows (no data in time range): {sum(~exists)}')
    print(f'omitted rows (no mmsi): {len(qryrows) - sum(validmmsi)}')

'''
csvfile = 'output/test.csv'
cols = {'mmsi':3, 'start':-2, 'end':-1, 'sort_output':1, 'keepcols':[0,1,2]}
csvfile='output/test.csv'
test = explode_month(csvfile=csvfile, **next(getrows(qryfcn,rows_months,months_str,cols)))
explode(qryfcn, qryrows, cols, csvfile='output/test.csv')


test = list(getrows(qryfcn,row_months,months_str, cols))
'''
