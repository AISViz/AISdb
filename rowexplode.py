import os
from multiprocessing import Pool#, set_start_method
from functools import partial
from datetime import datetime, timedelta

import numpy as np


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
