import warnings

from aisdb.gis import dt_2_epoch, shiftcoord


# callback functions
def in_bbox(*, alias, xmin, xmax, ymin, ymax, **_):
    ''' SQL callback restricting vessels in bounding box region

        args:
            alias (string)
                the 'alias' in a 'WITH tablename AS alias ...' SQL statement
            xmin (float)
                minimum longitude
            xmax (float)
                maximum longitude
            ymin (float)
                minimum latitude
            ymax (float)
                maximum latitude

        returns:
            SQL code (string)
    '''
    if not -180 <= xmin <= 180:
        warnings.warn(f'got {xmin}')
        xmin = shiftcoord([xmin])[0]
    if not -180 <= xmax <= 180:
        warnings.warn(f'got {xmax}')
        xmax = shiftcoord([xmax])[0]
    if not -90 <= ymin <= 90:
        warnings.warn(f'got {ymin=}')
    if not -90 <= ymax <= 90:
        warnings.warn(f'got {ymax=}')
    assert ymin < ymax, f'got {ymin=} {ymax=}'

    if xmin == -180 and xmax == 180:
        return f'''({alias}.longitude >= {xmin} AND {alias}.longitude <= {xmax}) AND
    {alias}.latitude >= {ymin} AND
    {alias}.latitude <= {ymax}'''

    if xmin < xmax:
        #if xmin < -180 and xmax > 180:
        #    raise ValueError(f'xmin, xmax are out of bounds! {xmin=} < -180,{xmax=} > 180')
        #elif -180 <= xmin <= 180 and -180 <= xmax <= 180:
        s = f'''{alias}.longitude >= {xmin} AND
    {alias}.longitude <= {xmax} AND '''
        """
        elif xmin < -180:
            s = f'''(
        ({alias}.longitude >= -180 AND {alias}.longitude <= {xmax}) OR
        ({alias}.longitude <= 180 AND {alias}.longitude >= {shiftcoord([xmin])[0]})
    ) AND '''
        elif xmax > 180:
            s = f'''(
        ({alias}.longitude <= 180 AND {alias}.longitude >= {shiftcoord([xmax])[0]}) OR
        ({alias}.longitude >= -180 AND {alias}.longitude <= {xmin})
    ) AND '''
        else:
            raise ValueError(
                f'Error creating SQL query in longitude bounds {xmin=} {xmax=}'
            )
        """

        query_args = f'''{s}
    {alias}.latitude >= {ymin} AND
    {alias}.latitude <= {ymax}'''
        return query_args

    else:
        '''
        if xmin < -180:
            x0 = shiftcoord([xmin])[0]
        else:
            x0 = xmin
        if xmax > 180:
            x1 = shiftcoord([xmax])[0]
        else:
            x1 = xmax
        '''

        #assert x0 <= x1

        return f'''({alias}.longitude >= {xmin} OR {alias}.longitude <= {xmax}) AND
    {alias}.latitude >= {ymin} AND
    {alias}.latitude <= {ymax}'''


def in_timerange(*, alias, start, end, **_):
    ''' SQL callback restricting vessels in temporal range.

        args:
            alias (string)
                the 'alias' in a 'WITH tablename AS alias ...' SQL statement
            start (datetime)
            end (datetime)

        returns:
            SQL code (string)
    '''
    return f'''{alias}.time >= {dt_2_epoch(start)} AND
    {alias}.time <= {dt_2_epoch(end)}'''


def has_mmsi(*, alias, mmsi, **_):
    ''' SQL callback selecting a single vessel identifier

        args:
            alias (string)
                the 'alias' in a 'WITH tablename AS alias ...' SQL statement
            mmsi (int)
                vessel identifier

        returns:
            SQL code (string)
    '''
    return f'''CAST({alias}.mmsi AS INT) = {mmsi}'''


def in_mmsi(*, alias, mmsis, **_):
    ''' SQL callback selecting multiple vessel identifiers

        args:
            alias (string)
                the 'alias' in a 'WITH tablename AS alias ...' SQL statement
            mmsis (tuple)
                tuple of vessel identifiers (int)

        returns:
            SQL code (string)
    '''
    return f'''{alias}.mmsi IN
    ({", ".join(map(str, mmsis))})'''


def valid_mmsi(*, alias='m123', **_):
    ''' SQL callback selecting all vessel identifiers within the valid vessel
        mmsi range, e.g. (201000000, 776000000)

        args:
            alias (string)
                the 'alias' in a 'WITH tablename AS alias ...' SQL statement

        returns:
            SQL code (string)
    '''
    return f'''{alias}.mmsi >= 201000000 AND
    {alias}.mmsi < 776000000 '''
