from gis import dt_2_epoch, shiftcoord


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
    x0 = shiftcoord([xmin])[0]
    x1 = shiftcoord([xmax])[0]
    if x0 <= x1:
        return f'''{alias}.longitude >= {x0} AND
    {alias}.longitude <= {x1} AND
    {alias}.latitude >= {ymin} AND
    {alias}.latitude <= {ymax}'''
    else:
        return f'''({alias}.longitude >= {x0} OR {alias}.longitude <= {x1}) AND
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
