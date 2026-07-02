import warnings

from aisdb.gis import dt_2_epoch, shiftcoord


# these callbacks compose WHERE fragments into larger query strings, so the
# values cannot be bound as parameters. every interpolated value is coerced
# to float/int at the interpolation site; the explicit numeric coercion is
# the invariant that closes the SQL injection vector for string-typed input.
def in_bbox(*, alias, xmin, xmax, ymin, ymax, **_):
    """SQL callback restricting vessels in bounding box region

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
    """
    xmin, xmax = float(xmin), float(xmax)
    ymin, ymax = float(ymin), float(ymax)

    if not -180 <= xmin <= 180:
        warnings.warn(f"got {xmin}")
        xmin = float(shiftcoord([xmin])[0])
    if not -180 <= xmax <= 180:
        warnings.warn(f"got {xmax}")
        xmax = float(shiftcoord([xmax])[0])
    if not -90 <= ymin <= 90:
        warnings.warn(f"got {ymin=}")
    if not -90 <= ymax <= 90:
        warnings.warn(f"got {ymax=}")
    assert ymin < ymax, f"got {ymin=} {ymax=}"

    if xmin == -180 and xmax == 180:
        return f"""({alias}.longitude >= {xmin} AND {alias}.longitude <= {xmax}) AND
    {alias}.latitude >= {ymin} AND
    {alias}.latitude <= {ymax}"""

    assert xmin < xmax, f"got {xmin=} {xmax=}"

    return f"""{alias}.longitude >= {xmin} AND
            {alias}.longitude <= {xmax} AND
    {alias}.latitude >= {ymin} AND
    {alias}.latitude <= {ymax}"""


def in_timerange(*, alias, start, end, **_):
    """SQL callback restricting vessels in temporal range.

    args:
        alias (string)
            the 'alias' in a 'WITH tablename AS alias ...' SQL statement
        start (datetime)
        end (datetime)

    returns:
        SQL code (string)
    """
    return f"""{alias}.time >= {int(dt_2_epoch(start))} AND
    {alias}.time <= {int(dt_2_epoch(end))}"""


def has_mmsi(*, alias, mmsi, **_):
    """SQL callback selecting a single vessel identifier

    args:
        alias (string)
            the 'alias' in a 'WITH tablename AS alias ...' SQL statement
        mmsi (int)
            vessel identifier

    returns:
        SQL code (string)
    """
    return f"""CAST({alias}.mmsi AS INT) = {int(mmsi)}"""


def in_mmsi(*, alias, mmsis, **_):
    """SQL callback selecting multiple vessel identifiers

    args:
        alias (string)
            the 'alias' in a 'WITH tablename AS alias ...' SQL statement
        mmsis (tuple)
            tuple of vessel identifiers (int)

    returns:
        SQL code (string)
    """
    return f"""{alias}.mmsi IN
    ({", ".join(str(int(mmsi)) for mmsi in mmsis)})"""


def valid_mmsi(*, alias="m123", **_):
    """SQL callback selecting all vessel identifiers within the valid vessel
    mmsi range, e.g. (201000000, 776000000)

    args:
        alias (string)
            the 'alias' in a 'WITH tablename AS alias ...' SQL statement

    returns:
        SQL code (string)
    """
    return f"""{alias}.mmsi >= 201000000 AND
    {alias}.mmsi < 776000000 """
