import os

SQLPATH = os.path.join('aisdb_sql')


def insert_msg_dynamic(cur, mstr, rows):
    tup123 = ((
        float(r['mmsi']),
        r['epoch'],
        r['lon'],
        r['lat'],
        r['turn'],
        r['speed'],
        r['course'],
        r['heading'],
        r['maneuver'],
        r['second'],
    ) for r in rows)

    with open(os.path.join(SQLPATH, 'insert_dynamic_clusteredidx.sql'),
              'r') as f:
        sql = f.read().format(mstr)

    cur.executemany(sql, tup123)
    return


def insert_msg_static(cur, mstr, rows):
    tup5 = (
        (
            int(r['mmsi']),
            r['epoch'],
            r['shipname'].rstrip(),
            r['shiptype'],
            r['callsign'],
            r['imo'],
            r['to_bow'],
            r['to_stern'],
            r['to_port'],
            r['to_starboard'],
            r['draught'],
            r['destination'],
            r['version'],
            r['epfd'],  # fixing device ???
            r['month'],
            r['day'],
            r['hour'],
            r['minute'],
        ) for r in rows)

    with open(os.path.join(SQLPATH, 'insert_static.sql'), 'r') as f:
        sql = f.read().format(mstr)

    cur.executemany(sql, tup5)
    return


insertfcns = {
    'msg1': insert_msg_dynamic,
    'msg2': insert_msg_dynamic,
    'msg3': insert_msg_dynamic,
    'msg5': insert_msg_static,
    'msg18': insert_msg_dynamic,
    'msg19': insert_msg_dynamic,
    'msg24': insert_msg_static,
    'msg27': insert_msg_dynamic,
}
