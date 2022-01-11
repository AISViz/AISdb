def insert_msg123(cur, mstr, rows):
    #cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="rtree_{mstr}_msg_1_2_3" ')
    #if not cur.fetchall():
    #    sqlite_create_table_msg123(cur, mstr)

    tup123 = (
        (
            float(r['mmsi']),
            r['epoch'],
            #r['type'],
            r['lon'],
            r['lat'],
            # int(r['status']),
            r['turn'],
            r['speed'],
            r['course'],
            r['heading'],
            r['maneuver'],
            r['second'],
        ) for r in rows)
    coveridx = ((r['mmsi'], r['epoch']) for r in rows)
    cur.executemany(
        f'''
                    INSERT OR IGNORE INTO ais_{mstr}_dynamic
                    (mmsi, time,
                    --msgtype,
                    longitude, latitude,
                    --navigational_status,
                    rot, sog, cog,
                    heading, maneuver, utc_second)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    ''', tup123)
    return


def insert_msg5(cur, mstr, rows):
    #cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="ais_{mstr}_msg_5" ')
    #if not cur.fetchall():
    #    create_table_msg5(cur, mstr)

    tup5 = (
        (  # r['type'], r['repeat'],
            int(r['mmsi']),
            # r['ais_version'],
            r['imo'],
            r['callsign'],
            r['shipname'].rstrip(),
            # r['shiptype'],
            r['to_bow'],
            r['to_stern'],
            r['to_port'],
            r['to_starboard'],
            # r['epfd'],
            r['month'],
            r['day'],
            r['hour'],
            r['minute'],
            r['draught'],
            r['destination'],
            # r['dte'],
            r['epoch']) for r in rows)
    cur.executemany(
        f'INSERT OR IGNORE INTO ais_{mstr}_static'
        '('
        #'message_id, repeat_indicator, '
        'mmsi, '
        #'ais_version, '
        'imo, call_sign, '
        'vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star, '
        #'fixing_device, '
        'eta_month, eta_day, eta_hour, eta_minute, draught, '
        'destination, '
        #'dte, '
        'time) '
        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        tup5)
    """
    cur.executemany(
        f'INSERT OR IGNORE INTO ais_{mstr}_static'
        '('
        #'message_id, repeat_indicator, '
        'mmsi, '
        #'sequence_id, '
        'vessel_name, '
        #'ship_type, vendor_id, model, serial, '
        'call_sign, dim_bow, dim_stern, dim_port, dim_star, '
        #'mother_ship_mmsi, '
        'time) '
        'VALUES (?,?,?,?,?,?,?,?)',
        tup5)
    """
    return


def insert_msg18(cur, mstr, rows):
    #cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="rtree_{mstr}_msg_18" ')
    #if not cur.fetchall():
    #    sqlite_create_table_msg18(cur, mstr)

    tup18 = (
        (
            int(r['mmsi']),
            r['epoch'],
            #r['type'],
            r['lon'],
            r['lat'],
            #r['radio'] if 'radio' in r.keys() else None,
            r['speed'],
            r['course'],
            r['heading'],
            r['second'],
        ) for r in rows)
    cur.executemany(
        f'''
                    INSERT OR IGNORE INTO ais_{mstr}_dynamic
                    (mmsi, time,
                    --msgtype,
                    longitude, latitude,
                    --navigational_status,
                    sog, cog,
                    heading, utc_second)
                    VALUES (?,?,?,?,?,?,?,?)
                    ''', tup18)
    return


def insert_msg24(cur, mstr, rows):
    #cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="ais_{mstr}_msg_24" ')
    #if not cur.fetchall():
    #    create_table_msg24(cur, mstr)

    tup24 = (
        (
            #r['type'],
            #r['repeat'],
            int(r['mmsi']),
            #r['partno'],
            r['shipname'] if r['partno'] == 0 else None,
            #r['shiptype'] if r['partno'] == 1 else None,
            #r['vendorid'] if r['partno'] == 1 else None,
            #r['model'] if r['partno'] == 1 else None,
            #r['serial'] if r['partno'] == 1 else None,
            r['callsign'] if r['partno'] == 1 else None,
            r['to_bow'] if r['partno'] == 1 else None,
            r['to_stern'] if r['partno'] == 1 else None,
            r['to_port'] if r['partno'] == 1 else None,
            r['to_starboard'] if r['partno'] == 1 else None,
            #r['mothership_mmsi'] if r['partno'] == 1 else None,
            r['epoch'],
        ) for r in rows)
    cur.executemany(
        f'INSERT OR IGNORE INTO ais_{mstr}_static'
        '('
        #'message_id, repeat_indicator, '
        'mmsi, '
        #'sequence_id, '
        'vessel_name, '
        #'ship_type, vendor_id, model, serial, '
        'call_sign, dim_bow, dim_stern, dim_port, dim_star, '
        #'mother_ship_mmsi, '
        'time) '
        'VALUES (?,?,?,?,?,?,?,?)',
        tup24)
    return


insertfcns = {
    'msg1': insert_msg123,
    'msg2': insert_msg123,
    'msg3': insert_msg123,
    'msg5': insert_msg5,
    'msg18': insert_msg18,
    #'msg19' : ,
    'msg24': insert_msg24,
    #'msg27' : insert_msg123,
}
