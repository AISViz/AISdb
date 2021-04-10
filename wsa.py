'''
Quantifying the extent of niche areas in the global fleet of commercial ships: the potential for ‘‘super-hot spots’’of biofouling

Cameron S. Moser, Timothy P. Wier, Matthew R. First, Jonathan F. Grant, Scott C. Riley, Stephanie H. Robbins-Wamsley, Mario N. Tamburri, Gregory M. Ruiz, A. Whitman Miller, Lisa A. Drake
'''


def dwt(mmsi):
    ''' returns dead weight tonnage for a given vessel MMSI '''
    # TODO: scrape DWT data from web or other external database
    return 100000.0


def wsa(ship_type, mmsi, imo, **_):
    ''' regression of Denny-Mumford WSA formula '''

    if msg5row['ship_type'] < 30:           # wing in ground craft
        assert False 

    elif msg5row['ship_type'] == 30:        # fishing
        coef = 15.58
        exp = 0.602

    elif msg5row['ship_type'] == 31:        # towing
        pass

    elif msg5row['ship_type'] == 32:        # large towing
        pass

    elif 80 <= msg5row['ship_type'] < 90:   # tankers
        coef = 9.56
        exp = 0.63

    # TODO: complete the rest of the coefficient and exponent values as described in table 2

    return coef * pow(base=get_tonnage_mmsi_imo(mmsi=mmsi, imo=imo), exp=exp)



if __name__ == '__main__':
    # testing 

    from database import *
    conn = dbconn('/run/media/matt/Seagate Backup Plus Drive/python/ais.db').conn
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute('select * from ais_s_201806_msg_5 as x where x.mmsi is not NULL and x.draught is not NULL and x.ship_type is not NULL and x.dim_bow is not NULL limit 10')
    res = list(map(dict, cur.fetchall()))
    msg5row = res[0]

    wsa(msg5row) 
