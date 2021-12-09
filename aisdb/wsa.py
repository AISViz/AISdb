'''
Quantifying the extent of niche areas in the global fleet of commercial ships: the potential for ‘‘super-hot spots’’of biofouling

Cameron S. Moser, Timothy P. Wier, Matthew R. First, Jonathan F. Grant, Scott C. Riley, Stephanie H. Robbins-Wamsley, Mario N. Tamburri, Gregory M. Ruiz, A. Whitman Miller, Lisa A. Drake
'''
#from webdata.marinetraffic import get_tonnage_mmsi_imo


def wsa(dwt, ship_type, **_):
    ''' regression of Denny-Mumford WSA formula '''

    if ship_type < 30:              # None, wing in ground craft, other
        return 0

    elif ship_type == 30:           # fishing
        coef = 15.58
        exp = 0.602

    elif 52 <= ship_type <= 53:     # tugs and port tenders
        coef = 19.36
        exp = 0.553

    elif 60 <= ship_type < 70:      # passenger
        coef = 14.64
        exp = 0.671
    
    elif 70 <= ship_type < 80:      # cargo
        # NOTE: no distinction for container ships or bulk carriers
        # general cargo ship regression is used for these categories
        coef = 14.24
        exp = 0.596

    elif ship_type == 84:           # tankers (LNG / LPG)
        coef = 5.41
        exp = 0.699

    elif 80 <= ship_type < 90:      # tankers (general)
        coef = 9.56
        exp = 0.63
        
    else:
        # SAR, law enforcement, towing, dredging, diving, military, sailing, pleasure craft, etc
        coef = 26.2
        exp = 0.551
    
    return coef * pow(base=dwt, exp=exp)



if False:
    # testing 

    from database import *
    conn = dbconn('/run/media/matt/Seagate Backup Plus Drive/python/ais.db').conn
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute('select * from ais_s_201806_msg_5 as x where x.mmsi is not NULL and x.draught is not NULL and x.ship_type is not NULL and x.dim_bow is not NULL limit 10')
    res = list(map(dict, cur.fetchall()))
    msg5row = res[0]

    wsa(msg5row) 
