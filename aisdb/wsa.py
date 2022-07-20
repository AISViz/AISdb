'''
Compute wetted surface area using denny-mumford regression on vessel summer
deadweight tonnage

See table 2 in below paper for coefficient and exponent by ship type

Reference:
Moser, Cameron S., et al. "Quantifying the total wetted surface area of the world fleet: a first step in determining the potential extent of ships’ biofouling." Biological Invasions 18.1 (2016): 265-277.

'''


def _wsa(dwt, ship_type, ship_type_detailed='', **_):
    ''' regression of Denny-Mumford WSA formula using ship type '''

    # None, wing in ground craft, other
    if (isinstance(ship_type, int)
            and ship_type < 30) or ship_type == 'Wing In Grnd':
        return 0

    # fishing
    elif (isinstance(ship_type, int)
          and ship_type == 30) or ship_type == 'Fishing':
        coef = 15.58
        exp = 0.602

    # tugs and port tenders
    elif (isinstance(ship_type, int)
          and 52 <= ship_type <= 53) or ship_type == 'Tug':
        coef = 19.36
        exp = 0.553

    # passenger
    elif (isinstance(ship_type, int)
          and 60 <= ship_type < 70) or ship_type == 'Passenger':
        coef = 14.64
        exp = 0.671

    # container ships
    elif 'Container' in ship_type_detailed:
        coef = 5.39
        exp = 0.698

    # bulk carriers
    # note: cement classified as bulk carrier
    elif 'Bulk' in ship_type_detailed or 'Cement' in ship_type_detailed:
        coef = 9.57
        exp = 0.63

    # NOTE: no distinction for container ships or bulk carriers when using
    # numeric ship type
    # general cargo ship regression is used for these categories
    elif (isinstance(ship_type, int)
          and 70 <= ship_type < 80) or (isinstance(ship_type, str)
                                        and 'Cargo' in ship_type):  # cargo
        coef = 14.24
        exp = 0.596

    # tankers (LNG / LPG)
    elif (isinstance(ship_type, int) and ship_type == 84) or (
        (isinstance(ship_type, str) and
         ('Tanker' in ship_type and
          ('Oil' in ship_type_detailed or 'LNG' in ship_type_detailed
           or 'LPG' in ship_type_detailed)))):
        coef = 5.41
        exp = 0.699

    # tankers (general)
    elif (isinstance(ship_type, int)
          and 80 <= ship_type < 90) or (isinstance(ship_type, str)
                                        and 'Tanker' in ship_type):
        coef = 9.56
        exp = 0.63

    # SAR, law enforcement, towing, dredging, diving, military, sailing,
    # pleasure craft, etc
    else:
        coef = 26.2
        exp = 0.551

    return coef * pow(base=dwt, exp=exp)


def wetted_surface_area(tracks):
    ''' regression of Denny-Mumford WSA formula using ship type

        args:
            tracks (:func:`aisdb.webdata.marinetraffic.vessel_info`)
                track generator with vessel_info appended

        yields:
            track dicts with submerged surface area in square meters appended
            to key 'submerged_hull_m^2'
    '''
    for track in tracks:
        dwt = track['marinetraffic_info']['summer_dwt'] or 0
        if 'marinetraffic_info' in track.keys(
        ) and track['marinetraffic_info']['vesseltype_generic'] is not None:
            hull = _wsa(dwt, track['marinetraffic_info']['vesseltype_generic'],
                        track['marinetraffic_info']['vesseltype_detailed'])
        else:
            if 'ship_type' not in track.keys():
                raise KeyError(
                    "'ship_type' not in track: try using "
                    "aisdb.database.sqlfcn.crawl_dynamic_static as 'fcn' arg "
                    "for DBQuery.gen_qry()")
            hull = _wsa(dwt, track['ship_type'] or 0)

        track['submerged_hull_m^2'] = hull
        track['static'] = set(track['static']).union(
            set([
                'submerged_hull_m^2',
            ]))
        yield track
