'''
compute wetted surface area using denny-mumford regression on vessel deadweight tonnage


Reference:
Moser, C. S., Wier, T. P., First, M. R., Grant, J. F., Riley, S. C., Robbins-Wamsley, S. H., ... & Drake, L. A. (2017). Quantifying the extent of niche areas in the global fleet of commercial ships: the potential for “super-hot spots” of biofouling. Biological Invasions, 19(6), 1745-1759.
'''


def wsa(dwt, ship_type, **_):
    ''' regression of Denny-Mumford WSA formula '''

    if ship_type < 30:  # None, wing in ground craft, other
        return 0

    elif ship_type == 30:  # fishing
        coef = 15.58
        exp = 0.602

    elif 52 <= ship_type <= 53:  # tugs and port tenders
        coef = 19.36
        exp = 0.553

    elif 60 <= ship_type < 70:  # passenger
        coef = 14.64
        exp = 0.671

    elif 70 <= ship_type < 80:  # cargo
        # NOTE: no distinction for container ships or bulk carriers
        # general cargo ship regression is used for these categories
        coef = 14.24
        exp = 0.596

    elif ship_type == 84:  # tankers (LNG / LPG)
        coef = 5.41
        exp = 0.699

    elif 80 <= ship_type < 90:  # tankers (general)
        coef = 9.56
        exp = 0.63

    else:
        # SAR, law enforcement, towing, dredging, diving, military, sailing, pleasure craft, etc
        coef = 26.2
        exp = 0.551

    return coef * pow(base=dwt, exp=exp)
