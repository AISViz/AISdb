from socket import gethostname

from . import *
#import __init__

#root = os.path.dirname(__file__) + os.path.sep 
'''
root='/home/ubuntu/ais/'
'''
if gethostname() == 'BIGDATA1':
    import psycopg2 
    import psycopg2.extras
    if __name__ == '__main__': 
        psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)  # enable interrupt
    conn = psycopg2.connect(dbname='ee_ais', user=os.environ.get('PGUSER'), port=os.environ.get('PGPORT'), password=os.environ.get('PGPASS'), host='localhost')
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
else:

    if not os.path.isdir('aislib'): 
        os.mkdir('aislib')
        import requests
        import tarfile
        req = requests.get('http://www.gaia-gis.it/gaia-sins/libspatialite-5.0.0.tar.gz')
        with open(tarpath:='aislib/libspatialite-5.0.0.tar.gz', 'wb') as f: f.write(req.content)
        tar = tarfile.open(tarpath, 'r:gz')
        tar.extractall('aislib/')
        os.remove(tarpath)
        os.chdir('aislib/libspatialite-5.0.0/')
        os.system(f'./configure --prefix={os.path.abspath("..")}')
        os.system('make')
        os.system('make install-strip')
        os.chdir('../..')

    import sys
    sys.path.append(os.path.abspath('aislib/lib'))


    import sqlite3
    conn = sqlite3.connect('AISLog.db')
    cur = conn.cursor()

    sqlite3.enable_load_extension(True)
    conn.execute('SELECT sqlite3_enable_load_extension(1)')
    conn.execute('SELECT load_extension("spatialite")')

    print('todo: init db')
    conn = 'NOT INITIALIZED'
    cur = 'NOT INITIALIZED'







'''
import shapefile as shp
def writeshp(rows, pathname='/data/smith6/ais/scripts/output.shp'):
    with shp.Writer(pathname, shapeType=shp.POLYLINE) as w:
        """
        del w; w = shp.Writer(pathname, shapeType=shp.POLYLINE)
        """
        fields = ('mmsi','time','longitude','latitude','sog','cog')
        ftypes = ('N','D','F','F','F','F')
        length = (10, None, 25, 25, 20, 20)
        _ = list(map(w.field, fields, ftypes, length))
        for r in rows: w.record(**dict(zip(fields,r)))
        w.close()
'''


"""
def check_coverage_poly(kwargs):
    poly = arr2polytxt(**kwargs)
    months = dt2monthstr(**kwargs)
    qry = count_poly(months[0], poly)
    cur.execute(qry)
    res = cur.fetchall()
    print(f'poly:\t{poly}\t\tcount:\t{res}')
"""



"""

cur.scroll(0, mode='absolute')

#conn.commit()

"""

