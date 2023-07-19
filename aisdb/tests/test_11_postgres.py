import os
from aisdb.database.dbconn import PostgresDBConn

#import dotenv
#dotenv.load_dotenv()


def test_postgres():

    # keyword arguments
    with PostgresDBConn(
            hostaddr='fc00::17',
            user='postgres',
            port=5432,
            password=os.environ.get('POSTGRES_PASSWORD', 'devel'),
    ) as dbconn:
        cur = dbconn.cursor()
        cur.execute('select * from coarsetype_ref;')
        res = cur.fetchall()
        print(res)


# libpq connection string
'''
with PostgresDBConn('postgresql://127.0.0.1:443/postgres?') as dbconn:
    with dbconn.cursor() as cur:
        cur.execute('select * from coarsetype_ref;')
        res = cur.fetchall()
        print(res)
'''
