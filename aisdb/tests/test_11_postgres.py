from aisdb.database.dbconn import PostgresDBConn
from aisdb.tests.create_testing_data import postgres_test_conn

#import dotenv
#dotenv.load_dotenv()


def test_postgres():

    # keyword arguments
    with PostgresDBConn(**postgres_test_conn) as dbconn:
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
