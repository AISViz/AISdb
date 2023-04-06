import os
import subprocess

# js client options
# os.environ['VITE_DISABLE_SSL_DB'] = '1'
# os.environ['VITE_DISABLE_SSL_STREAM'] = '1'
os.environ['VITE_NO_DB_LIMIT'] = '1'
os.environ['VITE_AISDBHOST'] = '127.0.0.1'
os.environ['VITE_AISDBPORT'] = '9924'
os.environ['VITE_TILESERVER'] = 'aisdb.meridian.cs.dal.ca'
os.environ['VITE_BINGMAPTILES'] = '1'

if __name__ == '__main__':
    args = ['npx', 'vite', '--port', '3000', 'aisdb_web/map']
    try:
        front_end = subprocess.Popen(args, env=os.environ)
        front_end.wait()
    finally:
        front_end.kill()
