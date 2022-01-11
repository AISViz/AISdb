import os
from aisdb import rawdata_dir, dbpath

#from aisdb.database.create_tables import aggregate_static_msgs


def rust_db_from_rawdata_dir():
    filepaths = sorted([
        "'" + os.path.join(rawdata_dir, f) + "'"
        for f in os.listdir(rawdata_dir)
    ])
    files_str = ' --file '.join(filepaths)
    x = (f"./aisdb_rust/target/release/aisdb "
         f" --dbpath '{dbpath}' --file {files_str}")
    os.system(x)
    '''

    filepaths = ["'"+os.path.join(rawdata_dir, f)+"'" for f in os.listdir(rawdata_dir)]
    n = 0
    while "exactEarth_20211105_005729Z_fb78bf45-bb2b-4192-8575-165e67b24f7b.nm4" not in filepaths[n]:
        n += 1
    filepaths = filepaths[n:]


    '''
    #aggregate_static_msgs(dbpath)


def rust_db_from_filepaths(filepaths):
    files_str = ' --file '.join(["'" + f + "'" for f in filepaths])
    x = (f"./aisdb_rust/target/release/aisdb "
         f" --dbpath '{dbpath}' --file {files_str}")
    os.system(x)
