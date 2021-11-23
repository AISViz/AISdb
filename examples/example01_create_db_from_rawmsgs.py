'''
Some points to note when decoding: 
  - Only position reports (messages 1, 2, 3, 18, 19) and static vessel reports (messages 5, 24) will be kept. All other messages are discarded.
  - Temporal resolution will be reduced to one message per MMSI per minute to reduce the database size and improve performance. The first occurring message will be kept.
  - Message checksums are not validated. Erroneous messages will be discarded if:
    - message cannot be decoded with pyais
    - message contains values outside the expected range, e.g. longitude > 180
'''


from ais import dbpath, decode_msgs

filepaths = ['/home/matt/ais_202101.nm4', '/home/matt/ais_202102.nm4', '.../etc']   # filepaths to raw AIS message data

decode_msgs(filepaths, dbpath)

# decoding can be sped up by processing each file in a new process
# number of maximum processes specified as follows:
decode_msgs(filepaths, dbpath=dbpath, processes=12)

