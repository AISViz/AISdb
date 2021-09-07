'''
Some points to note when decoding: 
  - Only position reports (messages 1, 2, 3, 18, 19) and static vessel reports (messages 5, 24) will be kept. All other messages are discarded.
  - Temporal resolution will be reduced to one message per MMSI per minute. The first occurring message will be kept.
'''


from ais import dbpath, decode_msgs

filepaths = ['/home/matt/ais_202101.nm4', '/home/matt/ais_202102.nm4', '.../etc']   # filepaths to raw AIS message data

decode_msgs(filepaths, dbpath)

