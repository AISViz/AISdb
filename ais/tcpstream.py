import os
import socket
import threading
from datetime import datetime

from common import rawdata_dir

'''
\c:1516593316,C:625,s:Q-HavreStPierre*7A\!AIVDM,1,1,9,B,14eIOF0000sO9a8Lgm6Ew:6P1TKL,0*1C
splitmsg    = lambda rawmsg: rawmsg.split('\\')
parsetime   = lambda comment: dt_2_epoch(datetime.fromtimestamp(int(comment.split('c:')[1].split(',')[0].split('*')[0])))
'''

unix_origin = datetime(1970, 1, 1)


class _threaded():
    def __enter__(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(('data.aishub.net', 5185))

    def __exit__(self, exc_type, exc_value, tb):
        pass



class AISMessageStream():
    def __init__(self):

    def _threaded(self):

        while True:
            msgfile = os.path.join(rawdata_dir, datetime.strftime(datetime.utcnow(), '%Y-%m-%d') + '.nm4')
            with open(msgfile, 'ab') as nm4:
                #test = s.recv(65536)
                epoch = str(int((datetime.utcnow() - unix_origin).total_seconds()))
                prefix = b'\\c:' + bytes(epoch, encoding='utf-8') + b',\\'
                streamdata = self.s.recv(262114).split(b'\r\n')[:-1]
                msgs = (prefix + msg for msg in filter(lambda rawmsg: rawmsg[:6] == b'!AIVDM', streamdata))
                for msg in msgs:
                    _ = nm4.write(msg + b'\r\n')

    def run():
        pass

    def stop():
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, tb):
        pass


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('data.aishub.net', 5185))


while True:
    msgfile = os.path.join(rawdata_dir, datetime.strftime(datetime.utcnow(), '%Y-%m-%d') + '.nm4')
    with open(msgfile, 'ab') as nm4:
        #test = s.recv(65536)
        stamp = str(int((datetime.utcnow() - unix_origin).total_seconds()))
        prefix = b'\\c:' + bytes(stamp, encoding='utf-8') + b',\\'
        streamdata = s.recv(262114).split(b'\r\n')[:-1]
        msgs = (prefix + msg for msg in filter(lambda rawmsg: rawmsg[:6] == b'!AIVDM', streamdata))
        for msg in msgs:
            _ = nm4.write(msg + b'\r\n')

s.close()
