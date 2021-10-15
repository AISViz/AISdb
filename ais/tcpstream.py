import os
import socket
import threading
from datetime import datetime
import time

from common import rawdata_dir

'''
\c:1516593316,C:625,s:Q-HavreStPierre*7A\!AIVDM,1,1,9,B,14eIOF0000sO9a8Lgm6Ew:6P1TKL,0*1C
splitmsg    = lambda rawmsg: rawmsg.split('\\')
parsetime   = lambda comment: dt_2_epoch(datetime.fromtimestamp(int(comment.split('c:')[1].split(',')[0].split('*')[0])))
'''

unix_origin = datetime(1970, 1, 1)


class _AISMessageStream():

    def __init__(self):
        self.enabled = True
        self.s = None

    def __enter__(self):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(('data.aishub.net', 5185))

    def __call__(self):

        if self.s == None: self.__enter__()
        
        tmpfilepath = lambda: os.path.join(rawdata_dir, datetime.strftime(datetime.utcnow(), '%Y-%m-%d') + '.nm4.tmp')
        msgfile = tmpfilepath()
        print(f'logging messages to {rawdata_dir}')

        while self.enabled:

            if msgfile != tmpfilepath():
                os.rename(msgfile, msgfile[:-4])
                msgfile = tmpfilepath()

            with open(msgfile, 'ab') as nm4:
                epoch = str(int((datetime.utcnow() - unix_origin).total_seconds()))
                prefix = b'\\c:' + bytes(epoch, encoding='utf-8') + b',\\'
                streamdata = self.s.recv(262114).split(b'\r\n')[:-1]
                msgs = (prefix + msg for msg in filter(lambda rawmsg: rawmsg[:6] == b'!AIVDM', streamdata))
                for msg in msgs:
                    _ = nm4.write(msg + b'\r\n')

        self.__exit__(None, None, None)

    def __exit__(self, exc_type, exc_value, tb):
        self.enabled = False
        self.s.close()


class _AISDatabaseBuilder():

    def __init__(self, dbpath=dbpath, processes=0):
        self.dbpath = dbpath
        self.processes = processes
        self.enabled = True

    def __call__(self):
        if os.name == 'posix' and self.processes > 0:
            os.system(f'taskset -cp 0-{self.processes-1} {os.getpid()}')

        while self.enabled:
            filepaths = [os.path.join(rawdata_dir, f) for f in os.listdir(rawdata_dir) if f[-4:]=='.nm4' or f[-4:]=='.csv']
            decode_msgs(filepaths, dbpath=self.dbpath, processes=self.processes)
            time.sleep(10)


class AISMessageStream():

    def run(self, dbpath=dbpath, processes=0):
        try:
            self.msgtarget = _AISMessageStream()
            self.msgthread = threading.Thread(target=self.msgtarget, name='AIS_messages_thread')
            self.msgthread.start()

            self.buildtarget = _AISDatabaseBuilder(dbpath, processes)
            self.dbthread = threading.Thread(target=self.buildtarget, name='database_thread')
            self.dbthread.start()
        except KeyboardInterrupt as err:
            print('caught KeyboardInterrupt, shutting down gracefully... press again to force shutdown')
            self.stop()
        except Exception as err:
            raise err

    def stop(self):
        self.msgtarget.enabled = False
        self.dbthread.enabled = False

        print('stopping message logs... please wait for database operations to finish')

        self.msgthread.join()
        self.dbthread.join()


'''
agent = AISMessageStream()
agent.run(processes=6)

agent.stop()


filepaths = sorted([os.path.join(rawdata_dir, f) for f in os.listdir(rawdata_dir) if '.nm4' in f])
filepaths=filepaths[-1:]

decode_msgs(filepaths, dbpath=dbpath, processes=0)
'''
