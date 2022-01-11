import os
import socket
import threading
from datetime import datetime
import time

from common import dbpath, rawdata_dir, host_addr, host_port

unix_origin = datetime(1970, 1, 1)


class _AISMessageStreamReader():
    ''' read binary AIS message stream from TCP socket and log messages to rawdata_dir '''

    def __init__(self):
        self.enabled = True
        self.s = None

    def __enter__(self):
        # TODO: read host address and port number from config file
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((host_addr, host_port))

    def __call__(self):

        if self.s == None: self.__enter__()

        tmpfilepath = lambda: os.path.join(
            rawdata_dir,
            datetime.strftime(datetime.utcnow(), '%Y-%m-%d') + '.nm4.tmp')
        msgfile = tmpfilepath()
        print(f'logging messages to {rawdata_dir}')

        while self.enabled:

            if msgfile != tmpfilepath():
                os.rename(msgfile, msgfile[:-4])
                msgfile = tmpfilepath()

            # TODO: preserve last portion of first and first portion of last
            # messages from s.recv() call, attempt to reassemble messages from
            # in between calls
            with open(msgfile, 'ab') as nm4:
                epoch = str(
                    int((datetime.utcnow() - unix_origin).total_seconds()))
                prefix = b'\\c:' + bytes(epoch, encoding='utf-8') + b',\\'
                streamdata = self.s.recv(262114).split(b'\r\n')[:-1]
                msgs = (prefix + msg for msg in filter(
                    lambda rawmsg: rawmsg[:6] == b'!AIVDM', streamdata))
                for msg in msgs:
                    _ = nm4.write(msg + b'\r\n')

        self.__exit__(None, None, None)

    def __exit__(self, exc_type, exc_value, tb):
        self.enabled = False
        self.s.close()


class _AISDatabaseBuilder():
    ''' periodically check the rawdata_dir folder for new .nm4 files, and add them to the database '''

    def __init__(self, dbpath=dbpath, processes=0):
        self.dbpath = dbpath
        self.processes = processes
        self.enabled = True

    def __call__(self):
        if os.name == 'posix' and self.processes > 0:
            os.system(f'taskset -cp 0-{self.processes-1} {os.getpid()}')

        while self.enabled:
            filepaths = [
                os.path.join(rawdata_dir, f) for f in os.listdir(rawdata_dir)
                if f[-4:] == '.nm4' or f[-4:] == '.csv'
            ]
            decode_msgs(filepaths,
                        dbpath=self.dbpath,
                        processes=self.processes)
            time.sleep(10)


class MessageLogger():
    ''' extends upon _AISMessageStreamReader() and _AISDatabaseBuilder() to run them in separate threads in parallel '''

    def run(self, dbpath=dbpath, processes=0):
        try:
            self.msgtarget = _AISMessageStreamReader()
            self.msgthread = threading.Thread(target=self.msgtarget,
                                              name='AIS_messages_thread')
            self.msgthread.start()

            self.buildtarget = _AISDatabaseBuilder(dbpath, processes)
            self.dbthread = threading.Thread(target=self.buildtarget,
                                             name='database_thread')
            self.dbthread.start()

            # this will cause program to block until database operations complete
            while input(
                    'type "stop" to terminate message logging\n') != 'stop':
                print(end='', flush=True)
            self.stop()

        except KeyboardInterrupt as err:
            print(
                'caught KeyboardInterrupt, shutting down gracefully... press again to force shutdown'
            )
            self.stop()
        except Exception as err:
            raise err

    def stop(self):
        self.msgtarget.enabled = False
        self.buildtarget.enabled = False

        print(
            'stopping message logging... please wait for database operations to finish'
        )

        self.msgthread.join()
        self.dbthread.join()


'''
agent = MessageLogger()
agent.run(processes=6)

agent.stop()
'''
