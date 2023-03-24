import os
from aisdb.aisdb import receiver as _receiver


def start_receiver(sqlite_dbpath=os.path.abspath("./ais_rx.db"),
                   postgres_connection_string=None,
                   connect_addr="aisdb.meridian.cs.dal.ca:9920",
                   tcp_listen_addr=None,
                   udp_listen_addr="[::0]:9921",
                   multicast_addr=None,
                   multicast_rebroadcast=None,
                   tcp_output_addr=None,
                   udp_output_addr=None,
                   stdout=False):
    '''
        Receive raw AIS data from an upstream UDP data source, parse the data into
        JSON format, and create a websocket listener to send parsed results downstream.
        If dbpath is given, parsed data will be stored in an SQLite database.


        args:
            sqlite_dbpath (Option<str>)
                If given, raw messages will be parsed and stored in an SQLite database at this location
            postgres_connection_string (Option<String>)
                Postgres database connection string
            connect_addr (Option<str>)
                Optionally forward upstream TCP/TLS host to udp_listen_addr
            tcp_listen_addr (str)
                if not None, a thread will be spawned to forward TCP connections to
                incoming UDP port ``udp_listen_addr``
            udp_listen_addr (str)
                UDP port to listen for incoming AIS data streams e.g. "0.0.0.0:9921" or "[::]:9921"
            multicast_addr (str)
                Raw UDP messages will be parsed and then routed to TCP socket listeners via this channel.
            multicast_rebroadcast (Option<str>)
                Optionally pass a UDP rebroadcast address where raw data will be filtered
                and rebroadcasted to this channel for e.g. forwarding to downstream
                networks
            tcp_output_addr (str)
                TCP port to listen for websocket clients to send parsed data in JSON format
            stdout (bool)
                If True, raw input will be copied to stdout
    '''

    _receiver(sqlite_dbpath,
              postgres_connection_string,
              connect_addr,
              tcp_listen_addr,
              udp_listen_addr,
              multicast_addr,
              multicast_rebroadcast,
              tcp_output_addr,
              udp_output_addr,
              dynamic_msg_bufsize=128,
              static_msg_bufsize=64,
              tee=stdout)
