from aisdb.aisdb import receiver


def start_receiver(udp_listen_addr="[::]:9921",
                   tcp_listen_addr="[::]:9920",
                   multicast_addr="224.0.0.20:9919",
                   dbpath=None,
                   multicast_rebroadcast=None,
                   dynamic_msg_bufsize=None,
                   static_msg_bufsize=None,
                   tee=False):

    _receiver(udp_listen_addr, tcp_listen_addr, multicast_addr, dbpath,
              multicast_rebroadcast, dynamic_msg_bufsize, static_msg_bufsize,
              tee)
