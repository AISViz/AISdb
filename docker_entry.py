import os
import sys
import socket


addr = socket.gethostbyaddr(socket.gethostname())[2][0]

match addr.count("."):
  case 3:  # ipv4
    pass
  case 0:  # ipv6
    addr = "["+addr+"]"
  case _:  # ???
    print(addr)

print("Starting environment over SSH\n")

print(f"ssh -X -p 22 -i ~/.ssh/id_aisdb ais_env@{addr}", end="\n\n", flush=True)

cmd = ' '.join(sys.argv[1:])
os.system(cmd)

