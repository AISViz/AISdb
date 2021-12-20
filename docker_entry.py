import os
import sys
import socket


addr = socket.gethostbyaddr(socket.gethostname())[2][0]

print("Starting environment over SSH\n")

print(f"ssh -X -i ~/.ssh/id_aisdb ais_env@{addr}", end="\n\n", flush=True)

cmd = ' '.join(sys.argv[1:])

os.system(cmd)

