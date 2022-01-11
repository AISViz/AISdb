import os
import sys
import socket

addr = socket.gethostbyaddr(socket.gethostname())[2][0]

print("Starting environment over SSH\n")
print(f"ssh -X ais_env@{addr} -i ~/.ssh/id_aisdb", end="\n\n", flush=True)

cmd = f'''
/sbin/sshd -D -e
-h /run/secrets/host_ssh_key
-oAuthorizedKeysFile=/run/secrets/host_authorized_keys
-oDenyUsers=root
-oKbdInteractiveAuthentication=no
-oPasswordAuthentication=no
-oPermitEmptyPasswords=no
-oPrintMotd=no
-oPort=22
-oPubkeyAuthentication=yes
-oUseDNS=no
-oX11Forwarding=yes
-oX11UseLocalhost=no
{' '.join(sys.argv[1:])}'''.replace('\n', ' ')

os.system(cmd)
