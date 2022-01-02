import os
import sys
import socket
import shutil


"""
for dirpath in list(os.walk('/home/ais_env/aisdb')):
    for fname in dirpath[2]:
        fpath = os.path.join(dirpath[0], fname)
        print(fpath)
        shutil.chown(fpath, 'ais_env')
        os.chown(fpath, uid=1000, gid=1000)
"""

'''
# give user permission to modify src
dirnames, dirpath, fnames = 
test = list(os.walk('/home/ais_env/aisdb'))
for dirname in dirnames[1]:
    #print(os.path.join(dirnames[0], dirname))
    shutil.chown(os.path.join(dirnames[0], dirname), uid='ais_env', gid='ais_env')
#os.chown('/home/ais_env/ais/tmp_parsing', uid=1000)
'''


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

