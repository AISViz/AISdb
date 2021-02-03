import os
import sys
import subprocess

#os.system('''
#        python -m pip install \
#                pyais \
#''')

subprocess.run(f'''{sys.executable} -m pip install pyais''', shell=True)

