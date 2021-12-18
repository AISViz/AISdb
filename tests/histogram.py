import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from aisdb import output_dir

fname =  os.path.join(output_dir, "output_cutdistance=5000m_maxdistance=200000m_cuttime=24hrs.csv")
fname2 = os.path.join(output_dir, 'testoutput__50knots_100000km_cuttime=28hrs.csv')

df = pd.read_csv(fname)
df2 = pd.read_csv(fname2)

df.dropna(subset=['rcv_zone'], inplace=True)

rcv_zone_values = df['rcv_zone'].values
rcv_zone_values2 = df2['rcv_zone'].values

bins = np.linspace(0, 17, 18)
plt.hist(rcv_zone_values, bins=bins, color='red', alpha=0.5)
plt.hist(rcv_zone_values2, bins=bins, color='blue', alpha=0.5)

plt.xlim(0, 17)
plt.xlabel('Zone')
plt.ylabel('No. crossings into zone')
plt.yscale('log')
plt.title(fname + '\n' + fname2)

plt.show()
