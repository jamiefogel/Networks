import pandas as pd
import os
import numpy as np

# DIRECTORY WHERE THE DATA IS
# Automatically chooses the folder between windows vs linux servers
if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
else:
    homedir = os.path.expanduser('~/labormkt')

# Data folder
os.chdir(homedir + '/labormkt_rafaelpereira/aug2022/dump/')
#os.chdir('/home/bm/Dropbox (University of Michigan)/_papers/Networks/RAIS_exports/job_transitions')


dfs = []

for core in range(1, 30):
    try:
            df = pd.read_csv('job_transitions_results/results_core' + str(core) + '_multiple_mkts.csv')    
            df['core'] = core
            dfs.append(df)
    except FileNotFoundError:
        print(f"Result file from core {core} not found. Skipping...")

combined_df = pd.concat(dfs, ignore_index=True)

combined_df 
np.round(combined_df.iloc[:,1:4].describe(),2)
