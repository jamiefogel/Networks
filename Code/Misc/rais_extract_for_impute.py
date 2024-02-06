import os
import sys
import pandas as pd
import torch
import numpy as np
import pickle
from datetime import datetime
import getpass

homedir = os.path.expanduser('~')
if getpass.getuser()=='p13861161':
    print("Running on Linux")
    root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
elif getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'

sys.path.append(root + 'Code/Modules')
figuredir = root + 'Results/'

os.chdir(root)

state_codes = [31, 33, 35]

incomevars = ['salario', 'rem_dez_r', 'rem_med_r', 'vl_rem_01', 'vl_rem_02', 'vl_rem_03', 'vl_rem_04', 'vl_rem_05', 'vl_rem_06', 'vl_rem_07', 'vl_rem_08', 'vl_rem_09', 'vl_rem_10', 'vl_rem_11', 'vl_rem_12']

usecols=['pis', 'id_estab', 'cnpj_raiz', 'uf', 'grau_instr','codemun','cbo2002','clas_cnae20','genero','grau_instr','idade','nacionalidad','raca_cor','salario','tipo_vinculo']  # + incomevars For now, the monthly incme vars probably just use up disk space and don't add much because in Census we will at best have quarterly earnings through LEHD

nrows = 10000
dfs = []
# Starting with 2006 because it's the first year we have clas_cna20
for year in range(2006,2022):
    print(year)
    now = datetime.now()
    currenttime = now.strftime('%H:%M:%S')
    print('Starting ', year, ' at ', currenttime)
    if ((year < 1998) | (year==2016) | (year==2018) | (year==2019) | (year==2020) | (year==2021) ):
        sep = ';'
    else:
        sep = ','
    filename = '~/rais/RAIS/csv/brasil' + str(year) + '.csv'
    raw_data = pd.read_csv(filename, usecols=usecols, sep=sep, nrows=nrows)
    # Drop public sector and some other types of employment (ask Bernardo exactly why we did this)
    raw_data = raw_data[~raw_data['tipo_vinculo'].isin([30,31,35])]
    # Restrict to a few states to minimize informality and reduce sample size somewhat
    raw_data = raw_data[raw_data['codemun'].fillna(99).astype(str).str[:2].astype('int').isin(state_codes)]
    raw_data['year'] = year
    raw_data['yob'] = raw_data['year'] - raw_data['idade']
    dfs.append(raw_data)

df = pd.concat( dfs, ignore_index=True)
