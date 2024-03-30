#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  1 13:53:05 2023

@author: jfogel
"""


import os
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import sys
import matplotlib.pyplot as plt
import getpass
from scipy.sparse import lil_matrix
from scipy.sparse import coo_matrix
from scipy.sparse import csr_matrix



homedir = os.path.expanduser('~')
if getpass.getuser()=='p13861161':
    if os.name == 'nt':
        homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
        root = homedir + '/labormkt_rafaelpereira/NetworksGit/'
        print("Running on IPEA Windows")
    else:
        root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
elif getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'

sys.path.append(root + 'Code/Modules')
figuredir = root + 'Results/'




# Pull region codes
region_codes = pd.read_csv(root + '/Data/raw/munic_microregion_rm.csv', encoding='latin1')
muni_micro_cw = pd.DataFrame({'code_micro':region_codes.code_micro,'codemun':region_codes.code_munic//10})

# Load Mayara's aggregation of cbo1994 into Occ2s
cbo1994_occ2_cw = pd.read_excel(root + 'Code/MarketPower/valid_cbo94.xlsx', sheet_name='fix_plus_ggregation', engine='openpyxl')
cbo1994_occ2_cw = cbo1994_occ2_cw.drop(columns='cbo942d').rename(columns={'aggregate':'occ2'})

usecols = ['pis', 'emp_31dez', 'grau_instr', 'genero', 'cnpj_raiz', 'codemun']
#'rem_dez_r', 'idade',, 'rem_dez_sm'

nrows=None
dfs = []
# range(1990, 2000)
for year in [1991,1997]:
    cols = usecols.copy()
    print(year)
    if year>2002:
        cols.append('cbo2002')
    else:
        cols.append('cbo1994')
    if year>1993:
        cols.append('idade')
        cols.append('rem_dez_r')
        cols.append('rem_dez_sm')
        cols.append('tipo_vinculo')
    else:
        cols.append('fx_etaria')
        cols.append('rem_dez_sm')
        cols.append('nat_vinculo')
    now = datetime.now()
    currenttime = now.strftime('%H:%M:%S')
    print('Starting ', year, ' at ', currenttime)
    if ((year < 1998) | (year==2016) | (year==2018) | (year==2019)):
        sep = ';'
    else:
        sep = ','
    filename = '~/rais/RAIS/csv/brasil' + str(year) + '.csv'
    raw_data = pd.read_csv(filename, usecols=cols, nrows=nrows, sep=sep)
    raw_data['year'] = year
    raw_data = raw_data.merge(muni_micro_cw, on='codemun', how='left', validate='m:1', indicator=True)
    raw_data['female'] = np.where(raw_data.genero==2, 1, np.where(raw_data.genero==1, 0, np.nan))
    print(raw_data._merge.value_counts())
    # Recode 'idade' into a categorical variable
    bins = [17, 24, 29, 39, 49, 64]
    labels = ['18-24', '25-29', '30-39', '40-49', '50-64']
    if year>1993:
        raw_data = raw_data.loc[(raw_data.idade>=18) & (raw_data.idade<=64)]
        raw_data['age_cat'] = pd.cut(raw_data['idade'], bins=bins, labels=labels, right=True)
    else:
        raw_data = raw_data.loc[(raw_data.fx_etaria>=3) & (raw_data.fx_etaria<=7)]
        raw_data['age_cat'] = raw_data.fx_etaria.astype('category').cat.rename_categories(labels)
    # Mapping from GRAU_INSTR to new English category labels
    education_map = {
        1: 'No formal education',
        2: 'Incomplete primary school',
        3: 'Primary school',
        4: 'Incomplete middle school',
        5: 'Middle school',
        6: 'Incomplete high school',
        7: 'High school',
        8: 'Incomplete college',
        9: 'College', # Combining college, master's, and doctorate
        10: 'College', # Combining college, master's, and doctorate
        11: 'College' # Combining college, master's, and doctorate
    }
    # Convert GRAU_INSTR to new categorical labels
    raw_data['educ_cat'] = raw_data['grau_instr'].map(education_map)
    # Convert the 'Education_Level' column to a categorical type
    raw_data['educ_cat'] = raw_data['educ_cat'].astype('category')
    raw_data.drop(columns=['codemun','_merge','grau_instr','idade','fx_etaria', 'genero'], inplace=True, errors='ignore')
    # XX Need to decide how to filter on tipo_vinculo/nat_vinculo
    # Merge on Mayara's occ2s (stored as the column 'aggregate'
    raw_data = raw_data.merge(cbo1994_occ2_cw, left_on='cbo1994', right_on='cbo94', how='left', validate='m:1', indicator=True)
    # Code firm-market dummies
    raw_data['has_nan'] = raw_data[['cnpj_raiz', 'code_micro', 'occ2']].isnull().any(axis=1)
    raw_data['firm_market_fe'] = raw_data['cnpj_raiz'].astype(str) + '_' + raw_data['code_micro'].astype(str) + '_' + raw_data['occ2'].astype(str)
    raw_data['market_fe'] = raw_data['code_micro'].astype(str) + '_' + raw_data['occ2'].astype(str)
    raw_data.loc[raw_data['has_nan'], 'firm_market_fe'] = np.nan
    raw_data.drop(columns='has_nan', inplace=True)
    dfs.append(raw_data)


df = pd.concat(dfs)    
df.to_pickle(root + "Data/derived/mayara_regression_panel_1991_1997.p")
df = pd.read_pickle(root + "Data/derived/mayara_regression_panel_1991_1997.p")


############################################
# Run the regression to compute wage premia

# Restrict to obs with positive wage
df = df.loc[(df.rem_dez_sm>0) & (df.firm_market_fe.isna()==False)]

# Compute firm-market employment differences
l_zm_t = df.groupby(['firm_market_fe','year'])['rem_dez_sm'].count().reset_index(name='l_zm_t').pivot(index='firm_market_fe', columns='year', values='l_zm_t')
l_zm_t['diff'] = l_zm_t[1997] - l_zm_t[1991]


# Step 2: Calculate total payroll for each market
market_total_payroll      = df.loc[df.year==1991].groupby('market_fe')['rem_dez_sm'].sum().reset_index(name='market_total_payroll')
firm_market_total_payroll = df.loc[df.year==1991].groupby(['firm_market_fe','market_fe'])['rem_dez_sm'].sum().reset_index(name='firm_market_total_payroll')

# Step 3: Merge the total market payroll back to the original DataFrame
merged = pd.merge(firm_market_total_payroll, market_total_payroll, on='market_fe', how='left', validate='m:1', indicator=True)

# Step 4: Calculate the share of each firm's payroll in its market's total payroll
merged['s_zm'] = merged['firm_market_total_payroll'] / merged['market_total_payroll']

s_zm = merged[['s_zm','firm_market_fe']]

# Check that shares always equal 1
merged.groupby('market_fe')['s_zm'].sum().min()
merged.groupby('market_fe')['s_zm'].sum().max()

l_zm_t.loc[l_zm_t[1991].notna()].shape

# Read in Mayara's tariffs data
cnae_avg_tariffs = pd.read_stata(root + 'Code/MarketPower/cnae_avg_tariffs.dta')






XX Next steps:
- figure out what sample restrictions (e.g. number of workers per firm X market) and then figure out how to estiamte all the firm X market FEs
- Try to actually run the regressions


for var in df.columns:
    print(var, df[var].isna().sum())












import pandas as pd
from linearmodels.panel import PanelOLS
from linearmodels.panel import PooledOLS
import statsmodels.api as sm
from linearmodels import PanelOLS

# Assuming df is your DataFrame
# Set the multi-index required for PanelOLS (if not already set)
df = df.set_index(['pis', 'year'])

# Convert 'firm_market_fe' to categorical if it's not already
df['firm_market_fe_cat'] = df['firm_market_fe'].astype('category')

# Ensure the dependent variable is in the correct format (log of December earnings)
df['log_rem_dez_r'] = np.log(df['rem_dez_r'])

# Define independent variables, adding 1 for intercept
exog_vars = ['female'] + [f'age_cat_{i}' for i in range(1, 5)] + [f'educ_cat_{i}' for i in range(1, 9)]
exog = sm.add_constant(df[exog_vars])

# Since 'age_cat' and 'educ_cat' are categorical, get dummies (omit the first category as reference)
exog = pd.get_dummies(exog, columns=['age_cat', 'educ_cat'], drop_first=True)

# Define the model
mod = PanelOLS(df['log_rem_dez_r'], exog, entity_effects=True)

# Fit the model
res = mod.fit(cov_type='clustered', cluster_entity=True)

# Print results
print(res)
