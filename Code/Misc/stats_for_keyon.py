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


incomevars = ['salario', 'rem_dez_r', 'rem_med_r', 'vl_rem_01', 'vl_rem_02', 'vl_rem_03', 'vl_rem_04', 'vl_rem_05', 'vl_rem_06', 'vl_rem_07', 'vl_rem_08', 'vl_rem_09', 'vl_rem_10', 'vl_rem_11', 'vl_rem_12']

rais2017 = pd.read_csv('~/rais/RAIS/csv/brasil2017.csv', sep=',', nrows=None, usecols=['pis', 'id_estab', 'cnpj_raiz', 'uf', 'regiao_metro', 'grau_instr','codemun','mun_trab'] + incomevars)

state_counts = rais2017.uf.value_counts()
metro_counts = rais2017.regiao_metro.value_counts()
educ_counts =  rais2017.grau_instr.value_counts()
income_var_sumstats = rais2017[incomevars].describe()
pis_nunique = rais2017.pis.nunique()
cnpj_raiz_nunique = rais2017.cnpj_raiz.nunique()
id_estab_nunique = rais2017.id_estab.nunique()

rais2017_sumstats = {
	'shape': rais2017.shape
	'state_counts':state_counts,
	'metro_counts':metro_counts,
	'educ_counts' :educ_counts,
	'income_var_sumstats' : income_var_sumstats,
	'pis_nunique' :pis_nunique,
	'cnpj_raiz_nunique' :cnpj_raiz_nunique,
	'id_estab_nunique' :id_estab_nunique}

pickle.dump( rais2017_sumstats, open(root + 'Code/Misc/stats_for_keyon.p', "wb" ) )
rais2017_sumstats = pickle.load( open(root + 'Code/Misc/stats_for_keyon.p', "rb" ) )

keyon_stats = pickle.load( open(root + 'Code/Misc/stats_for_jamie_fixed.p', "rb" ))

'''
state_counts.to_csv(root + 'Code/Misc/stats_for_keyon_state_counts.csv')
metro_counts.to_csv(root + 'Code/Misc/stats_for_keyon_metro_counts.csv')
educ_counts.to_csv(root + 'Code/Misc/stats_for_keyon_educ_counts.csv')
income_var_sumstats.to_csv(root + 'Code/Misc/stats_for_keyon_income_var_sumstats.csv')
'''



df_rr = rais2017.loc[rais2017.uf=='RR']
state_counts = df_rr.uf.value_counts()
metro_counts = df_rr.regiao_metro.value_counts()
educ_counts =  df_rr.grau_instr.value_counts()
income_var_sumstats = df_rr[incomevars].describe(include='all').append(df_rr[incomevars].nunique().rename('unique'))
pis_nunique = df_rr.pis.nunique()
cnpj_raiz_nunique = df_rr.cnpj_raiz.nunique()
id_estab_nunique = df_rr.id_estab.nunique()

custom_description = df.describe(include='all').append(df.nunique().rename('unique'))


df_rr_sumstats = {
	'shape': df_rr.shape
	'state_counts':state_counts,
	'metro_counts':metro_counts,
	'educ_counts' :educ_counts,
	'income_var_sumstats' : income_var_sumstats,
	'pis_nunique' :pis_nunique,
	'cnpj_raiz_nunique' :cnpj_raiz_nunique,
	'id_estab_nunique' :id_estab_nunique}


{'shape': (130449, 80),
 'educ_counts':
 7     68095   
 9     34639
 5     11627
 6      5130
 8      3938
 4      2983
 3      1467
 2      1284
 10      680
 1       413
 11      193
 Name: Escolaridade após 2005, dtype: int64,
 'income_var_sumstats':        Vl Salário Contratual  ... Vl Remun Dezembro (SM)
 count                 130449  ...                 130449
 unique                 13954  ...                   2638
 top               0000009,37  ...              000000,00
 freq                   11038  ...                  31969
 
 [4 rows x 15 columns],
 'pis_nunique': 113116,pd.set_option('display.max_columns', None)
 'cnpj_raiz_nunique': 4621}










import pandas as pd
import os
import pickle

# load from RR2017ID.txt
file_path = os.path.expanduser('~/Downloads/RR2017ID.txt')
df_rr = pd.read_csv(file_path, sep=';', encoding='latin-1')

incomevars = ['Vl Salário Contratual', 'Vl Remun Dezembro Nom', 'Vl Remun Média Nom', 'Vl Rem Janeiro CC', 'Vl Rem Fevereiro CC', 'Vl Rem Março CC', 'Vl Rem Abril CC', 'Vl Rem Maio CC', 'Vl Rem Junho CC', 'Vl Rem Julho CC', 'Vl Rem Agosto CC', 'Vl Rem Setembro CC', 'Vl Rem Outubro CC', 'Vl Rem Novembro CC', 'Vl Remun Dezembro (SM)']

# metro_counts = df_rr['Região Metropolitana'].value_counts()
educ_counts =  df_rr['Escolaridade após 2005'].value_counts()
income_var_sumstats = df_rr[incomevars].describe()
pis_nunique = df_rr['PIS'].nunique()
cnpj_raiz_nunique = df_rr['CNPJ Raiz'].nunique()
# id_estab_nunique = df_rr['ID Estab'].nunique()

df_rr_sumstats = {
	'shape': df_rr.shape,
	# 'metro_counts':metro_counts,
	'educ_counts' :educ_counts,
	'income_var_sumstats' : income_var_sumstats,
	'pis_nunique' :pis_nunique,
	'cnpj_raiz_nunique' :cnpj_raiz_nunique,
	# 'id_estab_nunique' :id_estab_nunique
}

pickle.dump(df_rr_sumstats, open(os.path.expanduser('~/Downloads/stats_for_jamie.p'), "wb" ))
