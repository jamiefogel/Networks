'''
Adapted from NetworksGit/Code/predicting_flows_data_pull.py

'''

from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import graph_tool.all as gt
import scipy.sparse as sp
import copy
import sys
import geobr
import cividis
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from haversine import haversine, Unit
from shapely.geometry import MultiPoint
from scipy.stats import mstats
from binsreg import binsregselect, binsreg, binsqreg, binsglm, binstest, binspwc
from sklearn.linear_model import LinearRegression

homedir = os.path.expanduser('~')
root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
sys.path.append(root + 'Code/Modules')
os.chdir(root)

import bisbm
from create_df_trans import create_df_trans
from create_unipartite_adjacency_and_degrees import create_unipartite_adjacency_and_degrees
from pull_one_year import pull_one_year




def binned_scatter_plot(X, y, num_bins, plot_actual_data=False, add_linear_fit=False):
    # Create bins for the independent variable
    X_binned = pd.qcut(X, q=num_bins, duplicates='drop') #, labels=[f'bin_{i}' for i in range(num_bins)]
    # Create a DataFrame with the binned data
    data = pd.DataFrame({'X': X, 'X_binned': X_binned, 'y': y})
    # Create a figure and axes
    fig, ax = plt.subplots()
    # Plot the actual data points
    if plot_actual_data:
        ax.scatter(X, y, label='Actual data', alpha=0.5)
    # Plot mean for each bin as circles
    bin_centers = data.groupby('X_binned').mean()['X']
    bin_means = data.groupby('X_binned').mean()['y']
    ax.scatter(bin_centers, bin_means, color='red', marker='o', label='Bin Mean')
    ax.set_xlabel('X')
    ax.set_ylabel('y')
    ax.legend()
    return fig, ax



#states = ['SP','RJ','MG']
states = ['MG','RJ','SP']
state_codes_rais = [31,33,35]
firstyear = 2013
lastyear = 2016
nrows = None
vars = ['pis','id_estab', 'cbo2002','codemun','tipo_vinculo','idade','data_adm','data_deslig','rem_med_r','salario', 'clas_cnae20', 'grau_instr'] 


geocodes = pd.read_parquet(root + 'Data/raw/geocodes/rais_geo_2013_to_2016.parquet')
geocodes = geocodes.loc[geocodes.uf.isin(states)]
geocodes.drop(columns=['name_muni','uf','code_muni','cep'], inplace=True)
geocodes.rename(columns={'ano':'year'}, inplace=True)
geocodes['id_estab'] = geocodes['id_estab'].astype(int).astype(str).str.zfill(14)
# Drop a small number of duplicates, aribtrarily choosing the first
geocodes = geocodes.drop_duplicates(subset=['id_estab', 'year'], keep='first')

# CPI: 06/2015=100
cpi = pd.read_csv('./Data/raw/BRACPIALLMINMEI.csv', parse_dates=['date'], names=['date','cpi'], header=0)
cpi['cpi'] = cpi.cpi/100
cpi['date'] = cpi['date'].dt.to_period('M')


munis = geobr.read_municipality(code_muni="all", year=2013)
munis['lon'] = munis.geometry.centroid.x
munis['lat'] = munis.geometry.centroid.y
munis['codemun'] = munis.code_muni//10
munis = munis.loc[munis.code_muni!=4300002] # In RAIS we have 6-digit municipality codes but geobr has 7-digit. We convert geobr codes to 6-digit but there is one non-unique 6-digit. Arbitrarily breaking that tie here.

sbm_modelname = '3states_2013to2016_mcmc'
gammas = pd.read_csv('./Data/derived/sbm_output/model_'+sbm_modelname+'_jblocks.csv', usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
gammas['gamma'] = gammas.gamma.fillna(-1)
iotas = pd.read_csv('./Data/derived/sbm_output/model_'+sbm_modelname+'_wblocks.csv', usecols=['wid','worker_blocks_level_0'], dtype={'wid': object}).rename(columns={'worker_blocks_level_0':'iota'})
iotas['iota'] = iotas.iota.fillna(-1)



########################################################################################
########################################################################################
# Create data frame of job-to-job transitions
########################################################################################
########################################################################################
 
raw_dfs = {}
for year in range(firstyear, lastyear+1):
    # Some years are semicolon delimited, others comma
    if ((year < 1998) | (year==2016) | (year==2018) | (year==2019)):
        sep = ';'
    else:
        sep = ','
    raw = pd.read_csv('~/rais/RAIS/csv/brasil' + str(year) + '.csv', usecols=vars, sep=sep, dtype={'id_estab':str, 'pis':str, 'cbo2002':str}, nrows=nrows, parse_dates=['data_adm','data_deslig'])
    raw = raw[raw['codemun'].fillna(99).astype(str).str[:2].astype('int').isin(state_codes_rais)]
    #df['id_column'] = df['id_column'].astype(str).str.zfill(14)
    # Because dates aren't stored correctly in some years. Also we had a very small number of invalid dates (5 out of hundreds of millions) and this sets them to missing rather than failing.
    raw['start_date'] = pd.to_datetime(raw['data_adm'], errors='coerce')
    raw['end_date'] = pd.to_datetime(raw['data_deslig'], errors='coerce')
    raw.drop(columns=['data_adm','data_deslig'], inplace=True)
    raw['jid'] = raw['id_estab'] + '_' + raw['cbo2002']
    raw.rename(columns={'pis':'wid'}, inplace=True)
    raw['year'] = int(year)
    raw_dfs[year] = raw

    
df = pd.concat(raw_dfs, axis=0)
#df = df.merge(munis[['codemun','lat','lon']], on='codemun', how='left', validate='m:1', indicator=True)
df = df.merge(geocodes[['id_estab','year','lat','lon']], on=['id_estab','year'], how='left', validate='m:1', indicator='_merge')
df._merge.value_counts()
df.drop(columns='_merge', inplace=True)

df = df.merge(gammas, on='jid', how='left', validate='m:1')
df['gamma'] = df.gamma.fillna(-1)
df = df.merge(iotas, on='wid', how='left', validate='m:1')
df['iota'] = df.iota.fillna(-1)

# XX new code
df = df.sort_values(by=['wid','start_date'])
df['jid_prev'] = df.groupby('wid')['jid'].shift(1)
df['jid_next'] = df.groupby('wid')['jid'].shift(-1)
# Flag rows where job changes
# Flag for the new job (job ID changes from the previous and previous is not missing)
df['flag_new_job'] = (df['jid'] != df['jid_prev']) & df['jid_prev'].notna()
# Flag for the old job (job ID changes to the next and next is not missing)
df['flag_old_job'] = (df['jid'] != df['jid_next']) & df['jid_next'].notna()
df['new_job_ever'] = df.groupby('wid')['flag_new_job'].transform('any')

df['latlon'] = list(zip(df['lat'], df['lon']))
df.drop(columns=['lat','lon'], inplace=True)
df.reset_index(drop=True, inplace=True)

# Create income deciles
df['rem_med_r_decile'] = pd.qcut(df['rem_med_r'], 10, labels=False)

df_trans = df.loc[(df.flag_new_job==True) | (df.flag_old_job==True)]

# Need to define reshapvars (which will be the vars specific to a job) and then keep only rows where one of the flags is true and then reshape. I think the way to do the reshape will actually just be to use the shift function b/c sometimes a row willbe both a new job and an old job, which will screw up trying to do a standard reshape
for var in ['gamma', 'codemun','tipo_vinculo','idade','rem_med_r', 'rem_med_r_decile', 'salario', 'clas_cnae20', 'latlon']:
    df_trans[var +'_next'] = df.groupby('wid')[var].shift(-1)

# Keep one row per transition
df_trans = df_trans.loc[df_trans.flag_old_job==True]
df_trans['move_distance'] = df_trans.apply(lambda row: haversine(row['latlon'], row['latlon_next'], unit=Unit.KILOMETERS), axis=1)
df_trans['salario_change'] = df_trans['salario_next'] - df_trans['salario']

# XX The main thing I havent dealt with here is when someone holds multiple jobs at the same time


df.to_parquet(      root + 'Data/derived/transitions_for_rafa_df.parquet')
df_trans.to_parquet(root + 'Data/derived/transitions_for_rafa_df_trans.parquet')

df       = pd.read_parquet(root + 'Data/derived/transitions_for_rafa_df.parquet')
df_trans = pd.read_parquet(root + 'Data/derived/transitions_for_rafa_df_trans.parquet')


##########################################################################################
# Summary stats

df['new_job_ever'] = pd.to_numeric(df['new_job_ever'], errors='coerce')

# Collapse the data set by person
df_person = pd.DataFrame(df.groupby('wid')['flag_new_job'].sum())
df_person = df_person.merge( df.groupby('wid')[['grau_instr','rem_med_r_decile']].max(), on='wid', how='inner',validate='1:1')
df_person = df_person.merge( df.groupby('wid')[['new_job_ever']].max(), on='wid', how='inner',validate='1:1')

df_trans['change_city'] = (df_trans['codemun']!=df_trans['codemun_next']) & (df_trans['codemun'].notna())

#########
# Change city rates and move distances by education
moves_by_educ = pd.merge(df_trans.groupby('grau_instr')[['change_city','move_distance']].mean(),df_trans.groupby('grau_instr')['change_city'].size()/len(df_trans) , left_index=True, right_index=True).rename(columns={'change_city_x':'Change City Rate','change_city_y':'Share of Movers', 'move_distance':'Average Move Distance (km)'})
# Fraction of people who changed jobs by education
moves_by_educ = moves_by_educ.merge( df_person.groupby('grau_instr')['new_job_ever'].mean(), on='grau_instr')
# Average number of new jobs per person by education
moves_by_educ = moves_by_educ.merge( df_person.groupby('grau_instr')['flag_new_job'].mean(), on='grau_instr')
moves_by_educ.rename(columns={'new_job_ever':'Frac Ever Change Job','flag_new_job':'Avg. Num Job Changes (incl. 0)'}, inplace=True)

# Compute the 5th and 95th percentiles of move distance for each education group
move_distance_percentiles = df_trans.groupby('grau_instr')['move_distance'].quantile([0.05, 0.75, 0.95, 0.99]).unstack()
move_distance_percentiles.columns = ['Move Distance P5 (km)', 'Move Distance P75 (km)', 'Move Distance P95 (km)', 'Move Distance P99 (km)']
moves_by_educ = moves_by_educ.merge(move_distance_percentiles, left_index=True, right_index=True)




#########
# Change city rates and move distances by income decile
moves_by_income_decile = pd.merge(df_trans.groupby('rem_med_r_decile')[['change_city','move_distance']].mean(),df_trans.groupby('rem_med_r_decile')['change_city'].size()/len(df_trans) , left_index=True, right_index=True).rename(columns={'change_city_x':'Change City Rate','change_city_y':'Share of Movers', 'move_distance':'Average Move Distance (km)'})
# Fraction of people who changed jobs by income decile
moves_by_income_decile = moves_by_income_decile.merge( df_person.groupby('rem_med_r_decile')['new_job_ever'].mean(), on='rem_med_r_decile')
# Average number of new jobs per person by income decile
moves_by_income_decile = moves_by_income_decile.merge( df_person.groupby('rem_med_r_decile')['flag_new_job'].mean(), on='rem_med_r_decile')
moves_by_income_decile.rename(columns={'new_job_ever':'Frac Ever Change Job','flag_new_job':'Avg. Num Job Changes (incl. 0)'}, inplace=True)

# Compute the 5th and 95th percentiles of move distance for each education group
move_distance_percentiles = df_trans.groupby('rem_med_r_decile')['move_distance'].quantile([0.05, 0.75, 0.95, 0.99]).unstack()
move_distance_percentiles.columns = ['Move Distance P5 (km)', 'Move Distance P75 (km)', 'Move Distance P95 (km)', 'Move Distance P99 (km)']
moves_by_income_decile = moves_by_income_decile.merge(move_distance_percentiles, left_index=True, right_index=True)






# Fraction of people who ever changed jobs
change_job_rate = df_person['new_job_ever'].mean()

# Average number of job changes per person
jobs_per_person = df_person['flag_new_job'].mean()

# Fraction of moves that change cities
frac_moves_change_city = df_trans.change_city.mean()

# Average move distance 
move_distance = df_trans['move_distance'].mean()

# Average move distance by whether person changed city
move_distance_by_change_city = df_trans.groupby('change_city')['move_distance'].mean()


print(change_job_rate)
print(jobs_per_person)
print(frac_moves_change_city)
print(move_distance)
print(move_distance_by_change_city)
print(moves_by_educ)
print(moves_by_income_decile)      

results_dict ={
    'change_job_rate'              :change_job_rate,
    'jobs_per_person'              :jobs_per_person,
    'frac_moves_change_city'       :frac_moves_change_city,
    'move_distance'                :move_distance,
    'move_distance_by_change_city' :move_distance_by_change_city,
    'moves_by_educ'                :moves_by_educ,
    'moves_by_income_decile'       :moves_by_income_decile}
pickle.dump(results_dict, open(root + 'Results/for_rafa/results_dict.p', 'wb'))

# Next steal some stuff from gamma_summary_stats:
# - Plot move distance against education level

for xvar in ['salario_change', 'salario', 'rem_med_r']: #, 'grau_instr'
    print(xvar)
    fig, ax = binned_scatter_plot(df_trans[xvar], df_trans['move_distance'], 20, plot_actual_data=False, add_linear_fit=True)
    ax.set_xlabel(xvar)
    ax.set_ylabel('Move Distance')
    # Save the plot to a PDF file                                                                 
    fig.savefig(root + 'Results/for_rafa/binsreg__move_distance__' + xvar + '.pdf', format='pdf')        


# Restricting to bachelor's degrees only
fig, ax = binned_scatter_plot(df_trans.loc[df_trans.grau_instr==9,'salario_change'], df_trans.loc[df_trans.grau_instr==9,'move_distance'], 19, plot_actual_data=False, add_linear_fit=True)
ax.set_xlabel('Salary Change')
ax.set_ylabel('Move Distance')
ax.set_title('College graduates')
# Save the plot to a PDF file                                                                 
fig.savefig(root + 'Results/for_rafa/binsreg__move_distance__salario_change_college.pdf', format='pdf')        
    
fig, ax = binned_scatter_plot(df_trans.loc[df_trans.grau_instr==9,'move_distance'], df_trans.loc[df_trans.grau_instr==9,'salario_change'], 20, plot_actual_data=False, add_linear_fit=True)
ax.set_xlabel('Move Distance')
ax.set_ylabel('Salary Change')
ax.set_title('High School Grads')
# Save the plot to a PDF file                                                                 
fig.savefig(root + 'Results/for_rafa/binsreg__salario_change__move_distance_college.pdf', format='pdf')        
    


# Restricting to high school diploma only
fig, ax = binned_scatter_plot(df_trans.loc[df_trans.grau_instr==7,'salario_change'], df_trans.loc[df_trans.grau_instr==7,'move_distance'], 20, plot_actual_data=False, add_linear_fit=True)
ax.set_xlabel('Salary Change')
ax.set_ylabel('Move Distance')
ax.set_title('High School Grads')
# Save the plot to a PDF file                                                                 
fig.savefig(root + 'Results/for_rafa/binsreg__move_distance__salario_change_hs.pdf', format='pdf')        
    
fig, ax = binned_scatter_plot(df_trans.loc[df_trans.grau_instr==7,'move_distance'], df_trans.loc[df_trans.grau_instr==7,'salario_change'], 20, plot_actual_data=False, add_linear_fit=True)
ax.set_xlabel('Move Distance')
ax.set_ylabel('Salary Change')
ax.set_title('High School Grads')
# Save the plot to a PDF file                                                                 
fig.savefig(root + 'Results/for_rafa/binsreg__salario_change__move_distance_hs.pdf', format='pdf')        
    
    
# Without actual data points


# To do:
# - Scatter plots where each dot is either a market or an occupation or an industry and we are plotting e.g. move distance against education or mean income or something else.
# - Convert grau_instr to something more meaningful like years of education or a categorical with 4 categories. 
