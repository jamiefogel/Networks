from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import sys
import gc
import matplotlib.pyplot as plt
import scipy.stats as stats
import geobr
import cividis
import matplotlib.colors as colors
from haversine import haversine, Unit
from shapely.geometry import MultiPoint
from scipy.stats import mstats

# DIRECTORY WHERE THE DATA IS
# Automatically chooses the folder between windows vs linux servers
if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
else:
    homedir = os.path.expanduser('~/labormkt')


root = homedir + '/labormkt_rafaelpereira/NetworksGit/'
sys.path.append(root + 'Code/Modules')
os.chdir(root)

import bisbm
from pull_one_year import pull_one_year
from explore_gammas_functions import *
from binscatter import *

state_codes = [31, 33, 35]

region_codes = pd.read_csv(root + '/Data/raw/munic_microregion_rm.csv', encoding='latin1')
region_codes = region_codes.loc[region_codes.code_uf.isin(state_codes)]
state_cw = region_codes[['code_meso','uf']].drop_duplicates()
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})
region_codes['codemun'] = region_codes.code_munic//10


# 2004 appears to be the first year in which we have job start end dates (data_adm and data_deslig)


# Load iotas and gammas
                           
jcolumns = ['jid']     
wcolumns = ['wid']
jrename = {}
wrename = {}
for l in range(0,1):
    jcolumns = jcolumns + ['job_blocks_level_'+str(l)]
    jrename['job_blocks_level_'+str(l)] = 'gamma'
    wcolumns = wcolumns + ['worker_blocks_level_'+str(l)]
    wrename['worker_blocks_level_'+str(l)] = 'iota'

#modelname = '3states_2009to2011_mcmc'
modelname = '3states_2009to2011'

gammas = pd.read_csv(root + '/Data/derived/sbm_output/model_' + modelname + '_jblocks.csv', usecols=jcolumns).rename(columns=jrename)
iotas  = pd.read_csv(root + '/Data/derived/sbm_output/model_' + modelname + '_wblocks.csv', usecols=wcolumns).rename(columns=wrename)
iotas['wid'] = iotas['wid'].astype('str')
#model = pickle.load(open(root + '/Data/derived/sbm_output/model_'+modelname+'.p', "rb" ))


                           
################################################
# Pull other variables like education for a specific year
# - These are variables that we want to merge on by wid-jid but then collapse by gamma to better characterize the different gammas


run_sbm = False
run_sbm_mcmc = False
run_pull=False
run_append = False
run_create_earnings_panel = False
maxrows=None
modelname = '3states_2009to2011'
filename_stub = "panel_"+modelname
rais_filename_stub =  '~/rais/RAIS/csv/brasil' 
#rais_filename_stub = root + './Data/raw/synthetic_data_'

firstyear_sbm = 2009
lastyear_sbm  = 2011
firstyear_panel = 2009
lastyear_panel  = 2014
state_codes = [31, 33, 35]
gamma_summary_stats_year = 2010   # Define a year to compute summary stats


raw = pull_one_year(gamma_summary_stats_year, 'cbo2002', othervars=['grau_instr','rem_med_r','clas_cnae20','codemun'], state_codes=state_codes, age_lower=25, age_upper=55, nrows=None, filename=rais_filename_stub + str(gamma_summary_stats_year) + '.csv')

raw = raw.merge(gammas, how='left', validate='m:1', on='jid',indicator=False)
raw = raw.merge(iotas, how='left', validate='m:1', on='wid',indicator=False)

raw = raw.merge(muni_meso_cw, on='codemun')

raw['occ2'] = raw['cbo2002'].str[0:2]
raw['occ4'] = raw['cbo2002'].str[0:4]
raw['ind2'] = raw['clas_cnae20'].astype('str').str[0:2].astype('int')
raw['sector_IBGE'] = np.nan
raw['sector_IBGE'].loc[( 1<=raw['ind2']) & (raw['ind2'] <= 3)] = 1  
raw['sector_IBGE'].loc[( 5<=raw['ind2']) & (raw['ind2'] <= 9)] = 2 
raw['sector_IBGE'].loc[(10<=raw['ind2']) & (raw['ind2'] <=33)] = 3 
raw['sector_IBGE'].loc[(35<=raw['ind2']) & (raw['ind2'] <=39)] = 4 
raw['sector_IBGE'].loc[(41<=raw['ind2']) & (raw['ind2'] <=43)] = 5 
raw['sector_IBGE'].loc[(45<=raw['ind2']) & (raw['ind2'] <=47)] = 6 
raw['sector_IBGE'].loc[(49<=raw['ind2']) & (raw['ind2'] <=53)] = 7 
raw['sector_IBGE'].loc[(55<=raw['ind2']) & (raw['ind2'] <=56)] = 8 
raw['sector_IBGE'].loc[(58<=raw['ind2']) & (raw['ind2'] <=63)] = 9 
raw['sector_IBGE'].loc[(64<=raw['ind2']) & (raw['ind2'] <=66)] = 10
raw['sector_IBGE'].loc[(68<=raw['ind2']) & (raw['ind2'] <=68)] = 11
raw['sector_IBGE'].loc[(69<=raw['ind2']) & (raw['ind2'] <=82)] = 12
raw['sector_IBGE'].loc[(84<=raw['ind2']) & (raw['ind2'] <=84)] = 13
raw['sector_IBGE'].loc[(85<=raw['ind2']) & (raw['ind2'] <=88)] = 14
raw['sector_IBGE'].loc[(90<=raw['ind2']) & (raw['ind2'] <=97)] = 15


# Recode education variable to approximately reflect years of schooling
raw['grau_instr'] = raw['grau_instr'].replace({1:1, 2:3, 3:5, 4:7, 5:9, 6:10, 7:12, 8:14, 9:16, 10:18, 11:21})

raw.to_pickle(root + '/Data/derived/mkt_geography_raw.p')
#raw = pd.read_pickle(root + '/Data/derived/mkt_geography_raw.p')




# Collapse a bunch of the variables in raw by gamma
gammas_w_attributes = raw.groupby(['gamma']).agg(educ_median=('grau_instr','median'), educ_mean=('grau_instr','mean'), educ_mode=('grau_instr',lambda x: stats.mode(x)[0][0]), mean_monthly_earnings=('rem_med_r','mean'),modal_ind2=('ind2', lambda x: stats.mode(x)[0][0]), modal_sector=('sector_IBGE', lambda x: stats.mode(x)[0][0]), modal_occ2=('occ2', lambda x: stats.mode(x)[0][0]), modal_occ4=('occ4', lambda x: stats.mode(x)[0][0])).reset_index()

# Create some rank variables 
gammas_w_attributes['educ_mean_rank'] = gammas_w_attributes['educ_mean'].rank(method='dense', pct=True)
gammas_w_attributes['mean_monthly_earnings_rank'] = gammas_w_attributes['mean_monthly_earnings'].rank(method='dense', pct=True)
gammas_w_attributes['log_mean_monthly_earnings'] = np.log(gammas_w_attributes['mean_monthly_earnings'])

# Compute the number of unique workers and jobs and worker--job pairs 
num_unique_jids = raw.groupby('gamma')['jid'].nunique().reset_index().rename(columns={'jid':'num_unique_jids'})
num_unique_wids = raw.groupby('gamma')['wid'].nunique().reset_index().rename(columns={'wid':'num_unique_wids'})
num_unique_wid_jids = raw.groupby('gamma').apply(lambda x: x[['jid', 'wid']].drop_duplicates().shape[0]).astype(int).reset_index().rename(columns={0:'num_unique_wid_jids'})


######################################
# Compute HHIs

hhi_iota = gamma_hhi(raw,'gamma','iota')
hhi_occ4 = gamma_hhi(raw,'gamma','occ4')
hhi_occ2 = gamma_hhi(raw,'gamma','occ2')
hhi_code_meso = gamma_hhi(raw,'gamma','code_meso')
hhi_codemun = gamma_hhi(raw,'gamma','codemun')
hhi_jid = gamma_hhi(raw,'gamma','jid')


# Merge on gamma characteristics 
for var in [hhi_iota, hhi_occ4, hhi_occ2, hhi_code_meso, hhi_codemun, hhi_jid, num_unique_jids, num_unique_wids, num_unique_wid_jids]:
    gammas_w_attributes = gammas_w_attributes.merge(var, on='gamma', validate='1:1')


#############################################################################################
# Job-to-job Move distances from the predicting flows transitions data

# Pull meso codes for our states of interest. Choosing 2010 b/c this isn't available for all years and 2010 is in the middle of our sample
muni_sp = geobr.read_municipality(code_muni="SP", year=2010)
muni_rj = geobr.read_municipality(code_muni='RJ', year=2010)
muni_mg = geobr.read_municipality(code_muni='MG', year=2010)
munis = pd.concat([muni_sp, muni_rj, muni_mg], ignore_index=True)
munis['lon'] = munis.geometry.centroid.x
munis['lat'] = munis.geometry.centroid.y
munis['codemun'] = munis.code_muni//10

df_trans = pd.read_pickle(root + '/Data/derived/predicting_flows/' + modelname + 'pred_flows_df_trans_ins.p')

# Assign each jid its modal municipality code based on 2016 data
jid_muni = raw[['jid','codemun']].groupby('jid').agg(lambda x: x.mode()[0]).reset_index()
jid_muni = jid_muni.merge(munis[['codemun','lat','lon']], on='codemun', how='inner', validate='m:1', indicator=True)

distances = df_trans[['jid_prev','jid','gamma_prev','gamma']].loc[(df_trans.jid_prev.notnull()) & (df_trans.gamma_prev.notnull())].drop_duplicates()
distances = distances.merge(jid_muni.rename(columns={'jid':'jid_prev','lat':'lat_prev','lon':'lon_prev'}), on='jid_prev', validate='m:1')
distances = distances.merge(jid_muni, left_on='jid', right_on='jid', validate='m:1')

# Combine lat_prev and lon columns into a single column of (lat, lon) tuples
distances['point'] = list(zip(distances['lat'], distances['lon']))
distances['point_prev'] = list(zip(distances['lat_prev'], distances['lon_prev']))

# Calculate the haversine distance between each pair of points
distances['distance'] = distances.apply(lambda row: haversine(row['point'], row['point_prev'], unit=Unit.KILOMETERS), axis=1)

distance_stats = distances.groupby('gamma')['distance'].agg(['mean', 'median', 'max', 'min', 'std', lambda x: x.quantile(0.25), lambda x: x.quantile(0.75)])
distance_stats = distance_stats.rename(columns={
    'mean': 'j2j_dist_mean',
    'median': 'j2j_dist_median',
    'max': 'j2j_dist_max',
    'min': 'j2j_dist_min',
    'std': 'j2j_dist_std',
    '<lambda_0>': 'j2j_dist_25th',
    '<lambda_1>': 'j2j_dist_75th'
})


gammas_w_attributes = gammas_w_attributes.merge(distance_stats, left_on='gamma', right_on='gamma', validate='1:1')


#############################################################################################
# Gamma spatial variances


# Spatial variance

jid_spatial_dist = raw[['jid','gamma']].merge(jid_muni, left_on='jid', right_on='jid', validate='m:1')
jid_spatial_dist['mean_lat'] = jid_spatial_dist.groupby('gamma')['lat'].transform('mean')
jid_spatial_dist['mean_lon'] = jid_spatial_dist.groupby('gamma')['lon'].transform('mean')

jid_spatial_dist['distance'] = jid_spatial_dist.apply(compute_distance, axis=1)
spatial_var_km = jid_spatial_dist.groupby('gamma')['distance'].mean().reset_index().rename(columns={'distance':'spatial_var_km'})

gammas_w_attributes = gammas_w_attributes.merge(spatial_var_km, left_on='gamma', right_on='gamma', how='left', validate='1:1')


gammas_w_attributes['spatial_var_km_rank'] = gammas_w_attributes['spatial_var_km'].rank(method='dense', pct=True)
gammas_w_attributes['j2j_dist_mean_rank'] = gammas_w_attributes['j2j_dist_mean'].rank(method='dense', pct=True)


gammas_w_attributes.to_pickle(root + '/Data/derived/explore_gammas_gammas_w_attributes.p')
gammas_w_attributes = pd.read_pickle(root + '/Data/derived/explore_gammas_gammas_w_attributes.p')

                                                                                           


###########################################################
# Compute the distribution of meso regions for each gamma


# Pull meso codes for our states of interest
meso_sp = geobr.read_meso_region(code_meso="SP", year=2016)
meso_rj = geobr.read_meso_region(code_meso='RJ', year=2016)
meso_mg = geobr.read_meso_region(code_meso='MG', year=2016)
mesos = pd.concat([meso_sp, meso_rj, meso_mg], ignore_index=True)


# Calculate share of jid observations for each gamma in each code_meso
pivot_df = pd.pivot_table(raw.loc[raw.gamma!=-1], index='code_meso', columns='gamma', aggfunc='size', fill_value=0)
meso_share_df = pivot_df.apply(lambda x: x/x.sum(), axis=0).reset_index()

# Issue: the meso codes from geobr only have 4 digits but in state_cw they sometimes have 5. Basically in state_cw there is always a 0 between the 2 state digits and the 2 meso code digits; in geo_br there is only a 0 if the meso code has 1 digit. Check with Bernardo about this
# For details, try: state_cw.loc[state_cw.uf=='São Paulo']
# Remove the midde 0 from code_meso to be consistent with the meso codes downloaded by geobr
num_str = meso_share_df['code_meso'].astype(str)
num_str.loc[num_str.str.len() == 5] = num_str.str[:2] + num_str.str[3:]
meso_share_df['code_meso'] = num_str.astype(int)

# Create an alternative meso share that is normalized by the meso's share of total employment. Values >1 will indicate that the gamma is overrepresented in the meso relative to population, <0 implies underrepresented.
meso_emp_share = pivot_df.sum(axis=1)/pivot_df.sum(axis=1).sum()
meso_share_norm_df = pd.concat([meso_share_df.iloc[:,0], meso_share_df.iloc[:,1:].div(meso_emp_share.values, axis=0)],axis=1)

# I think what I want is for each gamma I want the fraction of jobs in that gamma that are in a particular meso. Therefore, if I sum across code_mesos in a gamma the sum should be 1

# join the databases
meso_share_df      = mesos.merge(meso_share_df, how="left", on="code_meso")
meso_share_norm_df = mesos.merge(meso_share_norm_df, how="left", on="code_meso")


pickle.dump( meso_share_df,      open(root + '/Data/derived/dump/' + modelname + '_meso_share_df.p', "wb" ) )
pickle.dump( meso_share_norm_df, open(root + '/Data/derived/dump/' + modelname + '_meso_share_norm_df.p', "wb" ) )







##############################################################################################################################
# Compute occupation distributions for each iota and gamma in 2016

from occ_counts_by_type import occ_counts_by_type
[iota_dict, gamma_dict] = occ_counts_by_type(raw, root + 'Data/raw/translated_occ_codes_english_only.csv', 0)
                          

pickle.dump( iota_dict,          open(root + '/Data/derived/dump/' + modelname + '_iota_dict.p', "wb" ) )
pickle.dump( gamma_dict,         open(root + '/Data/derived/dump/' + modelname + '_gamma_dict.p', "wb" ) )

# Distribution of other variables by gamma

translated_occ_codes = pd.read_csv(root + 'Data/raw/translated_occ_codes_english_only.csv', names=['cbo2002','occupation_name'], header=0, dtype={'cbo2002':'str'})
translated_ind_codes = pd.read_csv(root + 'Data/raw/translated_clas_cnae20.csv', names=['clas_cnae20','industry_name'], header=0)



raw_temp = raw.rename(columns={'grau_instr':'education'})
raw_temp = raw_temp.merge(translated_occ_codes, on='cbo2002')
raw_temp = raw_temp.merge(translated_ind_codes, on='clas_cnae20') # ,validate='m:1',indicator='_merge'
raw_temp = raw_temp.merge(region_codes[['codemun','meso','micro','munic','uf']], on='codemun')
raw_temp['municipality'] = raw_temp['munic'] + ', ' + raw_temp['uf']


for var in ['education','industry_name','occupation_name','municipality']:
    dict = {gamma: sub_df['var'].value_counts() for gamma, sub_df in raw_temp.groupby('gamma')}
    pickle.dump(dict,         open(root + '/Data/derived/dump/' + modelname + '_gamma_' + var + '_dict.p', "wb" ) )





    
    
gamma_educ_dict = {gamma: sub_df['educ'].value_counts() for gamma, sub_df in raw.rename(columns={'grau_instr':'educ'}).groupby('gamma')}

gamma_industry_dict = {gamma: sub_df['clas_cnae20'].value_counts() for gamma, sub_df in raw.groupby('gamma')}


raw_temp = raw_temp.merge(region_codes[['codemun','meso','micro','munic','uf']], on='codemun')
raw_temp['municipality'] = raw_temp['munic'] + ', ' + raw_temp['uf']

gamma_codemun_dict = {gamma: sub_df[['codemun','meso','micro','munic','uf']].value_counts() for gamma, sub_df in temp.groupby('gamma')}


pickle.dump( gamma_educ_dict,         open(root + '/Data/derived/dump/' + modelname + '_gamma_educ_dict.p', "wb" ) )

pickle.dump( gamma_industry_dict,         open(root + '/Data/derived/dump/' + modelname + '_gamma_industry_dict.p', "wb" ) )

pickle.dump( gamma_codemun_dict,         open(root + '/Data/derived/dump/' + modelname + '_gamma_codemun_dict.p', "wb" ) )


#############################################################################################
#############################################################################################
# Spatial and occupation distribution figures    
#############################################################################################
#############################################################################################





meso_share_df=     pickle.load(  open(root + '/Data/derived/dump/' + modelname + '_meso_share_df.p', "rb" ) )
meso_share_norm_df=pickle.load(  open(root + '/Data/derived/dump/' + modelname + '_meso_share_norm_df.p', "rb" ) )

iota_dict=         pickle.load(  open(root + '/Data/derived/dump/' + modelname + '_iota_dict.p', "rb" ) )
gamma_dict=        pickle.load(  open(root + '/Data/derived/dump/' + modelname + '_gamma_dict.p', "rb" ) )




##############################################################################################################################
# Create figures for each of the gammas displaying the geographic and occupation distributions, along with other stats

for g in range(0,gammas.gamma.max()+1):
    print(g)
    plot_mesos(g, gammas_w_attributes, meso_share_df, meso_share_norm_df, gamma_dict)





##############################################################################################################################
# Print the correlation matrix for gamma atrributes

correlation_matrix = gammas_w_attributes[['educ_mean', 'educ_mean_rank', 'mean_monthly_earnings', 'mean_monthly_earnings_rank', 'hhi_occ4', 'hhi_code_meso', 'hhi_codemun', 'hhi_jid', 'num_unique_jids', 'num_unique_wids', 'num_unique_wid_jids', 'j2j_dist_mean', 'j2j_dist_std', 'j2j_dist_25th', 'j2j_dist_75th', 'spatial_var_km']].corr()

# Display all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)

# Make a mask for the upper triangle
mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))

# Replace the upper triangle with NaN values
lower_triangle_corr_matrix = correlation_matrix.mask(mask)

print(lower_triangle_corr_matrix)

lower_triangle_corr_matrix.to_csv(root + '/Results/gamma_summary_stats/correlation_matrix_lower.csv')

##############################################################################################################################
# A bunch of ploratory correlation plots

corr_plots(gammas_w_attributes['hhi_codemun'],gammas_w_attributes['hhi_occ4'])
corr_plots(gammas_w_attributes['hhi_jid'],gammas_w_attributes['educ_mean'])
corr_plots(gammas_w_attributes['hhi_jid'],gammas_w_attributes['j2j_dist_mean'])

corr_plots(gammas_w_attributes['educ_mean'],gammas_w_attributes['mean_monthly_earnings'])
corr_plots(gammas_w_attributes['educ_mean'],gammas_w_attributes['log_mean_monthly_earnings'])
corr_plots(gammas_w_attributes['j2j_dist_mean'],gammas_w_attributes['log_mean_monthly_earnings'])
corr_plots(gammas_w_attributes['spatial_var_km'],gammas_w_attributes['log_mean_monthly_earnings'])


binscatter_list = [('educ_mean','log_mean_monthly_earnings'),  
                   ('j2j_dist_mean','log_mean_monthly_earnings'),  
                   ('spatial_var_km','log_mean_monthly_earnings'),  
                   ('spatial_var_km','j2j_dist_mean'),  
                   ('spatial_var_km','educ_mean'),  
                   ('hhi_jid','log_mean_monthly_earnings'),  
                   ('hhi_occ4','log_mean_monthly_earnings'),  
                   ('hhi_code_meso','log_mean_monthly_earnings')]

for idx in binscatter_list: 
    xvar = idx[0]
    yvar = idx[1]
    fig, ax = plt.subplots()
    # Binned scatter plot of Wage vs Tenure
    ax.binscatter(gammas_w_attributes[xvar],gammas_w_attributes[yvar])
    xmax_gamma = int(gammas_w_attributes.loc[gammas_w_attributes[xvar].idxmax(), 'gamma'])
    ymax_gamma = int(gammas_w_attributes.loc[gammas_w_attributes[yvar].idxmax(), 'gamma'])
    print(xmax_gamma,ymax_gamma)


plot_mesos(xmax_gamma, gammas_w_attributes, meso_share_df, meso_share_norm_df, gamma_dict)
plot_mesos(xmax_gamma, gammas_w_attributes, meso_share_df, meso_share_norm_df, gamma_dict)

# Next step: start looking at these for a compelling story. Also see if there are interesting binscatters with the HHIs

############################################################################################
# Scatter plots by sector (can probably be converted to a function if we get interesting results)

def sector_scatter_plots(var1,var2):
    sector_labels = ["Agriculture/forestry",
                      "Extractive",
                      "Manufacturing",
                      "Utilities",
                      "Construction",
                      "Sales/repair of vehicles",
                      "Transport, storage/ mail",
                      "Accommodation and food",
                      "Information/communication",
                      "Finance/insurance",
                      "Real estate",
                      "Professional/sci/tech",
                      "Public sector",
                      "Private health/educ",
                      "Arts/culture/sports/rec"]
    # Scatterplots color coded by sector
    sector_colors = {1: 'red', 2: 'blue', 3: 'green', 4: 'orange', 5: 'purple', 6: 'brown', 7: 'pink', 8: 'gray', 9: 'olive', 10: 'cyan', 11: 'magenta', 12: 'darkred', 13: 'navy', 14: 'lime', 15: 'teal'}
    corr = np.corrcoef(gammas_w_attributes[var1], gammas_w_attributes[var2])[0][1]
    print('Correlation: ', round(corr,3))
    fig, ax = plt.subplots(figsize=(10,5))
    ax.scatter(gammas_w_attributes[var1], gammas_w_attributes[var2], s=2, c=gammas_w_attributes['modal_sector'].apply(lambda x: sector_colors[x]))
    ax.annotate("Correlation = {:.2f}".format(corr), xy=(0.05, 0.95), xycoords='axes fraction')
    ax.set_xlabel(var1)            
    ax.set_ylabel(var2)
    #plt.colorbar()
    # Create legend handles and labels for each unique value of 'ind'
    handles = [plt.plot([], [], marker="o", ls="", color=color)[0] for color in np.unique(list(sector_colors.values()))]
    # Adjust the main plot's width (2/3 for the plot, 1/3 for the legend)
    plt.subplots_adjust(right=0.69)
    plt.legend(handles, sector_labels, title='ind', loc='upper left', bbox_to_anchor=(1, 1))
    plt.tight_layout()
    plt.savefig(root + '/Results/gamma_summary_stats/sector_scatterplot_' + var1 + '_' + var2 +' .pdf', format='pdf')
    plt.show()
    
    

for idx in binscatter_list: 
    xvar = idx[0]
    yvar = idx[1]
    sector_scatter_plots(xvar,yvar)
