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


homedir = os.path.expanduser('~')
root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
sys.path.append(root + 'Code/Modules')
os.chdir(root)

import bisbm
from pull_one_year import pull_one_year
from explore_gammas_functions import all


state_codes = [31, 33, 35]

region_codes = pd.read_csv('./Data/raw/munic_microregion_rm.csv', encoding='latin1')
region_codes = region_codes.loc[region_codes.code_uf.isin(state_codes)]
state_cw = region_codes[['code_meso','uf']].drop_duplicates()
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})

modelname = '3states_2013to2016_mcmc'
# 2004 appears to be the first year in which we have job start end dates (data_adm and data_deslig)

#appended = pd.read_pickle('../dump/appended_sbm_' + modelname + '.p')


# Load gammas
                           
columns = ['jid']
rename = {}
for l in range(0,1):
    columns = columns + ['job_blocks_level_'+str(l)]
    rename['job_blocks_level_'+str(l)] = 'gamma'

gammas = pd.read_csv('./Data/derived/sbm_output/model_' + modelname + '_jblocks.csv', usecols=columns).rename(columns=rename)
#model = pickle.load(open('../data/model_'+modelname+'.p', "rb" ))


                           
################################################
# Pull other variables like education for 2016
# - These are variables that we want to merge on by wid-jid but then collapse by gamma to better characterize the different gammas

raw2016 = pull_one_year(2016, 'cbo2002', othervars=['grau_instr','rem_med_r','clas_cnae20','codemun'], state_codes=state_codes, age_lower=25, age_upper=55, nrows=None)

raw2016 = raw2016.merge(gammas, how='left', validate='m:1', on='jid',indicator=True)
raw2016 = raw2016.merge(muni_meso_cw, on='codemun')

raw2016['occ2'] = raw2016['cbo2002'].str[0:2]
raw2016['occ4'] = raw2016['cbo2002'].str[0:4]
raw2016['ind2'] = raw2016['clas_cnae20'].astype('str').str[0:2].astype('int')
raw2016['sector_IBGE'] = np.nan
raw2016['sector_IBGE'].loc[( 1<=raw2016['ind2']) & (raw2016['ind2'] <= 3)] = 1  
raw2016['sector_IBGE'].loc[( 5<=raw2016['ind2']) & (raw2016['ind2'] <= 9)] = 2 
raw2016['sector_IBGE'].loc[(10<=raw2016['ind2']) & (raw2016['ind2'] <=33)] = 3 
raw2016['sector_IBGE'].loc[(35<=raw2016['ind2']) & (raw2016['ind2'] <=39)] = 4 
raw2016['sector_IBGE'].loc[(41<=raw2016['ind2']) & (raw2016['ind2'] <=43)] = 5 
raw2016['sector_IBGE'].loc[(45<=raw2016['ind2']) & (raw2016['ind2'] <=47)] = 6 
raw2016['sector_IBGE'].loc[(49<=raw2016['ind2']) & (raw2016['ind2'] <=53)] = 7 
raw2016['sector_IBGE'].loc[(55<=raw2016['ind2']) & (raw2016['ind2'] <=56)] = 8 
raw2016['sector_IBGE'].loc[(58<=raw2016['ind2']) & (raw2016['ind2'] <=63)] = 9 
raw2016['sector_IBGE'].loc[(64<=raw2016['ind2']) & (raw2016['ind2'] <=66)] = 10
raw2016['sector_IBGE'].loc[(68<=raw2016['ind2']) & (raw2016['ind2'] <=68)] = 11
raw2016['sector_IBGE'].loc[(69<=raw2016['ind2']) & (raw2016['ind2'] <=82)] = 12
raw2016['sector_IBGE'].loc[(84<=raw2016['ind2']) & (raw2016['ind2'] <=84)] = 13
raw2016['sector_IBGE'].loc[(85<=raw2016['ind2']) & (raw2016['ind2'] <=88)] = 14
raw2016['sector_IBGE'].loc[(90<=raw2016['ind2']) & (raw2016['ind2'] <=97)] = 15


# Recode education variable to approximately reflect years of schooling
raw2016['grau_instr'] = raw2016['grau_instr'].replace({1:1, 2:3, 3:5, 4:7, 5:9, 6:10, 7:12, 8:14, 9:16, 10:18, 11:21})




######################################
# Compute HHIs


hhi_occ4 = gamma_hhi('gamma','occ4')
hhi_occ2 = gamma_hhi('gamma','occ2')
hhi_code_meso = gamma_hhi('gamma','code_meso')
hhi_codemun = gamma_hhi('gamma','codemun')
hhi_jid = gamma_hhi('gamma','jid')

# Collapse a bunch of the variables in raw2016 by gamma
gammas_w_attributes = raw2016.groupby(['gamma']).agg(educ_median=('grau_instr','median'), educ_mean=('grau_instr','mean'), educ_mode=('grau_instr',lambda x: stats.mode(x)[0][0]), mean_monthly_earnings=('rem_med_r','mean'),modal_ind2=('ind2', lambda x: stats.mode(x)[0][0]), modal_sector=('sector_IBGE', lambda x: stats.mode(x)[0][0]), modal_occ2=('occ2', lambda x: stats.mode(x)[0][0]), modal_occ4=('occ4', lambda x: stats.mode(x)[0][0]))

# Create some rank variables 
gammas_w_attributes['educ_mean_rank'] = gammas_w_attributes['educ_mean'].rank(method='dense', pct=True)
gammas_w_attributes['mean_monthly_earnings_rank'] = gammas_w_attributes['mean_monthly_earnings'].rank(method='dense', pct=True)


num_unique_jids = raw2016.groupby('gamma')['jid'].nunique().reset_index().rename(columns={'jid':'num_unique_jids'})
num_unique_wids = raw2016.groupby('gamma')['wid'].nunique().reset_index().rename(columns={'wid':'num_unique_wids'})
num_unique_wid_jids = raw2016.groupby('gamma').apply(lambda x: x[['jid', 'wid']].drop_duplicates().shape[0]).astype(int).reset_index().rename(columns={0:'num_unique_wid_jids'})

# Merge on HHIs
gammas_w_attributes = gammas_w_attributes.merge(hhi_occ4, on='gamma', validate='1:1')
gammas_w_attributes = gammas_w_attributes.merge(hhi_occ2, on='gamma', validate='1:1')
gammas_w_attributes = gammas_w_attributes.merge(hhi_code_meso, on='gamma', validate='1:1')
gammas_w_attributes = gammas_w_attributes.merge(hhi_codemun, on='gamma', validate='1:1')
gammas_w_attributes = gammas_w_attributes.merge(hhi_jid, on='gamma', validate='1:1')
gammas_w_attributes = gammas_w_attributes.merge(num_unique_jids, on='gamma', validate='1:1')
gammas_w_attributes = gammas_w_attributes.merge(num_unique_wids, on='gamma', validate='1:1')
gammas_w_attributes = gammas_w_attributes.merge(num_unique_wid_jids, on='gamma', validate='1:1')



corr_plots('hhi_codemun','hhi_occ4')




sector_labels = ["Agriculture/forestry",
                  "Extractive",
                  "Manufacturing",
                  "Utilities",
                  "Construction",
                  "Commerce;  repair of motor vehicles",
                  "Transport, storage and mail",
                  "Accommodation and food",
                  "Information/communication",
                  "Finance/insurance",
                  "Real estate",
                  "Professional/sci/tech",
                  "Public sector",
                  "Private health/educ",
                  "Arts/culture/sports/recreation"]

# Scatterplots color coded by sector
colors = {1: 'red', 2: 'blue', 3: 'green', 4: 'orange', 5: 'purple', 6: 'brown', 7: 'pink', 8: 'gray', 9: 'olive', 10: 'cyan', 11: 'magenta', 12: 'darkred', 13: 'navy', 14: 'lime', 15: 'teal'}
var1='hhi_codemun'
var2='hhi_occ4'
corr = np.corrcoef(gammas_w_attributes[var1], gammas_w_attributes[var2])[0][1]
print('Correlation: ', round(corr,3))
fig, ax = plt.subplots()
ax.scatter(gammas_w_attributes[var1], gammas_w_attributes[var2], s=5, c=gammas_w_attributes['modal_sector'], cmap='Set1')
ax.scatter(gammas_w_attributes[var1], gammas_w_attributes[var2], s=5, c=gammas_w_attributes['modal_sector'].apply(lambda x: colors[x]))
ax.annotate("Correlation = {:.2f}".format(corr), xy=(0.05, 0.95), xycoords='axes fraction')
ax.set_xlabel(var1)            
ax.set_ylabel(var2)
#plt.colorbar()
# Create legend handles and labels for each unique value of 'ind'
handles = [plt.plot([], [], marker="o", ls="", color=color)[0] for color in np.unique(list(colors.values()))]
labels = list(colors.keys())
#plt.legend(handles, labels, title='ind', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.legend(handles, labels, title='ind', loc='best')
plt.savefig('./Results/hhi_scatterplot_' + var1 + '_' + var2 +' .pdf', format='pdf')
plt.close()







                                                                                                                                                                                                                                        
################################
#
'''                           
gammas_w_attributes = appended[['jid','wid','occ4','codemun','year']].merge(gammas, how='left', validate='m:1', on='jid',indicator=True)
gammas_w_attributes['occ2'] = gammas_w_attributes['occ4'].str[0:2]
gammas_w_attributes = gammas_w_attributes.merge(muni_meso_cw, on='codemun')
gammas_w_attributes = gammas_w_attributes.merge(raw2016, on=['wid','jid'], validate='m:1')


# Collapse attributes by gamma
gamma_attributes = gamma_attributes.groupby(['wid','jid']).agg({'grau_instr':'max','rem_med_r':'max','clas_cnae20':'first'}).reset_index()
'''

# For each gamma0 compute the ooc and codemun HHIs
# Compute the distributions for a single year





gamma_hhi('occ4','codemun')
gamma_hhi('occ2','code_meso')        

#XX show the correlations of these things with education average education in the gamma. Maybe age. Or color code it by modal occ2 or industry to see if specific industries/occupations tend to be in partcular areas. OR color code by average education. 




# Calculate the correlation coefficient
corr = np.corrcoef(gamma_hhis.hhi_codemun, gamma_hhis.hhi_occ4)[0][1]

# Create the scatter plot
fig, ax = plt.subplots()
ax.scatter(gamma_hhis.hhi_codemun, gamma_hhis.hhi_occ4, s=5)
ax.annotate("Correlation = {:.2f}".format(corr), xy=(0.05, 0.95), xycoords='axes fraction')
ax.set_xlabel('codemun')            
ax.set_ylabel('occ4')
plt.savefig('./Results/hhi_scatterplot_codemun_occ4.pdf', format='pdf')
plt.close()
# Why do we have a strong positive correlation:
# - High education workers are concentrated workers are concentrated in occupations and are concentrated in specific municipalities, even though these municipalities may be geographically dispersed. The problem is the HHI won't capture the geographic dispersion.
# - 

df = pd.read_pickle('./Data/derived/predicting_flows/pred_flows_df.p')
pivot = pd.pivot_table(df,  values='wid', index='gamma', columns='uf', aggfunc='count', fill_value=0).reset_index()

# Counting modal states for each gamma
pivot.idxmax(axis=1).value_counts()
'''
35    840   SP
33    182   RJ
31    133   MG
'''

# Shares associated with the modal state counts above
pivot.idxmax(axis=1).value_counts()/pivot.idxmax(axis=1).value_counts().sum()
'''
35    0.727273
33    0.157576
31    0.115152
'''


# Population shares by state
a=pivot.loc[pivot.gamma!=-1][['MG','RJ','SP']].sum()/pivot.loc[pivot.gamma!=-1][['MG','RJ','SP']].sum().sum()
'''
uf
31    0.187040
33    0.192016
35    0.620944
'''





    

#############################################################################################
# Move distances 


# Pull meso codes for our states of interest
muni_sp = geobr.read_municipality(code_muni="SP", year=2016)
muni_rj = geobr.read_municipality(code_muni='RJ', year=2016)
muni_mg = geobr.read_municipality(code_muni='MG', year=2016)
munis = pd.concat([muni_sp, muni_rj, muni_mg], ignore_index=True)
munis['lon'] = munis.geometry.centroid.x
munis['lat'] = munis.geometry.centroid.y
munis['codemun'] = munis.code_muni//10

df_trans = pd.read_pickle('./Data/derived/predicting_flows/pred_flows_df_trans.p')

# Assign each jid its modal municipality code based on 2016 data
raw = pd.read_csv('~/rais/RAIS/csv/brasil2016.csv', usecols=['id_estab','cbo2002','codemun'], sep=';', dtype={'id_estab':str, 'cbo2002':str})
raw['occ4'] = raw['cbo2002'].str[0:4]
raw['jid']  = raw['id_estab'] + '_' + raw['occ4']
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
    'mean': 'dist_mean',
    'median': 'dist_median',
    'max': 'dist_max',
    'min': 'dist_min',
    'std': 'dist_std',
    '<lambda_0>': 'dist_25th',
    '<lambda_1>': 'dist_75th'
})


gammas_w_attributes = gammas_w_attributes.merge(distance_stats, left_on='gamma', right_on='gamma', validate='1:1')


#############################################################################################
# Gamma spatial variances

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.precision', 3)

print(gammas_w_attributes.drop(columns=['gamma','modal_ind2','modal_sector','dist_min']).corr())
'''
                       educ_median  educ_mean  educ_mode  mean_monthly_earnings  hhi_occ4  hhi_occ2  hhi_code_meso  hhi_codemun  dist_mean  dist_median  dist_max  dist_std  dist_25th  dist_75th
educ_median                  1.000      0.855      0.833                  0.529    -0.210    -0.294         -0.076       -0.038     -0.334       -0.360    -0.077    -0.220     -0.268     -0.269
educ_mean                    0.855      1.000      0.751                  0.620    -0.096    -0.196         -0.003        0.051     -0.360       -0.304    -0.121    -0.292     -0.228     -0.260
educ_mode                    0.833      0.751      1.000                  0.501    -0.170    -0.228         -0.024       -0.012     -0.287       -0.361    -0.115    -0.157     -0.322     -0.236
mean_monthly_earnings        0.529      0.620      0.501                  1.000    -0.088    -0.132         -0.124       -0.091     -0.096       -0.099    -0.062    -0.052     -0.038     -0.089
hhi_occ4                    -0.210     -0.096     -0.170                 -0.088     1.000     0.951          0.636        0.660      0.328        0.387    -0.242     0.074      0.157      0.428
hhi_occ2                    -0.294     -0.196     -0.228                 -0.132     0.951     1.000          0.678        0.679      0.343        0.409    -0.274     0.079      0.177      0.429
hhi_code_meso               -0.076     -0.003     -0.024                 -0.124     0.636     0.678          1.000        0.877     -0.004        0.150    -0.433    -0.196      0.081      0.095
hhi_codemun                 -0.038      0.051     -0.012                 -0.091     0.660     0.679          0.877        1.000      0.059        0.154    -0.394    -0.102      0.127      0.131
dist_mean                   -0.334     -0.360     -0.287                 -0.096     0.328     0.343         -0.004        0.059      1.000        0.725     0.049     0.859      0.436      0.906
dist_median                 -0.360     -0.304     -0.361                 -0.099     0.387     0.409          0.150        0.154      0.725        1.000    -0.017     0.348      0.795      0.701
dist_max                    -0.077     -0.121     -0.115                 -0.062    -0.242    -0.274         -0.433       -0.394      0.049       -0.017     1.000     0.157      0.012     -0.002
dist_std                    -0.220     -0.292     -0.157                 -0.052     0.074     0.079         -0.196       -0.102      0.859        0.348     0.157     1.000      0.125      0.665
dist_25th                   -0.268     -0.228     -0.322                 -0.038     0.157     0.177          0.081        0.127      0.436        0.795     0.012     0.125      1.000      0.346
dist_75th                   -0.269     -0.260     -0.236                 -0.089     0.428     0.429          0.095        0.131      0.906        0.701    -0.002     0.665      0.346      1.000
'''





# Spatial variance

jid_spatial_dist = raw2016[['jid','gamma']].merge(jid_muni, left_on='jid', right_on='jid', validate='m:1')
jid_spatial_dist['mean_lat'] = jid_spatial_dist.groupby('gamma')['lat'].transform('mean')
jid_spatial_dist['mean_lon'] = jid_spatial_dist.groupby('gamma')['lon'].transform('mean')

''' This gave me an error when I use transform but when I did it one gamma at a time I confirmed that it gave the same answer as the simple version just taking the mean below. Therefore I'm going with the simple version and accepting approximation error created by the curvature of the earth
def compute_centroid(row):
    coords = MultiPoint(list(zip(row['lon'], row['lat'])))
    return (coords.centroid.x, coords.centroid.y)

jid_spatial_dist['centroid'] = jid_spatial_dist.groupby('gamma').transform(compute_centroid)
'''


jid_spatial_dist['distance'] = jid_spatial_dist.apply(compute_distance, axis=1)
spatial_var_km = jid_spatial_dist.groupby('gamma')['distance'].mean().reset_index().rename(columns={'distance':'spatial_var_km'})

gammas_w_attributes = gammas_w_attributes.merge(spatial_var_km, left_on='gamma', right_on='gamma', how='left', validate='1:1')


gammas_w_attributes['spatial_var_km_rank'] = gammas_w_attributes['spatial_var_km'].rank(method='dense', pct=True)
gammas_w_attributes['dist_mean_rank'] = gammas_w_attributes['dist_mean'].rank(method='dense', pct=True)


gammas_w_attributes.to_pickle('./Data/derived/explore_gammas_gammas_w_attributes.p')
gammas_w_attributes = pd.read_pickle('./Data/derived/explore_gammas_gammas_w_attributes.p')

                                                                                           

#############################################################################################
#############################################################################################
# Spatial and occupation distribution figures    
#############################################################################################
#############################################################################################


########################################################
# Pull meso codes for our states of interest
meso_sp = geobr.read_meso_region(code_meso="SP", year=2016)
meso_rj = geobr.read_meso_region(code_meso='RJ', year=2016)
meso_mg = geobr.read_meso_region(code_meso='MG', year=2016)
mesos = pd.concat([meso_sp, meso_rj, meso_mg], ignore_index=True)


#Issue: the meso codes from geobr only have 4 digitsl in state_cw they sometimes have 5. Basically in state_cw there is always a 0 between the 2 state digits and the 2 meso code digits; in geo_br there is only a 0 if the meso code has 1 digit. Check with Bernardo about this

state_cw.loc[state_cw.uf=='São Paulo']

# XX I think this is unnecessary b/c already loaded df above
df = pd.read_pickle('./Data/derived/predicting_flows/pred_flows_df.p')

# Calculate share of jid observations for each gamma in each code_meso
pivot_df = pd.pivot_table(df.loc[df.gamma!=-1], index='code_meso', columns='gamma', aggfunc='size', fill_value=0)
meso_share_df = pivot_df.apply(lambda x: x/x.sum(), axis=0).reset_index()

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


from occ_counts_by_type import occ_counts_by_type
[iota_dict, gamma_dict] = occ_counts_by_type(df, 0, modelname)

pickle.dump( iota_dict,          open('./Data/derived/dump/' + modelname + '_iota_dict.p', "wb" ) )
pickle.dump( gamma_dict,         open('./Data/derived/dump/' + modelname + '_gamma_dict.p', "wb" ) )
pickle.dump( meso_share_df,      open('./Data/derived/dump/' + modelname + '_meso_share_df.p', "wb" ) )
pickle.dump( meso_share_norm_df, open('./Data/derived/dump/' + modelname + '_meso_share_norm_df.p', "wb" ) )

iota_dict=         pickle.load(  open('./Data/derived/dump/' + modelname + '_iota_dict.p', "rb" ) )
gamma_dict=        pickle.load(  open('./Data/derived/dump/' + modelname + '_gamma_dict.p', "rb" ) )
meso_share_df=     pickle.load(  open('./Data/derived/dump/' + modelname + '_meso_share_df.p', "rb" ) )
meso_share_norm_df=pickle.load(  open('./Data/derived/dump/' + modelname + '_meso_share_norm_df.p', "rb" ) )




'''
from scipy.stats import mstats

# Winsorize the "col" column of the "df" dataframe at the 1% and 99% levels
winsorized_col = mstats.winsorize(df["col"], limits=[0.01, 0.01])

# Replace the "col" column in the original dataframe with the winsorized values
df["col"] = winsorized_col
'''

for g in range(0,1154):
    print(g)
    plot_mesos(g)

[hhi_jid,num_unique_jids,num_unique_wids,num_unique_wid_jids] = gammas_w_attributes.loc[gammas_w_attributes.gamma==gamma][['hhi_jid','num_unique_jids','num_unique_wids','num_unique_wid_jids']].values.tolist()[0]
