from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import matplotlib.pyplot as plt
import scipy.stats as stats

if os.name == 'nt':
    homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
else:
    homedir = os.path.expanduser('~/labormkt')

os.chdir(homedir + '/labormkt_rafaelpereira/aug2022/code/')

#import bisbm
from pull_one_year import pull_one_year


state_codes = [31, 33, 35]

region_codes = pd.read_csv(homedir + '/labormkt_rafaelpereira/aug2022/external/munic_microregion_rm.csv', encoding='latin1')
region_codes = region_codes.loc[region_codes.code_uf.isin(state_codes)]
state_cw = region_codes[['code_meso','uf']].drop_duplicates()
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})

modelname = '3states_2013to2016'
# 2004 appears to be the first year in which we have job start end dates (data_adm and data_deslig)

#appended = pd.read_pickle('../dump/appended_sbm_' + modelname + '.p')


# Load gammas
                           
columns = ['jid']
rename = {}
for l in range(0,5):
    columns = columns + ['job_blocks_level_'+str(l)]
    rename['job_blocks_level_'+str(l)] = 'gamma_'+str(l)

gammas = pd.read_csv('../data/model_' + modelname + '_jblocks.csv', usecols=columns).rename(columns=rename)
#model = pickle.load(open('../data/model_'+modelname+'.p', "rb" ))


                           
################################################
# Pull other variables like education for 2016
# - These are variables that we want to merge on by wid-jid but then collapse by gamma to better characterize the different gammas
from pull_one_year import pull_one_year
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
                           

######################################
# Compute HHIs
                           
def gamma_hhi(gamma,var):
    hhis = {}
    gv_counts = raw2016.groupby([gamma,var])[gamma].count().reset_index(name='gv_count')
    g_counts  = raw2016.groupby([gamma])[gamma].count().reset_index(name='g_count')
    gv_counts = gv_counts.merge(g_counts, on=gamma, validate='m:1')
    gv_counts['gv_share_sq'] = (gv_counts.gv_count/gv_counts.g_count).pow(2)
    hhi = gv_counts.groupby(gamma)['gv_share_sq'].sum().reset_index(name='hhi_'+var)
    return hhi

hhi_occ4 = gamma_hhi('gamma_0','occ4')
hhi_occ2 = gamma_hhi('gamma_0','occ2')
hhi_code_meso = gamma_hhi('gamma_0','code_meso')
hhi_codemun = gamma_hhi('gamma_0','codemun')

# Collapse a bunch of the variables in raw2016 by gamma
gammas_w_attributes = raw2016.groupby(['gamma_0']).agg(educ_median=('grau_instr','median'), educ_mean=('grau_instr','mean'), educ_mode=('grau_instr',lambda x: stats.mode(x)[0][0]), mean_monthly_earnings=('rem_med_r','mean'),modal_ind2=('ind2', lambda x: stats.mode(x)[0][0]), modal_sector=('sector_IBGE', lambda x: stats.mode(x)[0][0]), modal_occ2=('occ2', lambda x: stats.mode(x)[0][0]), modal_occ4=('occ4', lambda x: stats.mode(x)[0][0]))

# Merge on HHIs
gammas_w_attributes = gammas_w_attributes.merge(hhi_occ4, on='gamma_0', validate='1:1')
gammas_w_attributes = gammas_w_attributes.merge(hhi_occ2, on='gamma_0', validate='1:1')
gammas_w_attributes = gammas_w_attributes.merge(hhi_code_meso, on='gamma_0', validate='1:1')
gammas_w_attributes = gammas_w_attributes.merge(hhi_codemun, on='gamma_0', validate='1:1')

gammas_w_attributes.to_pickle('../dump/explore_gammas_gammas_w_attributes.p')

def corr_plots(var1,var2):
    # Create the scatter plot
    corr = np.corrcoef(gammas_w_attributes[var1], gammas_w_attributes[var2])[0][1]
    print('Correlation: ', round(corr,3))
    fig, ax = plt.subplots()
    ax.scatter(gammas_w_attributes[var1], gammas_w_attributes[var2], s=5)
    ax.annotate("Correlation = {:.2f}".format(corr), xy=(0.05, 0.95), xycoords='axes fraction')
    ax.set_xlabel(var1)            
    ax.set_ylabel(var2)
    plt.savefig('hhi_scatterplot_' + var1 + '_' + var2 +' .pdf', format='pdf')
    plt.close()


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
plt.savefig('../results/hhi_scatterplot_' + var1 + '_' + var2 +' .pdf', format='pdf')
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





gamma_hhis('occ4','codemun')
gamma_hhis('occ2','code_meso')        

XX show the correlations of these things with education average education in the gamma. Maybe age. Or color code it by modal occ2 or industry to see if specific industries/occupations tend to be in partcular areas. OR color code by average education. 


'''
OLD
    
#######################
# Codemun HHIs
# - Problem with this: we want a measure of spatial dispersion. A gamma that is totally in Rio, SP, and BH would havea high HHI despite being very geographically dispersed. 
gc_counts = gammas_w_attributes.loc[gammas_w_attributes.year==2016].groupby(['gamma_0','codemun'])['gamma_0'].count().reset_index(name='gc_count')
g_counts = gammas_w_attributes.loc[gammas_w_attributes.year==2016].groupby(['gamma_0'])['gamma_0'].count().reset_index(name='g_count')
gc_counts = gc_counts.merge(g_counts, on='gamma_0', validate='m:1')
gc_counts['gc_share']    = (gc_counts.gc_count/gc_counts.g_count)
gc_counts['gc_share_sq'] = (gc_counts.gc_count/gc_counts.g_count).pow(2)
gamma_codemun_hhi = gc_counts.groupby('gamma_0')['gc_share_sq'].sum().reset_index(name='hhi_codemun')


#######################
# Occ4 HHIs
gammas_w_attributes 
go_counts = gammas_w_attributes.loc[gammas_w_attributes.year==2016].groupby(['gamma_0','occ4'])['gamma_0'].count().reset_index(name='go_count')
g_counts = gammas_w_attributes.loc[gammas_w_attributes.year==2016].groupby(['gamma_0'])['gamma_0'].count().reset_index(name='g_count')
go_counts = go_counts.merge(g_counts, on='gamma_0', validate='m:1')
go_counts['go_share']    = (go_counts.go_count/go_counts.g_count)
go_counts['go_share_sq'] = (go_counts.go_count/go_counts.g_count).pow(2)
gamma_occ4_hhi = go_counts.groupby('gamma_0')['go_share_sq'].sum().reset_index(name='hhi_occ4')


gamma_hhis = gamma_codemun_hhi.merge(gamma_occ4_hhi, on='gamma_0',validate='1:1')
'''



# Calculate the correlation coefficient
corr = np.corrcoef(gamma_hhis.hhi_codemun, gamma_hhis.hhi_occ4)[0][1]

# Create the scatter plot
fig, ax = plt.subplots()
ax.scatter(gamma_hhis.hhi_codemun, gamma_hhis.hhi_occ4, s=5)
ax.annotate("Correlation = {:.2f}".format(corr), xy=(0.05, 0.95), xycoords='axes fraction')
ax.set_xlabel('codemun')            
ax.set_ylabel('occ4')
plt.savefig('hhi_scatterplot_codemun_occ4.pdf', format='pdf')
plt.close()
# Why do we have a strong positive correlation:
# - High education workers are concentrated workers are concentrated in occupations and are concentrated in specific municipalities, even though these municipalities may be geographically dispersed. The problem is the HHI won't capture the geographic dispersion.
# - 


df = pd.read_pickle('../dump/' + modelname + '_df.p')
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
a=pivot.loc[pivot.gamma!=-1][['31','33','35']].sum()/pivot.loc[pivot.gamma!=-1][['31','33','35']].sum().sum()
'''
uf
31    0.187040
33    0.192016
35    0.620944
'''

