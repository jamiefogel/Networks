# -*- coding: utf-8 -*-
"""
Created on Mon Feb 27 16:23:12 2023

@author: p13861161
"""

from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc

homedir = os.path.expanduser('~')
os.chdir(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/code/')


import bisbm
from pull_one_year import pull_one_year


run_sbm = False

state_codes = [31, 33, 35]
rio_codes = [330045, 330170, 330185, 330190, 330200, 330227, 330250, 330285, 330320, 330330, 330350, 330360, 330414, 330455, 330490, 330510, 330555, 330575]
region_codes = pd.read_csv(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/external/munic_microregion_rm.csv', encoding='latin1')
region_codes = region_codes.loc[region_codes.code_uf.isin(state_codes)]
state_cw = region_codes[['code_meso','uf']].drop_duplicates()
muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'codemun':region_codes.code_munic//10})

firstyear = 2013
lastyear = 2016

maxrows = 100000
#maxrows=None

modelname='junk'
#modelname = 'mass_layoffs_3states_'+str(firstyear)+'to'+str(lastyear)
# 2004 appears to be the first year in which we have job start end dates (data_adm and data_deslig)

# CPI: 06/2015=100
cpi = pd.read_csv(homedir + '/labormkt/labormkt_rafaelpereira/ExternalData/BRACPIALLMINMEI.csv', parse_dates=['date'], names=['date','cpi'], header=0)
cpi['cpi'] = cpi.cpi/100
cpi['date'] = cpi['date'].dt.to_period('M')


gammas = pd.read_csv('../data/model_3states_2013to2016_jblocks.csv', usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
gammas['gamma'] = gammas.gamma.fillna(-1)
earn_vars = ['vl_rem_01', 'vl_rem_02', 'vl_rem_03', 'vl_rem_04', 'vl_rem_05', 'vl_rem_06', 'vl_rem_07', 'vl_rem_08', 'vl_rem_09', 'vl_rem_10', 'vl_rem_11', 'vl_rem_12', 'vl_rem_13_adiant', 'vl_rem_13_final']
const_vars = ['wid', 'jid', 'codemun', 'cbo2002', 'causa_deslig', 'start_date', 'end_date', 'year','clas_cnae20', 'yob','uf']

########################################################################################
########################################################################################
# Create a wid-jid-month panel
########################################################################################
########################################################################################

for year in range(firstyear,lastyear+1):
    raw = pull_one_year(year, 'cbo2002', othervars=['data_adm','data_deslig','causa_deslig','clas_cnae20','uf']+earn_vars, state_codes=state_codes, age_lower=25, age_upper=55, nrows=maxrows)
    # Deflate
    raw['months_earn_gt0'] = (raw[earn_vars].drop(columns=['vl_rem_13_adiant', 'vl_rem_13_final']).gt(0)).sum(axis=1)
    raw['vl_rem_13'] = raw[['vl_rem_13_adiant', 'vl_rem_13_final']].sum(axis=1)
    raw['causa_deslig'] = raw.causa_deslig.fillna(-1).astype('int16')
    # Allocate the 13th payment equally over all months with positive earnings
    for i in earn_vars[0:12]:                                         
        cond = ((raw[i]>0) & raw['vl_rem_13']>0)
        raw.loc[cond,i] = raw.loc[cond,i] + raw.loc[cond,'vl_rem_13'] / raw.loc[cond,'months_earn_gt0']
    raw['start_date'] = pd.to_datetime(raw['data_adm'])
    raw['end_date']   = pd.to_datetime(raw['data_deslig'])
    raw.drop(columns=['id_estab','occ4','data_adm','data_deslig','idade', 'tipo_vinculo', 'vl_rem_13_adiant', 'vl_rem_13_final', 'vl_rem_13','months_earn_gt0'], inplace=True) # Can extract id_estab and occ4 from jid if necessary. This saves space
    # Reshape monthly earnings from wide to long
    data_monthly = raw.melt(id_vars=const_vars, var_name='month', value_name='monthly_earnings')
    data_monthly['month'] = data_monthly.month.replace('vl_rem_','', regex=True).astype('int')
    del raw
    # Identify first and last day of the month for checking whether the worker was employed in that month
    data_monthly['month_start'] = pd.to_datetime((data_monthly.year*10000+data_monthly.month*100+1).apply(str),format='%Y%m%d')
    data_monthly['month_end'] = (data_monthly['month_start'] + pd.offsets.MonthEnd())
    # Keep only months in which worker is employed in this job (make panel unbalanced). Done to save memory
    data_monthly = data_monthly.loc[(data_monthly.start_date<=data_monthly.month_end) & ((data_monthly.end_date>=data_monthly.month_start) | (data_monthly.end_date.isna()==True))]
    data_monthly.drop(columns=['month_start','month_end'], inplace=True)
    data_monthly['date'] = pd.to_datetime(data_monthly[['year', 'month']].assign(day=1)).dt.to_period('M')
    data_monthly.drop(columns=['month','year'], inplace=True)
    # Merge on meso region codes and gammas so that we can define markets
    data_monthly = data_monthly.merge(muni_meso_cw, how='left', on='codemun', copy=False) # validate='m:1', indicator=True)
    data_monthly['occ2Xmeso'] = data_monthly.cbo2002.str[0:2] + '_' + data_monthly['code_meso'].astype('str')
    data_monthly = data_monthly.merge(gammas, on='jid', how='left')
    # Deflate monthly earnings
    data_monthly['gamma'] = data_monthly.gamma.fillna(-1)
    data_monthly = data_monthly.merge(cpi, on='date', how='left')
    data_monthly['monthly_earnings'] = data_monthly.monthly_earnings/data_monthly.cpi
    data_monthly.drop(columns='cpi',inplace=True)
    if year==firstyear:
        df = data_monthly
    else:
        df = pd.concat([df,data_monthly])
    del data_monthly
    gc.collect()

''' At this point df is an unbalanced panel of worker-job-months where months in which the worker is not employed are dropped to save memory. It has the following columns:
       'wid', 'jid', 'codemun', 'cbo2002', 'causa_deslig', 'start_date',
       'end_date', 'clas_cnae20', 'yob', 'uf', 'monthly_earnings', 'date',
       'code_meso', 'occ2Xmeso', 'gamma'
'''

# Flag months in which a job started or ended. This will allow us to identify layoffs. Then flag layoffs.
# causa_deslig=11 implies layoff, however many quits may be labeled as layoffs in order to receive UI because Brazil does not have the same sort of experience rating as the US does.
for i in ['start','end']:
    df['job_' + i + '_flag'] = df[i + '_date'].dt.to_period('M')==df.date

df['layoff'] = ((df.causa_deslig==11) & (df.job_end_flag==1))
df['employed_in_month'] = 1

df.sort_values(by=['wid','jid','date'],inplace=True)
gc.collect()
df = df.reset_index().drop(columns='index')

# XX Temp fix because of the issue with UF in 2015
df['uf'] = df.codemun.astype(str).str[0:2]

##########################################################

# UNTIL HERE WE HAVE A DF OBJECT WITH WHAT WE NEED

# cleaning the DF object to keep only what we need
df = df[['codemun', 'cbo2002', 'uf', 'gamma']]
df['occ4'] = df.cbo2002.astype(str).str[:4]
df = df[['codemun', 'occ4', 'uf', 'gamma']]

df = df.loc[(df.gamma!=-1)] # removing observations with no gamma

df

# Now constructing HHIs

# HHI function
def hhi_fun(df, index, cells):
    crosstab_index_cells = pd.crosstab(index = df[index], columns = df[cells])
    cells_probabilities_by_index = crosstab_index_cells.div(crosstab_index_cells.sum(axis=1),axis=0).reset_index()
    cells_probabilities_by_index['hhi'] = cells_probabilities_by_index.drop(columns=index).pow(2).sum(axis=1)
    return cells_probabilities_by_index[[index, 'hhi']]

# HHI computations
occ4_gamma_hhi = hhi_fun(df, 'gamma', 'occ4')
codemun_gamma_hhi = hhi_fun(df, 'gamma', 'codemun')




########################################################
########################################################
########################################################
########################################################
########################################################
########################################################
########################################################
########################################################


# CONCENTRATION FIGURES CODE BELOW FOR INSPIRATION

# fname_stub = 'concentration_figures'
# data_full = pd.read_csv(mle_data_filename)
# data_full = data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1)]

# # iota concentration
# crosstab_iota_sector = pd.crosstab(index = data_full.iota, columns = data_full.sector_IBGE)
# sector_probabilities_by_iota = crosstab_iota_sector.div(crosstab_iota_sector.sum(axis=1),axis=0).reset_index()
# sector_probabilities_by_iota['hhi'] = sector_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)

# crosstab_iota_sector5 = pd.crosstab(index = data_full.iota, columns = data_full.clas_cnae20)
# sector5_probabilities_by_iota = crosstab_iota_sector5.div(crosstab_iota_sector5.sum(axis=1),axis=0).reset_index()
# sector5_probabilities_by_iota['hhi'] = sector5_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)

# crosstab_iota_gamma = pd.crosstab(index = data_full.iota, columns = data_full.gamma).drop(columns=0)
# gamma_probabilities_by_iota = crosstab_iota_gamma.div(crosstab_iota_gamma.sum(axis=1),axis=0).reset_index()
# gamma_probabilities_by_iota['hhi'] = gamma_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)

# crosstab_iota_occ4 = pd.crosstab(index = data_full.iota, columns = data_full.occ4_first_recode)
# occ4_probabilities_by_iota = crosstab_iota_occ4.div(crosstab_iota_occ4.sum(axis=1),axis=0).reset_index()
# occ4_probabilities_by_iota['hhi'] = occ4_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)

# # gamma concentration

# crosstab_gamma_iota = pd.crosstab(index = data_full.gamma, columns = data_full.iota).drop(index=0)
# iota_probabilities_by_gamma = crosstab_gamma_iota.div(crosstab_gamma_iota.sum(axis=1),axis=0).reset_index()
# iota_probabilities_by_gamma['hhi'] = iota_probabilities_by_gamma.drop(columns='gamma').pow(2).sum(axis=1)

# crosstab_gamma_occ4 = pd.crosstab(index = data_full.gamma, columns = data_full.occ4_first_recode).drop(index=0)
# occ4_probabilities_by_gamma = crosstab_gamma_occ4.div(crosstab_gamma_occ4.sum(axis=1),axis=0).reset_index()
# occ4_probabilities_by_gamma['hhi'] = occ4_probabilities_by_gamma.drop(columns='gamma').pow(2).sum(axis=1)




# # Public sector
# sector_index = 13
# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,291), sector_probabilities_by_iota[sector_index].sort_values(),s=5)
# ax.set_xlabel('Worker types (sorted by shocked sector share)')            
# ax.set_ylabel('Shocked sector share of Employment')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.figure.savefig(figuredir+fname_stub+'_iota_shocked_sector_share.png') #Unused




# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,291), sector_probabilities_by_iota['hhi'].sort_values(),s=5)
# ax.set_xlabel('Worker types (sorted by sector employment HHI)')
# ax.set_ylabel('Sector employment HHI')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.set_ylim(0,1)
# ax.figure.savefig(figuredir+fname_stub+'_iota_sector_hhi.png') #Unused


# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(),s=5)
# ax.set_xlabel('Worker types (sorted by market employment HHI)')
# ax.set_ylabel('Market employment HHI')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.set_ylim(0,1)
# ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_hhi.png') #Unused


# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,291), sector_probabilities_by_iota['hhi'].sort_values(),s=5,label='Sector')
# ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(), s=5,label='Market', marker='+')
# ax.set_xlabel('Worker types (sorted by employment HHI)')
# ax.set_ylabel('Concentration (HHI)')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.set_ylim(0,1)
# ax.legend()
# ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_sector_hhi.png', dpi=300,bbox_inches='tight') #Unused


# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,291), sector5_probabilities_by_iota['hhi'].sort_values(),s=5,label='5-digit industry')
# ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(), s=5,label='Market', marker='+')
# ax.set_xlabel('Worker types (sorted by employment HHI)')
# ax.set_ylabel('Concentration (HHI)')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.set_ylim(0,1)
# ax.legend()
# ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_sector5_hhi.png', dpi=300,bbox_inches='tight') #Unused


# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,291), occ4_probabilities_by_iota['hhi'].sort_values(),s=5,label='4-digit occupation')
# ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(), s=5,label='Market', marker='+')
# ax.set_xlabel('Worker types (sorted by employment HHI)')
# ax.set_ylabel('Concentration (HHI)')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.set_ylim(0,1)
# ax.legend()
# ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_occ4_hhi.png', dpi=300,bbox_inches='tight') #Unused


# # gamma hhi
# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,428), occ4_probabilities_by_gamma['hhi'].sort_values(),s=5,label='4-digit occupation')
# ax.scatter(np.arange(1,428), iota_probabilities_by_gamma['hhi'].sort_values(), s=5,label='Worker type', marker='+')
# ax.set_xlabel('Markets (sorted by employment HHI)')
# ax.set_ylabel('Concentration (HHI)')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.set_ylim(0,1)
# ax.legend()
# ax.figure.savefig(figuredir+fname_stub+'_gamma_iota_occ4_hhi.png', dpi=300,bbox_inches='tight') #Unused





# factor = 1/mle_data_sums['m_i'].min().numpy()
# i_weights = np.round(factor*np.array(mle_data_sums['m_i']),0)

# sector_probabilities_by_iota['i_weights']  = i_weights
# sector5_probabilities_by_iota['i_weights'] = i_weights
# occ4_probabilities_by_iota['i_weights'] = i_weights
# gamma_probabilities_by_iota['i_weights']  = i_weights

# iota_probabilities_by_gamma['g_weights']  = mle_data_sums['sum_count_g'][1:428]
# occ4_probabilities_by_gamma['g_weights']  = mle_data_sums['sum_count_g'][1:428]


# l =sector_probabilities_by_iota.loc[sector_probabilities_by_iota.index.repeat(sector_probabilities_by_iota.i_weights)].shape[0]+1

 



# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,l), sector_probabilities_by_iota.loc[sector_probabilities_by_iota.index.repeat(sector_probabilities_by_iota.i_weights)]['hhi'].sort_values(),s=3,label='Sector')
# ax.scatter(np.arange(1,l), gamma_probabilities_by_iota.loc[gamma_probabilities_by_iota.index.repeat(gamma_probabilities_by_iota.i_weights)]['hhi'].sort_values(), s=3,label='Market', marker='+')
# ax.set_xlabel('Workers (sorted by employment HHI)')
# ax.set_ylabel('Concentration (HHI)')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.set_ylim(0,1)
# ax.legend()
# ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_sector_hhi_worker_weighted.png', dpi=300,bbox_inches='tight') # Used by paper and slides


# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,l), sector5_probabilities_by_iota.loc[sector5_probabilities_by_iota.index.repeat(sector5_probabilities_by_iota.i_weights)]['hhi'].sort_values(),s=5,label='5-digit industry')
# ax.scatter(np.arange(1,l), gamma_probabilities_by_iota.loc[gamma_probabilities_by_iota.index.repeat(gamma_probabilities_by_iota.i_weights)]['hhi'].sort_values(), s=3,label='Market', marker='+')
# ax.set_xlabel('Workers (sorted by employment HHI)')
# ax.set_ylabel('Concentration (HHI)')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.set_ylim(0,1)
# ax.legend()
# ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_sector5_hhi_worker_weighted.png', dpi=300,bbox_inches='tight') # Used by paper and slides


# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,l), occ4_probabilities_by_iota.loc[occ4_probabilities_by_iota.index.repeat(occ4_probabilities_by_iota.i_weights)]['hhi'].sort_values(),s=5,label='4-digit occupation')
# ax.scatter(np.arange(1,l), gamma_probabilities_by_iota.loc[gamma_probabilities_by_iota.index.repeat(gamma_probabilities_by_iota.i_weights)]['hhi'].sort_values(), s=3,label='Market', marker='+')
# ax.set_xlabel('Workers (sorted by employment HHI)')
# ax.set_ylabel('Concentration (HHI)')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.set_ylim(0,1)
# ax.legend()
# ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_occ4_hhi_worker_weighted.png', dpi=300,bbox_inches='tight') #Used by slides


# # gamma hhi
# ll = iota_probabilities_by_gamma.g_weights.sum()+1

# fig, ax = plt.subplots()
# ax.scatter(np.arange(1,ll), occ4_probabilities_by_gamma.loc[occ4_probabilities_by_gamma.index.repeat(occ4_probabilities_by_gamma.g_weights)]['hhi'].sort_values(),s=5,label='4-digit occupation')
# ax.scatter(np.arange(1,ll), iota_probabilities_by_gamma.loc[iota_probabilities_by_gamma.index.repeat(iota_probabilities_by_gamma.g_weights)]['hhi'].sort_values(),s=5,label='Worker type', marker='+')
# ax.set_xlabel('Markets (sorted by hiring HHI)')
# ax.set_ylabel('Concentration (HHI)')
# ax.set_xticklabels([])
# ax.set_xticks([])
# ax.set_ylim(0,1)
# ax.legend()
# ax.figure.savefig(figuredir+fname_stub+'_gamma_iota_occ4_hhi_market_weighted.png', dpi=300,bbox_inches='tight') #Used by slides







# # ../../Code/aug2021/results/concentration_figures_gamma_iota_occ4_hhi_market_weighted
# # ../../Code/aug2021/results/concentration_figures_iota_gamma_occ4_hhi_worker_weighted
# # ../../Code/aug2021/results/concentration_figures_iota_gamma_sector5_hhi_worker_weighted
# # ../../Code/aug2021/results/concentration_figures_iota_gamma_sector_hhi_worker_weighted



