#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  4 14:04:06 2021

@author: jfogel
"""



fname_stub = 'concentration_figures'
data_full = pd.read_csv(mle_data_filename)
data_full = data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1)]

# iota concentration
crosstab_iota_sector = pd.crosstab(index = data_full.iota, columns = data_full.sector_IBGE)
sector_probabilities_by_iota = crosstab_iota_sector.div(crosstab_iota_sector.sum(axis=1),axis=0).reset_index()
sector_probabilities_by_iota['hhi'] = sector_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)

crosstab_iota_sector5 = pd.crosstab(index = data_full.iota, columns = data_full.clas_cnae20)
sector5_probabilities_by_iota = crosstab_iota_sector5.div(crosstab_iota_sector5.sum(axis=1),axis=0).reset_index()
sector5_probabilities_by_iota['hhi'] = sector5_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)

crosstab_iota_gamma = pd.crosstab(index = data_full.iota, columns = data_full.gamma).drop(columns=0)
gamma_probabilities_by_iota = crosstab_iota_gamma.div(crosstab_iota_gamma.sum(axis=1),axis=0).reset_index()
gamma_probabilities_by_iota['hhi'] = gamma_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)

crosstab_iota_occ4 = pd.crosstab(index = data_full.iota, columns = data_full.occ4_first_recode)
occ4_probabilities_by_iota = crosstab_iota_occ4.div(crosstab_iota_occ4.sum(axis=1),axis=0).reset_index()
occ4_probabilities_by_iota['hhi'] = occ4_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)

# gamma concentration

crosstab_gamma_iota = pd.crosstab(index = data_full.gamma, columns = data_full.iota).drop(index=0)
iota_probabilities_by_gamma = crosstab_gamma_iota.div(crosstab_gamma_iota.sum(axis=1),axis=0).reset_index()
iota_probabilities_by_gamma['hhi'] = iota_probabilities_by_gamma.drop(columns='gamma').pow(2).sum(axis=1)

crosstab_gamma_occ4 = pd.crosstab(index = data_full.gamma, columns = data_full.occ4_first_recode).drop(index=0)
occ4_probabilities_by_gamma = crosstab_gamma_occ4.div(crosstab_gamma_occ4.sum(axis=1),axis=0).reset_index()
occ4_probabilities_by_gamma['hhi'] = occ4_probabilities_by_gamma.drop(columns='gamma').pow(2).sum(axis=1)




# Public sector
sector_index = 13
fig, ax = plt.subplots()
ax.scatter(np.arange(1,291), sector_probabilities_by_iota[sector_index].sort_values(),s=5)
ax.set_xlabel('Worker types (sorted by shocked sector share)')            
ax.set_ylabel('Shocked sector share of Employment')
ax.set_xticklabels([])
ax.set_xticks([])
ax.figure.savefig(figuredir+fname_stub+'_iota_shocked_sector_share.png') #Unused




fig, ax = plt.subplots()
ax.scatter(np.arange(1,291), sector_probabilities_by_iota['hhi'].sort_values(),s=5)
ax.set_xlabel('Worker types (sorted by sector employment HHI)')
ax.set_ylabel('Sector employment HHI')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.figure.savefig(figuredir+fname_stub+'_iota_sector_hhi.png') #Unused


fig, ax = plt.subplots()
ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(),s=5)
ax.set_xlabel('Worker types (sorted by market employment HHI)')
ax.set_ylabel('Market employment HHI')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_hhi.png') #Unused


fig, ax = plt.subplots()
ax.scatter(np.arange(1,291), sector_probabilities_by_iota['hhi'].sort_values(),s=5,label='Sector')
ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(), s=5,label='Market', marker='+')
ax.set_xlabel('Worker types (sorted by employment HHI)')
ax.set_ylabel('Concentration (HHI)')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.legend()
ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_sector_hhi.png', dpi=300,bbox_inches='tight') #Unused


fig, ax = plt.subplots()
ax.scatter(np.arange(1,291), sector5_probabilities_by_iota['hhi'].sort_values(),s=5,label='5-digit industry')
ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(), s=5,label='Market', marker='+')
ax.set_xlabel('Worker types (sorted by employment HHI)')
ax.set_ylabel('Concentration (HHI)')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.legend()
ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_sector5_hhi.png', dpi=300,bbox_inches='tight') #Unused


fig, ax = plt.subplots()
ax.scatter(np.arange(1,291), occ4_probabilities_by_iota['hhi'].sort_values(),s=5,label='4-digit occupation')
ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(), s=5,label='Market', marker='+')
ax.set_xlabel('Worker types (sorted by employment HHI)')
ax.set_ylabel('Concentration (HHI)')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.legend()
ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_occ4_hhi.png', dpi=300,bbox_inches='tight') #Unused


# gamma hhi
fig, ax = plt.subplots()
ax.scatter(np.arange(1,428), occ4_probabilities_by_gamma['hhi'].sort_values(),s=5,label='4-digit occupation')
ax.scatter(np.arange(1,428), iota_probabilities_by_gamma['hhi'].sort_values(), s=5,label='Worker type', marker='+')
ax.set_xlabel('Markets (sorted by employment HHI)')
ax.set_ylabel('Concentration (HHI)')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.legend()
ax.figure.savefig(figuredir+fname_stub+'_gamma_iota_occ4_hhi.png', dpi=300,bbox_inches='tight') #Unused





factor = 1/mle_data_sums['m_i'].min().numpy()
i_weights = np.round(factor*np.array(mle_data_sums['m_i']),0)

sector_probabilities_by_iota['i_weights']  = i_weights
sector5_probabilities_by_iota['i_weights'] = i_weights
occ4_probabilities_by_iota['i_weights'] = i_weights
gamma_probabilities_by_iota['i_weights']  = i_weights

iota_probabilities_by_gamma['g_weights']  = mle_data_sums['sum_count_g'][1:428]
occ4_probabilities_by_gamma['g_weights']  = mle_data_sums['sum_count_g'][1:428]


l =sector_probabilities_by_iota.loc[sector_probabilities_by_iota.index.repeat(sector_probabilities_by_iota.i_weights)].shape[0]+1

 



fig, ax = plt.subplots()
ax.scatter(np.arange(1,l), sector_probabilities_by_iota.loc[sector_probabilities_by_iota.index.repeat(sector_probabilities_by_iota.i_weights)]['hhi'].sort_values(),s=3,label='Sector')
ax.scatter(np.arange(1,l), gamma_probabilities_by_iota.loc[gamma_probabilities_by_iota.index.repeat(gamma_probabilities_by_iota.i_weights)]['hhi'].sort_values(), s=3,label='Market', marker='+')
ax.set_xlabel('Workers (sorted by employment HHI)')
ax.set_ylabel('Concentration (HHI)')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.legend()
ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_sector_hhi_worker_weighted.png', dpi=300,bbox_inches='tight') # Used by paper and slides


fig, ax = plt.subplots()
ax.scatter(np.arange(1,l), sector5_probabilities_by_iota.loc[sector5_probabilities_by_iota.index.repeat(sector5_probabilities_by_iota.i_weights)]['hhi'].sort_values(),s=5,label='5-digit industry')
ax.scatter(np.arange(1,l), gamma_probabilities_by_iota.loc[gamma_probabilities_by_iota.index.repeat(gamma_probabilities_by_iota.i_weights)]['hhi'].sort_values(), s=3,label='Market', marker='+')
ax.set_xlabel('Workers (sorted by employment HHI)')
ax.set_ylabel('Concentration (HHI)')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.legend()
ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_sector5_hhi_worker_weighted.png', dpi=300,bbox_inches='tight') # Used by paper and slides


fig, ax = plt.subplots()
ax.scatter(np.arange(1,l), occ4_probabilities_by_iota.loc[occ4_probabilities_by_iota.index.repeat(occ4_probabilities_by_iota.i_weights)]['hhi'].sort_values(),s=5,label='4-digit occupation')
ax.scatter(np.arange(1,l), gamma_probabilities_by_iota.loc[gamma_probabilities_by_iota.index.repeat(gamma_probabilities_by_iota.i_weights)]['hhi'].sort_values(), s=3,label='Market', marker='+')
ax.set_xlabel('Workers (sorted by employment HHI)')
ax.set_ylabel('Concentration (HHI)')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.legend()
ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_occ4_hhi_worker_weighted.png', dpi=300,bbox_inches='tight') #Used by slides


# gamma hhi
ll = iota_probabilities_by_gamma.g_weights.sum()+1

fig, ax = plt.subplots()
ax.scatter(np.arange(1,ll), occ4_probabilities_by_gamma.loc[occ4_probabilities_by_gamma.index.repeat(occ4_probabilities_by_gamma.g_weights)]['hhi'].sort_values(),s=5,label='4-digit occupation')
ax.scatter(np.arange(1,ll), iota_probabilities_by_gamma.loc[iota_probabilities_by_gamma.index.repeat(iota_probabilities_by_gamma.g_weights)]['hhi'].sort_values(),s=5,label='Worker type', marker='+')
ax.set_xlabel('Markets (sorted by hiring HHI)')
ax.set_ylabel('Concentration (HHI)')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.legend()
ax.figure.savefig(figuredir+fname_stub+'_gamma_iota_occ4_hhi_market_weighted.png', dpi=300,bbox_inches='tight') #Used by slides







# ../../Code/aug2021/results/concentration_figures_gamma_iota_occ4_hhi_market_weighted
# ../../Code/aug2021/results/concentration_figures_iota_gamma_occ4_hhi_worker_weighted
# ../../Code/aug2021/results/concentration_figures_iota_gamma_sector5_hhi_worker_weighted
# ../../Code/aug2021/results/concentration_figures_iota_gamma_sector_hhi_worker_weighted

