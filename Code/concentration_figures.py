#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct  4 14:04:06 2021

@author: jfogel
"""

from matplotlib.collections import LineCollection


################
## Make into function or loop and then incorporate occ2Xmeso
##################


fname_stub = 'concentration_figures'
data_full = pd.read_csv(mle_data_filename)
data_full = data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1)]


def concentration_figures(xvar, xvarlabel, yvarlist, yvarlabels):
    # Example usage: 
    #xvar = 'iota'
    #xvarlabel = 'Workers'
    #yvarlist = ['sector_IBGE','gamma']
    #yvarlabels = {'sector_IBGE':'Sector','gamma':'Market'}
    fig, ax = plt.subplots()
    savename_str = ''
    for yvar in yvarlist:
        print(yvar)
        crosstab = pd.crosstab(index = data_full[xvar], columns = data_full[yvar])
        yvar_probabilities_by_xvar = crosstab.div(crosstab.sum(axis=1),axis=0).reset_index()
        yvar_probabilities_by_xvar['hhi'] = yvar_probabilities_by_xvar.drop(columns=xvar).pow(2).sum(axis=1)
        xvar_counts = data_full[xvar].value_counts().reset_index().rename(columns={'index':xvar,xvar:'count'})
        # Normalize the counts by the minimum count and round to an integer. This avoids having so many points in the scatter plot that it won't print.
        xvar_counts['count'] = np.round(xvar_counts['count']/xvar_counts['count'].min())
        yvar_probabilities_by_xvar = yvar_probabilities_by_xvar.merge(xvar_counts, on=xvar)
        weighted_hhis = yvar_probabilities_by_xvar.loc[yvar_probabilities_by_xvar.index.repeat(yvar_probabilities_by_xvar['count'])]['hhi'].sort_values()
        ax.scatter(np.arange(1,weighted_hhis.shape[0]+1), weighted_hhis,s=3,label=yvarlabels[yvar])
        del crosstab, yvar_probabilities_by_xvar, xvar_counts, weighted_hhis
        savename_str = savename_str + '_' + yvar
    ax.set_xlabel(xvarlabel + ' (sorted by employment HHI)')
    ax.set_ylabel('Concentration (HHI)')
    ax.set_xticklabels([])
    ax.set_xticks([])
    ax.set_ylim(0,1)
    ax.legend()
    ax.figure.savefig(figuredir+fname_stub+'_'+xvar+savename_str+'_test.png', dpi=300,bbox_inches='tight') # Used by paper and slides

concentration_figures('iota', 'Workers', ['sector_IBGE','gamma'], {'sector_IBGE':'Sector','gamma':'Market'})
concentration_figures('iota', 'Workers', ['clas_cnae20','gamma'], {'clas_cnae20':'5-Digit Industry','gamma':'Market'})
concentration_figures('iota', 'Workers', ['occ2Xmeso','gamma'], {'occ2Xmeso':'Occ2 X Meso Region','gamma':'Market'})





fig, ax = plt.subplots()

for yvar in yvarlist:
    print(yvar)
    crosstab = pd.crosstab(index=data_full[xvar], columns=data_full[yvar])
    yvar_probabilities_by_xvar = crosstab.div(crosstab.sum(axis=1), axis=0).reset_index()
    yvar_probabilities_by_xvar['hhi'] = yvar_probabilities_by_xvar.drop(columns=xvar).pow(2).sum(axis=1)
    xvar_counts = data_full[xvar].value_counts().reset_index().rename(columns={'index': xvar, xvar: 'count'})
    yvar_probabilities_by_xvar = yvar_probabilities_by_xvar.merge(xvar_counts, on=xvar)
    # Sort the values
    weighted_hhis_sorted = yvar_probabilities_by_xvar['hhi'].sort_values()
    # List to hold the line segments
    lines = []
    x_values = np.arange(1, weighted_hhis_sorted.shape[0] + 1)
    for x, y, count in zip(x_values, weighted_hhis_sorted, yvar_probabilities_by_xvar['count']):
        # Compute the width based on the count
        width = count / 100  # Adjust the division factor to control the width
        # Append the line segment as (x1, y1), (x2, y2) to the lines list
        lines.append([(x - width, y), (x + width, y)])
    # Create a LineCollection from the lines and add to the axes
    lc = LineCollection(lines, color='blue') # Adjust color as needed
    ax.add_collection(lc)

    
ax.set_xlabel('Workers (sorted by employment HHI)')
ax.set_ylabel('Concentration (HHI)')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.legend()
ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_sector_hhi_worker_weighted_test.png', dpi=300,bbox_inches='tight') # Used by paper and slides

# Rest of the code...


# Rest of the code...



# Issue: the way I'm doing the weighting is running out of memory

'''
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
'''

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




factor = 1/mle_data_sums['m_i'].min().numpy()
i_weights = np.round(factor*np.array(mle_data_sums['m_i']),0)

sector_probabilities_by_iota['i_weights']  = i_weights
sector5_probabilities_by_iota['i_weights'] = i_weights
occ4_probabilities_by_iota['i_weights'] = i_weights
gamma_probabilities_by_iota['i_weights']  = i_weights

iota_probabilities_by_gamma['g_weights']  = mle_data_sums['sum_count_g'][1:]
occ4_probabilities_by_gamma['g_weights']  = mle_data_sums['sum_count_g'][1:]


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

