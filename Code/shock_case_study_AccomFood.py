#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 29 06:41:05 2021

This code only runs after running the code in case_study_func excluding the function definition so that everything is defined in local scope. This is super sloppy and should be cleaned up at some point.

@author: jfogel
"""

import torch
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pickle
import os 

from model_fit_func import compute_expected_Phi


print('------------------------------------------------------------------------------------')
print('Running shock_case_study_AccomFood')
print('------------------------------------------------------------------------------------')


(w_dict, j_dict) = pickle.load(open('Data/Derived/occ_counts/occ_counts.p', "rb"), encoding='bytes')

# We don't use these. Only use the 4th most shocked, defined below
# Occupation counts for the most shocked iotas
# print('Occupation counts for the most shocked iotas:')
# for i in range(0,9):
#     print(i+1)
#     #(w_dict, j_dict) = occ_counts_by_type(mle_data_filename, 0)
#     #pickle.dump((w_dict, j_dict), open('Results/'+fname_stub+'_occ_counts.p', 'wb'))
#     #(w_dict, j_dict) = pickle.load(open('Results/'+fname_stub+'_occ_counts.p', "rb"), encoding='bytes')
#     print(w_dict[most_shocked_iotas[i]].head(20))
#     print('')
#     w_dict[most_shocked_iotas[i]]['cbo2002'] = w_dict[most_shocked_iotas[i]]['cbo2002'].astype(int)
#     w_dict[most_shocked_iotas[i]]['share'] = np.round(w_dict[most_shocked_iotas[i]]['share'],3)
#     w_dict[most_shocked_iotas[i]].head(10).to_latex(index=False, buf='Results/' + fname_stub + "_most_shocked_iota_occ_counts_" + str(i) + ".tex", label='table:'+fname_stub + '_most_shocked_iota_occ_counts_' + str(i), caption=r'Occupation counts for $\iota=$'+str(most_shocked_iotas[i]), columns=['cbo2002','description', 'share'], header=['Occ Code','Occ Description','Occ share'], multicolumn=False)

#     df = pd.DataFrame({'Sector':sector_labels_abbr,'Share (%)':np.round(sector_probabilities_by_iota.loc[sector_probabilities_by_iota.iota==most_shocked_iotas[i]].drop(columns=['iota','hhi']).values,3).flatten()*100})
#     df.sort_values(by='Share (%)', ascending=False).to_latex('Results/'+fname_stub+'_sector_share_table_' + str(i) +'.tex', index=False)
#     print( df.sort_values(by='Share (%)', ascending=False))
#     print('')


#     df_g = pd.DataFrame(equi_pre['p_ig'][most_shocked_iotas[i]-1,:]).reset_index().rename(columns={'index':'gamma',0:'p_ig'}).sort_values(by='p_ig', ascending=False) #94% work in gamma=106, gamma=2, or non-employment

#     df_g_print =pd.DataFrame(np.round(df_g.loc[df_g.gamma!=0].p_ig/df_g.loc[df_g.gamma!=0].p_ig.sum(),3)*100).reset_index().head(10).rename(columns={'index':'Market ($\gamma$)','p_ig':'Share (\%)'})
#     df_g_print.to_latex('Results/'+fname_stub+'_gamma_share_table_' + str(i) + '.tex', index=False, escape=False)
#     print(df_g_print, '\n')


'''
Candidates:
    - 6th most shocked iota (iota=134) from construction shock. It's a variety of telecoms workers spread across many sectors'
    - 5th most shocked iota (iota=120) from manufacturing shock. Primarily welders. 44% manufacturing, 12% construction, 11% motor vehicles
    - 4th most shocked iota (iota=91)  from transport shock, Bus and truck drivers. 42% transport, 13% utilities, 10% motor vehicles...
    - 4th most shocked iota (iota=64) from AccomFood
    
'''









# Let's investigate the 4th most-shocked iotas


# We use this one
corr = np.corrcoef(delta_earn, sector_shares_iota[sector_index])[0,1]
fig, ax = plt.subplots()
ax.scatter(delta_earn, sector_shares_iota[sector_index], s=1)  
ax.text(0.8, 0.9, 'Corr = ' + str(np.round(corr,3)), verticalalignment='top', transform=ax.transAxes) 
ax.set_xlabel('Change in earnings (by worker type)')
ax.set_ylabel('Share of worker type employed in shocked sector')
ax.figure.savefig('Results/'+fname_stub+'_iota_scatter.png')

# We use this one
highlight_index = most_shocked_iotas[3]-1
fig, ax = plt.subplots()
ax.scatter(delta_earn, sector_shares_iota[sector_index], s=1)     
ax.scatter(delta_earn[highlight_index],sector_shares_iota[sector_index].iloc[highlight_index], c='r', s=1)
ax.annotate(r'$\iota=$'+str(highlight_index+1), xy=(delta_earn[highlight_index],sector_shares_iota[sector_index].iloc[highlight_index]), xytext=(.1,.5) ,arrowprops=dict(facecolor='black',  arrowstyle='fancy'), fontsize=12, textcoords='axes fraction')
ax.text(0.8, 0.9, 'Corr = ' + str(np.round(corr,3)), verticalalignment='top', transform=ax.transAxes) 
ax.set_xlabel('Change in earnings (by worker type)')
ax.set_ylabel('Share of worker type employed in shocked sector')
ax.figure.savefig('Results/'+fname_stub+'_iota_scatter_highlight.png')





(w_dict, j_dict) = pickle.load(open(homedir + '/Networks/Code/aug2021/dump/occ_counts.p', "rb"), encoding='bytes')

# Look at 4th most-shocked iota
i = 3

# We use this one
# Sector shares
df = pd.DataFrame({'Sector':sector_labels_abbr,'Share (%)':np.round(sector_probabilities_by_iota.loc[sector_probabilities_by_iota.iota==most_shocked_iotas[i]].drop(columns=['iota','hhi']).values,3).flatten()*100})
df.sort_values(by='Share (%)', ascending=False).to_latex('Results/'+fname_stub+'_sector_share_table_' + str(i) +'.tex', index=False)
print( df.sort_values(by='Share (%)', ascending=False))
print('')

# We use this one
# Occ counts
w_dict[most_shocked_iotas[i]]['cbo2002'] = w_dict[most_shocked_iotas[i]]['cbo2002'].astype(int)
w_dict[most_shocked_iotas[i]]['share'] = np.round(w_dict[most_shocked_iotas[i]]['share'],3)
w_dict[most_shocked_iotas[i]].head(10).to_latex(index=False, buf='Results/' + fname_stub + "_most_shocked_iota_occ_counts_" + str(i) + ".tex", label='table:'+fname_stub + '_most_shocked_iota_occ_counts_' + str(i), caption=r'Occupation counts for $\iota=$'+str(most_shocked_iotas[i]), columns=['cbo2002','description', 'share'], header=['Occ Code','Occ Description','Occ share'], multicolumn=False)


# Gamma counts for iota of interest
print('Gamma counts for the most shocked iotas:')
df_g = pd.DataFrame(equi_pre['p_ig'][most_shocked_iotas[i]-1,:]).reset_index().rename(columns={'index':'gamma',0:'p_ig'}).sort_values(by='p_ig', ascending=False) #94% work in gamma=106, gamma=2, or non-employment

# We use this one
df_g_print =pd.DataFrame(np.round(df_g.loc[df_g.gamma!=0].p_ig/df_g.loc[df_g.gamma!=0].p_ig.sum(),3)*100).reset_index().head(10).rename(columns={'index':'Market ($\gamma$)','p_ig':'Share (\%)'})
df_g_print.to_latex('Results/'+fname_stub+'_gamma_share_table_' + str(i) + '.tex', index=False, escape=False)
print(df_g_print, '\n')

gamma_of_interest = df_g_print['Market ($\gamma$)'][0]

# Most shocked gammas
df_w = pd.DataFrame({'gamma':np.arange(1,428),'delta_w_g':equi_shock['w_g']/equi_pre['w_g']})
df_w.sort_values(by='delta_w_g', inplace=True) 
df_w['delta_w_g']       = (df_w['delta_w_g']-1)*100
df_w['delta_w_g_round'] = np.round(df_w['delta_w_g'],2)
df_w_print = df_w[['gamma','delta_w_g_round']].head(20).rename(columns={'gamma':'Market ($\gamma$)','delta_w_g_round':'Earnings decline (\%)'})
df_w_print.head(10).to_latex('Results/'+fname_stub+'_most_shocked_gammas_table.tex', index=False, escape=False, caption='10 markets with the largest shock-induced wage decrease')
print('Most-shocked gammas:')
print(df_w_print, '\n')

most_shocked_gammas = df_w.reset_index().gamma
most_shocked_gamma = most_shocked_gammas[0]
#############
# Investigate the most-shocked gamma

pd.options.display.max_columns=20
pd.options.display.width=200

np.set_printoptions(linewidth=200)
np.set_printoptions(suppress=True)
 
 
# What sectors is gamma=106 most likely to be used by?
df_l_gs = pd.DataFrame(equi_shock['l_gs_demand'].numpy())
df_l_gs.columns = np.arange(1,16)
df_l_gs['gamma'] = np.arange(1,428)
np.round((df_l_gs.loc[df_l_gs.gamma==gamma_of_interest].drop(columns='gamma').values)/(df_l_gs.loc[df_l_gs.gamma==gamma_of_interest].drop(columns='gamma').sum(axis=1).values),4)
# 77% of gamma=106's tasks are supplied to the manufacturing sector. 16% are supplied to health and education. So what's going on here is that 
# 1. Shock hits manufacturing
# 2. Shock changes wages for gammas that are used heavily by manufacturing
# 3. Some of the worker types that supply lots of labor to the gammas that are most affected aren't actually in manufacturing. Think about a carpenter who works for a school. The carpenter will probably be more affected by a shock to construction, since that's where most of the demand for tasks the carpenter has a comparative advantage in comes from. So it makes more sense to measure the carpenter's exposure to their gamma than to their sector. 


# Divide by total tasks supplied by each gamma (row sums)
df_l_gs_rescaled = pd.DataFrame(np.round(np.divide(df_l_gs.drop(columns='gamma').values,np.transpose(np.tile(df_l_gs.drop(columns='gamma').sum(axis=1).values,(15,1)))),4))
df_l_gs_rescaled.columns = np.arange(1,16)
df_l_gs_rescaled['gamma'] = np.arange(1,428)

df_l_gs_rescaled[[sector_index,'gamma']].sort_values(by=sector_index, ascending=False)
# gamma=106 supplies a greater share of its tasks to the manufacturing sector than any other gamma


df_l_gs_rescaled_print = df_l_gs_rescaled[[sector_index,'gamma']].sort_values(by=sector_index, ascending=False).rename(columns={'gamma':'Market ($\gamma$)',sector_index:'Share (\%)'}).head(15)
df_l_gs_rescaled_print.to_latex('Results/'+fname_stub+'_gamma_supply_to_sector_table.tex', index=False, escape=False)

row = equi_pre['l_gs_demand'].numpy()[gamma_of_interest-1,:]
np.round(row/row.sum(),3)




# Bar chart of gamma shocked sector shares and delta w_g
df_bar_chart = df_l_gs_rescaled[[sector_index,'gamma']].merge(df_w, on='gamma').sort_values(sector_index)
# width = 0.35
# fig, ax = plt.subplots()
# ax.bar(np.arange(1,428), df_bar_chart[sector_index], width, label='Shocked sector share')
# ax.bar(np.arange(1,428), df_bar_chart['delta_w_g'], width, label='Pct $\Delta w_{\gamma}$', alpha=.7)
# ax.set_xlabel('Markets (sorted by Shocked Sector share)')
# ax.legend()

# Scatterplot of gamma manufacturing shares and delta w_g
corr = np.corrcoef(df_bar_chart['delta_w_g'], df_bar_chart[sector_index])[0,1]
fig, ax = plt.subplots()
ax.scatter(df_bar_chart['delta_w_g'], df_bar_chart[sector_index], s=1)  
ax.text(0.78, 0.9, 'Corr = ' + str(np.round(corr,3)), verticalalignment='top', transform=ax.transAxes) 
ax.set_xlabel('Change in $w_{\gamma}$ (by market)')
ax.set_ylabel("Share of market's tasks supplied to shocked sector")
ax.figure.savefig('Results/'+fname_stub+'_gamma_scatter.png')


# Scatterplot of gamma manufacturing shares and delta w_g
g_highlight_index = gamma_of_interest-1
corr = np.corrcoef(df_bar_chart['delta_w_g'], df_bar_chart[sector_index])[0,1]
fig, ax = plt.subplots()
ax.scatter(df_bar_chart['delta_w_g'], df_bar_chart[sector_index], s=1)  
ax.scatter(df_bar_chart['delta_w_g'][g_highlight_index],df_bar_chart[sector_index][g_highlight_index], c='r', s=1)
ax.annotate(r'$\gamma=$'+str(g_highlight_index+1), xy=(df_bar_chart['delta_w_g'][g_highlight_index],df_bar_chart[sector_index][g_highlight_index]), xytext=(.6, .05),arrowprops=dict(facecolor='black',  arrowstyle='fancy'), fontsize=12, textcoords='axes fraction')
ax.text(0.78, 0.9, 'Corr = ' + str(np.round(corr,3)), verticalalignment='top', transform=ax.transAxes) 
ax.set_xlabel('Change in $w_{\gamma}$ (by market)')
ax.set_ylabel("Share of market's tasks supplied to shocked sector")
ax.figure.savefig('Results/'+fname_stub+'_gamma_scatter_highlight_' + str(gamma_of_interest) + '.png')


g = gamma_of_interest
print('Occupation counts for the most shocked gammas:')
j_dict[most_shocked_gammas[g]]['cbo2002'] = j_dict[most_shocked_gammas[g]]['cbo2002'].astype(int)
j_dict[most_shocked_gammas[g]]['share'] = np.round(j_dict[most_shocked_gammas[g]]['share'],3)
j_dict[most_shocked_gammas[g]].head(10).to_latex(index=False, buf='Results/' + fname_stub + "_most_shocked_gamma_occ_counts_" + str(g) + ".tex", label='table:'+fname_stub + '_most_shocked_gamma_occ_counts_' + str(g), caption=r'Occupation counts for $\gamma=$'+str(most_shocked_gammas[g]), columns=['cbo2002','description', 'share'], header=['Occ Code','Occ Description','Occ share'], multicolumn=False)



fig, ax = plt.subplots()
ax.scatter(np.arange(1,291), sector_probabilities_by_iota[sector_index].sort_values(),s=5)
ax.set_xlabel('Worker types (sorted by shocked sector share)')            
ax.set_ylabel('Shcoked sector share of Employment')
ax.set_xticklabels([])
ax.set_xticks([])
ax.figure.savefig('Results/'+fname_stub+'_iota_shocked_sector_share.png')



sector_probabilities_by_iota['hhi'] = sector_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)


crosstab_iota_gamma = pd.crosstab(index = fake_data_full.iota.loc[fake_data_full.year==2009], columns = fake_data_full.gamma.loc[fake_data_full.year==2009]).drop(columns=0)
gamma_probabilities_by_iota = crosstab_iota_gamma.div(crosstab_iota_gamma.sum(axis=1),axis=0).reset_index()
gamma_probabilities_by_iota['hhi'] = gamma_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)

fig, ax = plt.subplots()
ax.scatter(np.arange(1,291), sector_probabilities_by_iota['hhi'].sort_values(),s=5)
ax.set_xlabel('Worker types (sorted by sector employment HHI)')
ax.set_ylabel('Sector employment HHI')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.figure.savefig('Results/'+fname_stub+'_iota_sector_hhi.png')


fig, ax = plt.subplots()
ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(),s=5)
ax.set_xlabel('Worker types (sorted by market employment HHI)')
ax.set_ylabel('Market employment HHI')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.figure.savefig('Results/'+fname_stub+'_iota_gamma_hhi.png')


fig, ax = plt.subplots()
ax.scatter(np.arange(1,291), sector_probabilities_by_iota['hhi'].sort_values(),s=5,label='Sector')
ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(), s=5,label='Market', marker='+')
ax.set_xlabel('Worker types (sorted by employment HHI)')
ax.set_ylabel('HHI')
ax.set_xticklabels([])
ax.set_xticks([])
ax.set_ylim(0,1)
ax.legend()
ax.figure.savefig('Results/'+fname_stub+'_iota_gamma_sector_hhi.png', dpi=300)



j_dict[47]['share'] = np.round(j_dict[47]['share'],3)
j_dict[47].head(10).to_latex(index=False, buf='Results/' + fname_stub + "_gamma_47_occ_counts.tex", label='table:'+fname_stub + '_gamma_47_occ_counts', columns=['cbo2002','description', 'share'], header=['Occ Code','Occ Description','Occ share'], multicolumn=False)




