#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 28 15:59:18 2021

@author: jfogel
"""
import torch
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pickle
import os 

from model_fit_func import compute_expected_Phi


# fake_data_filename=fake_data_Public_filename
# equi_shock = equi_Public
# fname_stub = 'shock_case_study_Public'
# sector_index = 13
    

# fake_data_filename=fake_data_const_filename
# equi_shock = equi_const
# fname_stub = 'shock_case_study_const'
# sector_index = 5

# fake_data_filename=fake_data_AccomFood_filename
# equi_shock = equi_AccomFood
# fname_stub = 'shock_case_study_AccomFood'
# sector_index = 8


# fake_data_filename=fake_data_Extractive_filename
# equi_shock = equi_Extractive
# fname_stub = 'shock_case_study_Extractive'
# sector_index = 2

# fake_data_filename=fake_data_AccomFood_filename
# equi_shock = equi_AccomFood
# fname_stub = 'shock_case_study_AccomFood'
# sector_index = 8

# fake_data_filename=fake_data_Vehicles_filename
# equi_shock = equi_Vehicles
# fname_stub = 'shock_case_study_Vehicles'
# sector_index = 6

# fake_data_filename=fake_data_Finance_filename
# equi_shock = equi_Finance
# fname_stub = 'shock_case_study_Finance'
# sector_index = 10


# fake_data_filename=fake_data_RealEstate_filename
# equi_shock = equi_RealEstate
# fname_stub = 'shock_case_study_RealEstate'
# sector_index = 11


# fake_data_filename=fake_data_ProfSciTech_filename
# equi_shock = equi_ProfSciTech
# fname_stub = 'shock_case_study_ProfSciTech'
# sector_index = 12

def case_study(mle_data_filename, fake_data_filename, equi_shock, equi_pre, psi_hat, mle_estimates, figuredir, fname_stub, sector_index):
    
    homedir = os.path.expanduser('~')

    upperlim = 800 # Drop very large wages

    
    if 1==1:
        sector_labels_abbr = ["Agriculture, livestock, forestry, fisheries and aquaculture",
                      "Extractive industries",
                      "Manufacturing industries",
                      "Utilities",
                      "Construction",
                      "Trade and repair of motor vehicles and motorcycles",
                      "Transport, storage and mail",
                      "Accommodation and food",
                      "Information and communication",
                      "Financial, insurance and related services",
                      "Real estate activities",
                      "Professional, scientific and technical svcs",
                      "Public admin, defense, educ, health and soc security",
                      "Private health and education",
                      "Arts, culture, sports and recreation and other svcs"]


    
    fake_data_full = pd.read_csv(fake_data_filename)
    
    drop = fake_data_full.real_hrly_wage_dec>upperlim
    fake_data_full = fake_data_full.loc[drop==False]

    
    crosstab_iota_sector = pd.crosstab(index = fake_data_full.iota.loc[fake_data_full.year==2009], columns = fake_data_full.sector_IBGE.loc[fake_data_full.year==2009])
    sector_shares_iota = crosstab_iota_sector.div(crosstab_iota_sector.sum(axis=1),axis=0)
    
    E_phi_N_pre  = compute_expected_Phi(equi_pre['p_ig'][:,1:] ,  psi_hat, equi_pre['w_g'],   mle_estimates['sigma_hat'])
    E_phi_N_post = compute_expected_Phi(equi_shock['p_ig'][:,1:], psi_hat, equi_shock['w_g'], mle_estimates['sigma_hat'])
    
    delta_earn     = E_phi_N_post - E_phi_N_pre
    delta_earn_pct = E_phi_N_post / E_phi_N_pre - 1
       
    temp =fake_data_full[['iota','ln_real_hrly_wage_dec']].loc[fake_data_full.year==2009]
    temp['ln_real_hrly_wage_dec'] = temp['ln_real_hrly_wage_dec'].fillna(0)
    temp.groupby('iota')['ln_real_hrly_wage_dec'].mean()
    
    
    
    
    
    # Let's investigate the most-shocked iotas
    delta_earn_df = pd.DataFrame({'iota':np.arange(1,291),'delta_earn':delta_earn})
    delta_earn_df.sort_values(by='delta_earn', inplace=True)
    
    # fig, ax = plt.subplots()
    # ax.hist(delta_earn_df.delta_earn, bins=40)
    # ax.set_title('Distribution of earnings changes by $\iota$')
    # ax.figure.savefig(figuredir+fname_stub+'_delta_earn_by_iota_hist.png')
    
    
    # Print most shocked iotas
    most_shocked_iotas = delta_earn_df.loc[delta_earn_df.delta_earn<0].iota.values
    print('3 most-shocked iotas: ', most_shocked_iotas[0],most_shocked_iotas[1],most_shocked_iotas[2])
    
    
    
    
    # corr = np.corrcoef(delta_earn, sector_shares_iota[sector_index])[0,1]
    # fig, ax = plt.subplots()
    # ax.scatter(delta_earn, sector_shares_iota[sector_index], s=1)  
    # ax.text(0.8, 0.9, 'Corr = ' + str(np.round(corr,3)), verticalalignment='top', transform=ax.transAxes) 
    # ax.set_xlabel('Change in earnings (by worker type)')
    # ax.set_ylabel('Share of worker type employed in shocked sector')
    # ax.figure.savefig(figuredir+fname_stub+'_iota_scatter.png')
    
    
    highlight_index = most_shocked_iotas[0]-1
    # fig, ax = plt.subplots()
    # ax.scatter(delta_earn, sector_shares_iota[sector_index], s=1)     
    # ax.scatter(delta_earn[highlight_index],sector_shares_iota[sector_index].iloc[highlight_index], c='r', s=1)
    # ax.annotate(r'$\iota=$'+str(highlight_index+1), xy=(delta_earn[highlight_index],sector_shares_iota[sector_index].iloc[highlight_index]), xytext=(.1,.5) ,arrowprops=dict(facecolor='black',  arrowstyle='fancy'), fontsize=12, textcoords='axes fraction')
    # ax.text(0.8, 0.9, 'Corr = ' + str(np.round(corr,3)), verticalalignment='top', transform=ax.transAxes) 
    # ax.set_xlabel('Change in earnings (by worker type)')
    # ax.set_ylabel('Share of worker type employed in shocked sector')
    # ax.figure.savefig(figuredir+fname_stub+'_iota_scatter_highlight.png')
    
    
    
    
    
    sector_probabilities_by_iota = crosstab_iota_sector.div(crosstab_iota_sector.sum(axis=1),axis=0).reset_index()
    sector_probabilities_by_iota.loc[sector_probabilities_by_iota.iota==most_shocked_iotas[0]]
    sector_probabilities_by_iota.loc[sector_probabilities_by_iota.iota==most_shocked_iotas[1]] 
    sector_probabilities_by_iota.loc[sector_probabilities_by_iota.iota==most_shocked_iotas[2]]
    
    sector_probabilities_by_iota.loc[sector_probabilities_by_iota.iota==most_shocked_iotas[0]].drop(columns='iota').values
    
    
    print('Sector shares of 3 most-shocked iotas:')
    for i in range(0,3):
        df = pd.DataFrame({'Sector':sector_labels_abbr,'Share (%)':np.round(sector_probabilities_by_iota.loc[sector_probabilities_by_iota.iota==most_shocked_iotas[i]].drop(columns='iota').values,3).flatten()*100})
        df.sort_values(by='Share (%)', ascending=False).to_latex(figuredir+fname_stub+'_sector_share_table_' + str(i) +'.tex', index=False)
        print( df.sort_values(by='Share (%)', ascending=False))
        print('')
    
    
    (w_dict, j_dict) = pickle.load(open(homedir + '/Networks/Code/aug2021/dump/occ_counts.p', "rb"), encoding='bytes')

    # Occupation counts for the most shocked iotas
    print('Occupation counts for the most shocked iotas:')
    for i in range(0,4):
        #(w_dict, j_dict) = occ_counts_by_type(mle_data_filename, 0)
        #pickle.dump((w_dict, j_dict), open(figuredir+fname_stub+'_occ_counts.p', 'wb'))
        #(w_dict, j_dict) = pickle.load(open(figuredir+fname_stub+'_occ_counts.p', "rb"), encoding='bytes')
        print(w_dict[most_shocked_iotas[i]].head(20))
        print('')
        w_dict[most_shocked_iotas[i]]['cbo2002'] = w_dict[most_shocked_iotas[i]]['cbo2002'].astype(int)
        w_dict[most_shocked_iotas[i]]['share'] = np.round(w_dict[most_shocked_iotas[i]]['share'],3)
        # w_dict[most_shocked_iotas[i]].head(10).to_latex(index=False, buf=figuredir + fname_stub + "_most_shocked_iota_occ_counts_" + str(i) + ".tex", label='table:'+fname_stub + '_most_shocked_iota_occ_counts_' + str(i), caption=r'Occupation counts for $\iota=$'+str(most_shocked_iotas[i]), columns=['cbo2002','description', 'share'], header=['Occ Code','Occ Description','Occ share'], multicolumn=False)
    
    
    print('Gamma counts for the most shocked iotas:')
    for i in range(0,4):
        df_g = pd.DataFrame(equi_pre['p_ig'][most_shocked_iotas[i]-1,:]).reset_index().rename(columns={'index':'gamma',0:'p_ig'}).sort_values(by='p_ig', ascending=False) #94% work in gamma=106, gamma=2, or non-employment
    
        df_g_print =pd.DataFrame(np.round(df_g.loc[df_g.gamma!=0].p_ig/df_g.loc[df_g.gamma!=0].p_ig.sum(),3)*100).reset_index().head(10).rename(columns={'index':'Market ($\gamma$)','p_ig':'Share (\%)'})
        # df_g_print.to_latex(figuredir+fname_stub+'_gamma_share_table_' + str(i) + '.tex', index=False, escape=False)
        print(df_g_print, '\n')
    
    
    df_w = pd.DataFrame({'gamma':np.arange(1,428),'delta_w_g':equi_shock['w_g']/equi_pre['w_g']})
    df_w.sort_values(by='delta_w_g', inplace=True) 
    df_w['delta_w_g']       = (df_w['delta_w_g']-1)*100
    df_w['delta_w_g_round'] = np.round(df_w['delta_w_g'],2)
    df_w_print = df_w[['gamma','delta_w_g_round']].head(20).rename(columns={'gamma':'Market ($\gamma$)','delta_w_g_round':'Earnings decline (\%)'})
    # df_w_print.head(10).to_latex(figuredir+fname_stub+'_most_shocked_gammas_table.tex', index=False, escape=False, caption='10 markets with the largest shock-induced wage decrease')
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
    np.round((df_l_gs.loc[df_l_gs.gamma==most_shocked_gamma].drop(columns='gamma').values)/(df_l_gs.loc[df_l_gs.gamma==most_shocked_gamma].drop(columns='gamma').sum(axis=1).values),4)
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
    df_l_gs_rescaled_print.to_latex(figuredir+fname_stub+'_gamma_supply_to_sector_table.tex', index=False, escape=False)
    
    row = equi_pre['l_gs_demand'].numpy()[most_shocked_gamma-1,:]
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
    # fig, ax = plt.subplots()
    # ax.scatter(df_bar_chart['delta_w_g'], df_bar_chart[sector_index], s=1)  
    # ax.text(0.78, 0.9, 'Corr = ' + str(np.round(corr,3)), verticalalignment='top', transform=ax.transAxes) 
    # ax.set_xlabel('Change in $w_{\gamma}$ (by market)')
    # ax.set_ylabel("Share of market's tasks supplied to shocked sector")
    # ax.figure.savefig(figuredir+fname_stub+'_gamma_scatter.png')
    
    
    
    # Scatterplot of gamma manufacturing shares and delta w_g
    for g in range(0,4):
        g_highlight_index = most_shocked_gammas[g]-1
        corr = np.corrcoef(df_bar_chart['delta_w_g'], df_bar_chart[sector_index])[0,1]
        # fig, ax = plt.subplots()
        # ax.scatter(df_bar_chart['delta_w_g'], df_bar_chart[sector_index], s=1)  
        # ax.scatter(df_bar_chart['delta_w_g'][g_highlight_index],df_bar_chart[sector_index][g_highlight_index], c='r', s=1)
        # ax.annotate(r'$\gamma=$'+str(g_highlight_index+1), xy=(df_bar_chart['delta_w_g'][g_highlight_index],df_bar_chart[sector_index][g_highlight_index]), xytext=(.2, .9),arrowprops=dict(facecolor='black',  arrowstyle='fancy'), fontsize=12, textcoords='axes fraction')
        # ax.text(0.78, 0.9, 'Corr = ' + str(np.round(corr,3)), verticalalignment='top', transform=ax.transAxes) 
        # ax.set_xlabel('Change in $w_{\gamma}$ (by market)')
        # ax.set_ylabel("Share of market's tasks supplied to shocked sector")
        # ax.figure.savefig(figuredir+fname_stub+'_gamma_scatter_highlight_' + str(g) + '.png')
        
        
    print('Occupation counts for the most shocked gammas:')
    for g in range(0,4):
        #(w_dict, j_dict) = occ_counts_by_type(mle_data_filename, 0)
        #pickle.dump((w_dict, j_dict), open(figuredir+fname_stub+'_occ_counts.p', 'wb'))
        #(w_dict, j_dict) = pickle.load(open(figuredir+fname_stub+'_occ_counts.p', "rb"), encoding='bytes')
        #print(j_dict[most_shocked_gammas[g]].head(20))
        #print('')
        j_dict[most_shocked_gammas[g]]['cbo2002'] = j_dict[most_shocked_gammas[g]]['cbo2002'].astype(int)
        j_dict[most_shocked_gammas[g]]['share'] = np.round(j_dict[most_shocked_gammas[g]]['share'],3)
        # j_dict[most_shocked_gammas[g]].head(10).to_latex(index=False, buf=figuredir + fname_stub + "_most_shocked_gamma_occ_counts_" + str(g) + ".tex", label='table:'+fname_stub + '_most_shocked_gamma_occ_counts_' + str(g), caption=r'Occupation counts for $\gamma=$'+str(most_shocked_gammas[g]), columns=['cbo2002','description', 'share'], header=['Occ Code','Occ Description','Occ share'], multicolumn=False)
    
    
# Scatterplot of gamma manufacturing shares and delta w_g
    # g = 254
    # g_highlight_index = g-1
    # corr = np.corrcoef(df_bar_chart['delta_w_g'], df_bar_chart[sector_index])[0,1]
    # fig, ax = plt.subplots()
    # ax.scatter(df_bar_chart['delta_w_g'], df_bar_chart[sector_index], s=1)  
    # ax.scatter(df_bar_chart['delta_w_g'][g_highlight_index],df_bar_chart[sector_index][g_highlight_index], c='r', s=1)
    # ax.annotate(r'$\gamma=$'+str(g_highlight_index+1), xy=(df_bar_chart['delta_w_g'][g_highlight_index],df_bar_chart[sector_index][g_highlight_index]), xytext=(.2, .9),arrowprops=dict(facecolor='black',  arrowstyle='fancy'), fontsize=12, textcoords='axes fraction')
    # ax.text(0.78, 0.9, 'Corr = ' + str(np.round(corr,3)), verticalalignment='top', transform=ax.transAxes) 
    # ax.set_xlabel('Change in $w_{\gamma}$ (by market)')
    # ax.set_ylabel("Share of market's tasks supplied to shocked sector")
    # ax.figure.savefig(figuredir+fname_stub+'_gamma_scatter_highlight_' + str(g) + '.png')
    
    

    # if fname_stub == 'shock_case_study_PrivHealthEduc':
    #     p_ig_pre_no_N  = equi_pre['p_ig'][:,1:]  / torch.reshape( (1-equi_pre['p_ig'][:,0]),  (290,1))
        
    #     max_gamma_share = torch.max(p_ig_pre_no_N,dim=1)[0]
    #     max_gamma_temp = torch.max(p_ig_pre_no_N,dim=1)[1]
    #     max_gamma = max_gamma_temp+1 #Correct for 0-indexing
        
    #     a = pd.DataFrame({'iota':np.arange(1,291),'max_gamma_share':max_gamma_share, 'max_gamma':max_gamma})
    #     a['iota'] = a.index+1
        
        
    #     a = a.merge(df_w, left_on='max_gamma', right_on='gamma').sort_values(by='iota')
        
    #     i = 19
    #     i_highlight_index = g-1
    #     fig, ax = plt.subplots()
    #     ax.scatter(a['delta_w_g'], a['max_gamma_share'], s=1)  
    #     ax.scatter(a['delta_w_g'].loc[a.iota==i],a['max_gamma_share'].loc[a.iota==i], c='r', s=1)
    #     ax.annotate(r'$\iota=$'+str(i), xy=(a['delta_w_g'].loc[a.iota==i],a['max_gamma_share'].loc[a.iota==i]), xytext=(.3,.9) ,arrowprops=dict(facecolor='black',  arrowstyle='fancy'), fontsize=12, textcoords='axes fraction')
    #     ax.set_xlabel("Change in earnings in $\iota$'s modal labor market")
    #     ax.set_ylabel("Concentration of $\iota$'s labor supply")
    #     ax.figure.savefig(figuredir+fname_stub+'_iota_19_highlight_fig.png')
    
    

    # if fname_stub == 'shock_case_study_Public':
    #     p_ig_pre_no_N  = equi_pre['p_ig'][:,1:]  / torch.reshape( (1-equi_pre['p_ig'][:,0]),  (290,1))
        
    #     max_gamma_share = torch.max(p_ig_pre_no_N,dim=1)[0]
    #     max_gamma_temp = torch.max(p_ig_pre_no_N,dim=1)[1]
    #     max_gamma = max_gamma_temp+1 #Correct for 0-indexing
        
    #     a = pd.DataFrame({'iota':np.arange(1,291),'max_gamma_share':max_gamma_share, 'max_gamma':max_gamma})
    #     a['iota'] = a.index+1
        
        
    #     a = a.merge(df_w, left_on='max_gamma', right_on='gamma').sort_values(by='iota')
        
    #     i = 19
    #     i_highlight_index = g-1
    #     fig, ax = plt.subplots()
    #     ax.scatter(a['delta_w_g'], a['max_gamma_share'], s=1)  
    #     ax.scatter(a['delta_w_g'].loc[a.iota==i],a['max_gamma_share'].loc[a.iota==i], c='r', s=1)
    #     ax.annotate(r'$\iota=$'+str(i), xy=(a['delta_w_g'].loc[a.iota==i],a['max_gamma_share'].loc[a.iota==i]), xytext=(.3,.9) ,arrowprops=dict(facecolor='black',  arrowstyle='fancy'), fontsize=12, textcoords='axes fraction')
    #     ax.set_xlabel("Change in earnings in $\iota$'s modal labor market")
    #     ax.set_ylabel("Concentration of $\iota$'s labor supply")
    #     ax.figure.savefig(figuredir+fname_stub+'_iota_19_highlight_fig.png')

    #     w_dict[118].head(10).to_latex(index=False, buf=figuredir + fname_stub + "_least_concentrated_iota.tex", label='table:'+fname_stub + '_least_concentrated_iota', caption=r'Occupation counts for $\iota=118$', columns=['cbo2002','description', 'share'], header=['Occ Code','Occ Description','Occ share'], multicolumn=False)
    
    
    # fig, ax = plt.subplots()
    # ax.scatter(np.arange(1,291), sector_probabilities_by_iota[sector_index].sort_values(),s=5)
    # ax.set_xlabel('Worker types (sorted by shocked sector share)')            
    # ax.set_ylabel('Shcoked sector share of Employment')
    # ax.set_xticklabels([])
    # ax.set_xticks([])
    # ax.figure.savefig(figuredir+fname_stub+'_iota_shocked_sector_share.png')


    
    sector_probabilities_by_iota['hhi'] = sector_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)
    
    
    crosstab_iota_gamma = pd.crosstab(index = fake_data_full.iota.loc[fake_data_full.year==2009], columns = fake_data_full.gamma.loc[fake_data_full.year==2009]).drop(columns=0)
    gamma_probabilities_by_iota = crosstab_iota_gamma.div(crosstab_iota_gamma.sum(axis=1),axis=0).reset_index()
    gamma_probabilities_by_iota['hhi'] = gamma_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)
    
    # fig, ax = plt.subplots()
    # ax.scatter(np.arange(1,291), sector_probabilities_by_iota['hhi'].sort_values(),s=5)
    # ax.set_xlabel('Worker types (sorted by sector employment HHI)')
    # ax.set_ylabel('Sector employment HHI')
    # ax.set_xticklabels([])
    # ax.set_xticks([])
    # ax.set_ylim(0,1)
    # ax.figure.savefig(figuredir+fname_stub+'_iota_sector_hhi.png')

    
    # fig, ax = plt.subplots()
    # ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(),s=5)
    # ax.set_xlabel('Worker types (sorted by market employment HHI)')
    # ax.set_ylabel('Market employment HHI')
    # ax.set_xticklabels([])
    # ax.set_xticks([])
    # ax.set_ylim(0,1)
    # ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_hhi.png')

    
    # fig, ax = plt.subplots()
    # ax.scatter(np.arange(1,291), sector_probabilities_by_iota['hhi'].sort_values(),s=5,label='Sector')
    # ax.scatter(np.arange(1,291), gamma_probabilities_by_iota['hhi'].sort_values(), s=5,label='Market', marker='+')
    # ax.set_xlabel('Worker types (sorted by employment HHI)')
    # ax.set_ylabel('HHI')
    # ax.set_xticklabels([])
    # ax.set_xticks([])
    # ax.set_ylim(0,1)
    # ax.legend()
    # ax.figure.savefig(figuredir+fname_stub+'_iota_gamma_sector_hhi.png', dpi=300)
    print("fname_stub")

    if fname_stub=="shock_case_study_AccomFood":
        exec(open('shock_case_study_AccomFood.py').read())
