#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 18:21:00 2021

@author: jfogel
"""
import pandas as pd
import numpy as np
import statsmodels.formula.api as sm
from statsmodels.iolib.summary2 import summary_col
from stargazer.stargazer import Stargazer, LineLocation
import matplotlib.pyplot as plt


equi_china = pickle.load(open(root + "Data/dgp/dgp_equi_china.p", "rb"))
fake_data_china_filename = root + "Data//dgp/fake_data_china_rio_2009_2012_level_" + str(level) + ".csv"


shock_source = 'data'
equi_shock = equi_china


if 1==1:    
    df_iota = pd.DataFrame(columns = ['iota_p','gamma_p','coef','r2'], index=[])
    gamma_p = 0
    for iota_p in np.arange(0,105,5):
        print(iota_p)
        
        # Load DGP data and restrict to obs with an iota and a gamma
        data_full = pd.read_csv(fake_data_china_filename)
        data_full = data_full.loc[(data_full['gamma']!=-1) & (data_full['iota']!=-1)]
        data_full = data_full.loc[(data_full.year==2009) | (data_full.year==2014)]
        
            # Define the shocks
        if shock_source=='model':
            shock_s    = (equi_shock['y_s']/equi_pre['y_s']-1).numpy()
            shock_l_g  = (equi_shock['l_g']/equi_pre['l_g']-1).numpy()
        
        if shock_source=='data':
            shock_s = y_ts.loc[y_ts.index==2014].values/y_ts.loc[y_ts.index==2009].values-1
            
            gamma_sums_2009    = data_full.loc[data_full.year==2009].groupby(['gamma'])['real_hrly_wage_dec'].sum().reset_index().rename(columns={'real_hrly_wage_dec':'w_2009'})
            gamma_sums_2014    = data_full.loc[data_full.year==2014].groupby(['gamma'])['real_hrly_wage_dec'].sum().reset_index().rename(columns={'real_hrly_wage_dec':'w_2014'})
            gamma_sums = gamma_sums_2009.merge(gamma_sums_2014, on='gamma', how='left')
            gamma_sums = gamma_sums.loc[gamma_sums.gamma!=0]
            shock_l_g = np.array(gamma_sums.w_2014/gamma_sums.w_2009 - 1)
                
            
        
        # Shuffle iotas   
        data_full['iota_rand'] = np.random.randint(1, 291, size=data_full.shape[0])
        draw =  np.random.uniform(0,100,size=data_full.shape[0])
        data_full['iota'].loc[draw < iota_p] = data_full['iota_rand'].loc[draw < iota_p] 
            
        
        # Shuffle gammas   
        #data_full['gamma_rand'] = np.random.randint(1, 428, size=data_full.shape[0])
        #draw =  np.random.uniform(0,100,size=data_full.shape[0])
        #data_full['occ4_first_recode'] = data_full.iota
        #data_full['gamma'].loc[draw < gamma_p] = data_full['gamma_rand'].loc[draw < gamma_p] 
            
        
        # iota exposure measures
        crosstab_iota_sector = pd.crosstab(index = data_full.iota.loc[data_full.year==2009], columns = data_full.sector_IBGE.loc[data_full.year==2009])
        sector_shares_iota = crosstab_iota_sector.div(crosstab_iota_sector.sum(axis=1),axis=0)
        crosstab_iota_gamma = pd.crosstab(index = data_full.iota.loc[(data_full.year==2009) & (data_full.gamma>0)], columns = data_full.gamma.loc[(data_full.year==2009) & (data_full.gamma>0)])   # Drop non-employment from these shares
        gamma_shares_iota = crosstab_iota_gamma.div(crosstab_iota_gamma.sum(axis=1),axis=0)
        
        iota_exposure = sector_shares_iota.multiply(shock_s, axis=1).sum(axis=1).reset_index().rename(columns={0:'iota_exposure'})
        iota_exposure_l_g = gamma_shares_iota.multiply(shock_l_g, axis=1).sum(axis=1).reset_index().rename(columns={0:'iota_exposure_l_g'})
        
        iota_exposure['iota_exposure_std']         = (iota_exposure['iota_exposure']-iota_exposure['iota_exposure'].mean())/iota_exposure['iota_exposure'].std()
        iota_exposure_l_g['iota_exposure_l_g_std'] = (iota_exposure_l_g['iota_exposure_l_g']-iota_exposure_l_g['iota_exposure_l_g'].mean())/iota_exposure_l_g['iota_exposure_l_g'].std()
        
        
        
        # Reshape to wide
        data_full['employed'] = ((data_full.gamma>0)*1).astype(float)
        data_full['year'] = data_full.year.astype(str)
        data_full = data_full.pivot(index='wid_masked', columns='year', values=['employed','real_hrly_wage_dec','ln_real_hrly_wage_dec','iota','occ4_first_recode','sector_IBGE'])
        
        data_full.columns = ['_'.join(col) for col in data_full.columns.values]
        data_full = data_full.drop(columns=['iota_2014','occ4_first_recode_2014']).rename(columns={'iota_2009':'iota','occ4_first_recode_2009':'occ4_first_recode'})
        
        data_full = data_full.merge(iota_exposure, on='iota')
        data_full = data_full.merge(iota_exposure_l_g, on='iota')
        
        
        data_full['delta_ln_real_hrly_wage_dec'] = data_full.ln_real_hrly_wage_dec_2014-data_full.ln_real_hrly_wage_dec_2009
        data_full['delta_ln_real_hrly_wage_dec_N'] = data_full.ln_real_hrly_wage_dec_2014.fillna(0)-data_full.ln_real_hrly_wage_dec_2009.fillna(0)
        data_full['delta_employed_dec'] = data_full.employed_2014-data_full.employed_2009
        
        # converting the Y variables to float objects
        data_full.ln_real_hrly_wage_dec_2009 = data_full.ln_real_hrly_wage_dec_2009.astype(float)
        data_full.ln_real_hrly_wage_dec_2014 = data_full.ln_real_hrly_wage_dec_2014.astype(float)
        data_full['ln_real_hrly_wage_dec_2009_N'] = data_full.ln_real_hrly_wage_dec_2009.fillna(0)
        data_full['ln_real_hrly_wage_dec_2014_N'] = data_full.ln_real_hrly_wage_dec_2014.fillna(0)
        data_full['real_hrly_wage_dec_2009_N'] = data_full.real_hrly_wage_dec_2009.fillna(0)
        data_full['real_hrly_wage_dec_2014_N'] = data_full.real_hrly_wage_dec_2014.fillna(0)
        data_full['delta_ln_real_hrly_wage_dec'] = data_full['delta_ln_real_hrly_wage_dec'].astype(float)
        data_full['delta_ln_real_hrly_wage_dec_N'] = data_full['delta_ln_real_hrly_wage_dec_N'].astype(float)
        data_full['delta_employed_dec'] = data_full['delta_employed_dec'].astype(float)
        
        
            
        ##############################################################################
        # Compute earnings and earnings changes by the worker's pre-shock sector
        if 1==1: 
            earnings_by_sector         = data_full.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2014_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014_N':'2014'})
            earnings_by_sector['2009'] = data_full.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2009_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009_N':'2009'})['2009']
            earnings_by_sector['emp_2009'] = pd.DataFrame(data_full.groupby(['sector_IBGE_2009'])['sector_IBGE_2009'].count()).rename(columns={'sector_IBGE_2009':'count'}).reset_index()['count']
            
            earnings_by_sector['delta_earnings'] = earnings_by_sector['2014'] - earnings_by_sector['2009']
            earnings_by_sector['delta_earnings_per_worker'] = (earnings_by_sector['2014'] - earnings_by_sector['2009'])/earnings_by_sector['emp_2009']
            earnings_by_sector['pct_delta_earnings'] = earnings_by_sector['2014'] / earnings_by_sector['2009']-1
            earnings_by_sector['sector_share_delta_earnings'] = earnings_by_sector['delta_earnings']/earnings_by_sector['delta_earnings'].sum()
            
        
        ##############################################################################
        # Compute earnings and earnings changes by the worker's pre-shock iota
        if 1==1: 
            earnings_by_iota         = data_full.groupby(['iota'])['real_hrly_wage_dec_2014_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014_N':'2014'})
            earnings_by_iota['2009'] = data_full.groupby(['iota'])['real_hrly_wage_dec_2009_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009_N':'2009'})['2009']
            earnings_by_iota['emp_2009'] = pd.DataFrame(data_full.groupby(['iota'])['iota'].count()).rename(columns={'iota':'count'}).reset_index()['count']
            
            earnings_by_iota['delta_earnings'] = earnings_by_iota['2014'] - earnings_by_iota['2009']
            earnings_by_iota['delta_earnings_per_worker'] = (earnings_by_iota['2014'] - earnings_by_iota['2009'])/earnings_by_iota['emp_2009']
            earnings_by_iota['pct_delta_earnings'] = earnings_by_iota['2014'] / earnings_by_iota['2009']-1
            earnings_by_iota['iota_share_delta_earnings'] = earnings_by_iota['delta_earnings']/earnings_by_iota['delta_earnings'].sum()
            
        
        ##############################################################################
        # Compute earnings and earnings changes by the worker's pre-shock occ4
        if 1==1: 
            earnings_by_occ4         = data_full.groupby(['occ4_first_recode'])['real_hrly_wage_dec_2014_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014_N':'2014'})
            earnings_by_occ4['2009'] = data_full.groupby(['occ4_first_recode'])['real_hrly_wage_dec_2009_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009_N':'2009'})['2009']
            earnings_by_occ4['emp_2009'] = pd.DataFrame(data_full.groupby(['occ4_first_recode'])['occ4_first_recode'].count()).rename(columns={'occ4_first_recode':'count'}).reset_index()['count']
            
            earnings_by_occ4['delta_earnings'] = earnings_by_occ4['2014'] - earnings_by_occ4['2009']
            earnings_by_occ4['delta_earnings_per_worker'] = (earnings_by_occ4['2014'] - earnings_by_occ4['2009'])/earnings_by_occ4['emp_2009']
            earnings_by_occ4['pct_delta_earnings'] = earnings_by_occ4['2014'] / earnings_by_occ4['2009']-1
            earnings_by_occ4['occ4_share_delta_earnings'] = earnings_by_occ4['delta_earnings']/earnings_by_occ4['delta_earnings'].sum()
            
        
        
        
        
        ##############
        # Restricting to workers employed in both pre-shock and post-shock period
        if 1==1:
            data_full_emp_both = data_full.loc[(np.isnan(data_full.real_hrly_wage_dec_2009)==False) & (np.isnan(data_full.real_hrly_wage_dec_2014)==False)]
            
            earnings_by_sector_emp_both         = data_full_emp_both.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2014'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014':'2014'})
            earnings_by_sector_emp_both['2009'] = data_full_emp_both.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2009'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009':'2009'})['2009']
            earnings_by_sector_emp_both['emp_2009'] = pd.DataFrame(data_full_emp_both.groupby(['sector_IBGE_2009'])['sector_IBGE_2009'].count()).rename(columns={'sector_IBGE_2009':'count'}).reset_index()['count']
            
            earnings_by_sector_emp_both['delta_earnings'] = earnings_by_sector_emp_both['2014'] - earnings_by_sector_emp_both['2009']
            earnings_by_sector_emp_both['delta_earnings_per_worker'] = (earnings_by_sector_emp_both['2014'] - earnings_by_sector_emp_both['2009'])/earnings_by_sector_emp_both['emp_2009']
            earnings_by_sector_emp_both['pct_delta_earnings'] = earnings_by_sector_emp_both['2014'] / earnings_by_sector_emp_both['2009']-1
            earnings_by_sector_emp_both['sector_share_delta_earnings'] = earnings_by_sector_emp_both['delta_earnings']/earnings_by_sector_emp_both['delta_earnings'].sum()
            
          
        
        
        
        emp_by_sector         = data_full.groupby(['sector_IBGE_2009'])['employed_2014'].sum().reset_index().rename(columns={'employed_2014':'2014'})
        emp_by_sector['2009'] = data_full.groupby(['sector_IBGE_2009'])['employed_2009'].sum().reset_index().rename(columns={'employed_2009':'2009'})['2009']
        emp_by_sector['delta_emp'] = emp_by_sector['2014'] - emp_by_sector['2009']
        emp_by_sector['pct_delta_emp'] = emp_by_sector['2014'] / emp_by_sector['2009']-1
        emp_by_sector['sector_share_delta_emp'] = emp_by_sector['delta_emp']/emp_by_sector['delta_emp'].sum()
        
        
        
        
        
        #######################################################################################################################################stargazer
        # At the aggregate level
        #######################################################################################################################################
        
        
        agg_iota = (data_full.groupby(['iota'])['employed_2014'].mean() / data_full.groupby(['iota'])['employed_2009'].mean()-1).reset_index().rename(columns={0:'delta_employed_dec'})
        agg_iota_ln_wage = (data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2014'].mean() / data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2009'].mean()-1).reset_index().rename(columns={0:'delta_ln_real_hrly_wage_dec'})
        agg_iota_ln_wage_N = (data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2014_N'].mean() / data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2009_N'].mean()-1).reset_index().rename(columns={0:'delta_ln_real_hrly_wage_dec_N'})
        agg_iota_emp_2009 = data_full.groupby(['iota'])['employed_2009'].sum().reset_index()
        agg_iota = agg_iota.merge(agg_iota_ln_wage, on='iota')
        agg_iota = agg_iota.merge(agg_iota_ln_wage_N, on='iota')
        agg_iota = agg_iota.merge(iota_exposure, on='iota')
        agg_iota = agg_iota.merge(iota_exposure_l_g, on='iota')
        agg_iota = agg_iota.merge(agg_iota_emp_2009, on='iota')
        
        
        reg = sm.ols('delta_ln_real_hrly_wage_dec_N ~ iota_exposure_l_g', data=agg_iota).fit()
        
        append_df = pd.DataFrame(data=[[iota_p, gamma_p, reg.params[1], reg.rsquared]], columns = ['iota_p','gamma_p','coef','r2'], index=[0])
        df_iota = df_iota.append(append_df)
    
        # fig, ax = plt.subplots()
        # ax.scatter(agg_iota.iota_exposure_l_g,agg_iota.delta_ln_real_hrly_wage_dec_N, s=3)
        # ax.set_title('$\iota$, p='+str(iota_p))
        # ax.set_xlabel('Exposure')
        # ax.set_ylabel('$\Delta$ log earnings')
        # ax.set_xlim(-8,2)
        # ax.set_ylim(-.1,.15)
        
        fig = plt.figure(figsize=(16, 10))
        left, width = 0.1, 0.65
        bottom, height = 0.1, 0.65
        spacing = 0.01
        
        rect_scatter = [left, bottom, width, height]
        rect_histx = [left, bottom + height + spacing, width, 0.18]
        rect_histy = [left + width + spacing, bottom, 0.2, height]
        
        ax_scatter = plt.axes(rect_scatter)
        ax_scatter.tick_params(direction='in', top=True, right=True)
        ax_histx = plt.axes(rect_histx)
        ax_histx.tick_params(direction='in', labelleft=False, labelbottom=False)
        ax_histy = plt.axes(rect_histy)
        ax_histy.tick_params(direction='in', labelleft=False, labelbottom=False)
        
        plot = ax_scatter.scatter(agg_iota.iota_exposure_l_g,agg_iota.delta_ln_real_hrly_wage_dec_N, s=3)
        ax_scatter.set_xlabel('Exposure', fontsize=24)
        ax_scatter.set_ylabel('$\Delta$ log earnings', fontsize=24)
        ax_scatter.set_xlim(-.2,.1)
        ax_histx.set_xlim(-.2,.1)
        ax_scatter.set_ylim(-.1,.15)
        ax_histy.set_ylim(-.1,.15)
        ax_scatter.tick_params(labelsize=20)
        ax_scatter.spines["top"].set_position(("outward", 3))
        ax_scatter.spines["left"].set_position(("outward", 3))
        ax_scatter.spines["top"].set_visible(False)
        ax_scatter.spines["right"].set_visible(False)
        ax_scatter.tick_params(top=False, right=False)
        
        ax_histx.hist(agg_iota.iota_exposure_l_g, bins=40, density=True)
        ax_histy.hist(agg_iota.delta_ln_real_hrly_wage_dec_N, orientation='horizontal', bins=40, density=True)
        ax_histx.text(-.2, .2, r'S.D.='+str(np.round(np.std(agg_iota.iota_exposure_l_g), decimals=3)), fontsize=24)
        ax_histy.text(.15, .12, r'S.D.='+str(np.round(np.std(agg_iota.delta_ln_real_hrly_wage_dec_N), decimals=3)), fontsize=24)
        
        
        ax_scatter.axhline(y=0, color='k')
        ax_histy.axhline(y=0, color='k')
        fig.suptitle('$\iota$, p='+str(iota_p), fontsize=30)
    
        fig.savefig(figuredir + 'error_analysis_shock_' + shock_source + '_i_' + str(iota_p) + '.png')
        plt.show()
        
        print(df_iota)
        
    
    fig, ax = plt.subplots()
    ax.plot(df_iota.iota_p, df_iota.coef)
    ax.set_title('Coefficients, $\iota$')
    ax.set_xlabel('Share of workers misclassified')
    ax.set_ylabel('Regression coefficient')
    
    fig, ax = plt.subplots()
    ax.plot(df_iota.iota_p, df_iota.r2)
    ax.set_title("$R^2$, $\iota$")
    ax.set_xlabel('Share of workers misclassified')
    ax.set_ylabel('$R^2$')
 

pickle.dump(df_iota,   open(root + "Results/df_iota_"+ shock_source + ".p", "wb"))
df_iota = pickle.load( open(root + "Results/df_iota_"+ shock_source + ".p", "rb"))

   









if 1==1:
    df_gamma = pd.DataFrame(columns = ['iota_p','gamma_p','coef','r2'], index=[])
    
    
    iota_p=0
    for gamma_p in np.arange(0,105,5):
        print(gamma_p)
        
        # Load DGP data and restrict to obs with an iota and a gamma
        data_full = pd.read_csv(fake_data_china_filename)
        data_full = data_full.loc[(data_full['gamma']!=-1) & (data_full['iota']!=-1)]
        data_full = data_full.loc[(data_full.year==2009) | (data_full.year==2014)]
        
            # Define the shocks
        if shock_source=='model':
            shock_s    = (equi_shock['y_s']/equi_pre['y_s']-1).numpy()
            shock_l_g  = (equi_shock['l_g']/equi_pre['l_g']-1).numpy()
        
        if shock_source=='data':
            shock_s = y_ts.loc[y_ts.index==2014].values/y_ts.loc[y_ts.index==2009].values-1
            
            gamma_sums_2009    = data_full.loc[data_full.year==2009].groupby(['gamma'])['real_hrly_wage_dec'].sum().reset_index().rename(columns={'real_hrly_wage_dec':'w_2009'})
            gamma_sums_2014    = data_full.loc[data_full.year==2014].groupby(['gamma'])['real_hrly_wage_dec'].sum().reset_index().rename(columns={'real_hrly_wage_dec':'w_2014'})
            gamma_sums = gamma_sums_2009.merge(gamma_sums_2014, on='gamma', how='left')
            gamma_sums = gamma_sums.loc[gamma_sums.gamma!=0]
            shock_l_g = np.array(gamma_sums.w_2014/gamma_sums.w_2009 - 1)
                
            
        
        # Shuffle iotas   
        #data_full['iota_rand'] = np.random.randint(1, 291, size=data_full.shape[0])
        #draw =  np.random.uniform(0,100,size=data_full.shape[0])
        #data_full['iota'].loc[draw < iota_p] = data_full['iota_rand'].loc[draw < iota_p] 
            
        
        # Shuffle gammas   
        data_full['gamma_rand'] = np.random.randint(1, 428, size=data_full.shape[0])
        draw =  np.random.uniform(0,100,size=data_full.shape[0])
        data_full['gamma'].loc[draw < gamma_p] = data_full['gamma_rand'].loc[draw < gamma_p] 
            
        
        # iota exposure measures
        crosstab_iota_sector = pd.crosstab(index = data_full.iota.loc[data_full.year==2009], columns = data_full.sector_IBGE.loc[data_full.year==2009])
        sector_shares_iota = crosstab_iota_sector.div(crosstab_iota_sector.sum(axis=1),axis=0)
        crosstab_iota_gamma = pd.crosstab(index = data_full.iota.loc[(data_full.year==2009) & (data_full.gamma>0)], columns = data_full.gamma.loc[(data_full.year==2009) & (data_full.gamma>0)])   # Drop non-employment from these shares
        gamma_shares_iota = crosstab_iota_gamma.div(crosstab_iota_gamma.sum(axis=1),axis=0)
        
        iota_exposure = sector_shares_iota.multiply(shock_s, axis=1).sum(axis=1).reset_index().rename(columns={0:'iota_exposure'})
        iota_exposure_l_g = gamma_shares_iota.multiply(shock_l_g, axis=1).sum(axis=1).reset_index().rename(columns={0:'iota_exposure_l_g'})
        
        iota_exposure['iota_exposure_std']         = (iota_exposure['iota_exposure']-iota_exposure['iota_exposure'].mean())/iota_exposure['iota_exposure'].std()
        iota_exposure_l_g['iota_exposure_l_g_std'] = (iota_exposure_l_g['iota_exposure_l_g']-iota_exposure_l_g['iota_exposure_l_g'].mean())/iota_exposure_l_g['iota_exposure_l_g'].std()
        
        
        
        # Reshape to wide
        data_full['employed'] = ((data_full.gamma>0)*1).astype(float)
        data_full['year'] = data_full.year.astype(str)
        data_full = data_full.pivot(index='wid_masked', columns='year', values=['employed','real_hrly_wage_dec','ln_real_hrly_wage_dec','iota','occ4_first_recode','sector_IBGE'])
        
        data_full.columns = ['_'.join(col) for col in data_full.columns.values]
        data_full = data_full.drop(columns=['iota_2014','occ4_first_recode_2014']).rename(columns={'iota_2009':'iota','occ4_first_recode_2009':'occ4_first_recode'})
        
        data_full = data_full.merge(iota_exposure, on='iota')
        data_full = data_full.merge(iota_exposure_l_g, on='iota')
        
        
        data_full['delta_ln_real_hrly_wage_dec'] = data_full.ln_real_hrly_wage_dec_2014-data_full.ln_real_hrly_wage_dec_2009
        data_full['delta_ln_real_hrly_wage_dec_N'] = data_full.ln_real_hrly_wage_dec_2014.fillna(0)-data_full.ln_real_hrly_wage_dec_2009.fillna(0)
        data_full['delta_employed_dec'] = data_full.employed_2014-data_full.employed_2009
        
        # converting the Y variables to float objects
        data_full.ln_real_hrly_wage_dec_2009 = data_full.ln_real_hrly_wage_dec_2009.astype(float)
        data_full.ln_real_hrly_wage_dec_2014 = data_full.ln_real_hrly_wage_dec_2014.astype(float)
        data_full['ln_real_hrly_wage_dec_2009_N'] = data_full.ln_real_hrly_wage_dec_2009.fillna(0)
        data_full['ln_real_hrly_wage_dec_2014_N'] = data_full.ln_real_hrly_wage_dec_2014.fillna(0)
        data_full['real_hrly_wage_dec_2009_N'] = data_full.real_hrly_wage_dec_2009.fillna(0)
        data_full['real_hrly_wage_dec_2014_N'] = data_full.real_hrly_wage_dec_2014.fillna(0)
        data_full['delta_ln_real_hrly_wage_dec'] = data_full['delta_ln_real_hrly_wage_dec'].astype(float)
        data_full['delta_ln_real_hrly_wage_dec_N'] = data_full['delta_ln_real_hrly_wage_dec_N'].astype(float)
        data_full['delta_employed_dec'] = data_full['delta_employed_dec'].astype(float)
        
        
            
        ##############################################################################
        # Compute earnings and earnings changes by the worker's pre-shock sector
        if 1==1: 
            earnings_by_sector         = data_full.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2014_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014_N':'2014'})
            earnings_by_sector['2009'] = data_full.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2009_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009_N':'2009'})['2009']
            earnings_by_sector['emp_2009'] = pd.DataFrame(data_full.groupby(['sector_IBGE_2009'])['sector_IBGE_2009'].count()).rename(columns={'sector_IBGE_2009':'count'}).reset_index()['count']
            
            earnings_by_sector['delta_earnings'] = earnings_by_sector['2014'] - earnings_by_sector['2009']
            earnings_by_sector['delta_earnings_per_worker'] = (earnings_by_sector['2014'] - earnings_by_sector['2009'])/earnings_by_sector['emp_2009']
            earnings_by_sector['pct_delta_earnings'] = earnings_by_sector['2014'] / earnings_by_sector['2009']-1
            earnings_by_sector['sector_share_delta_earnings'] = earnings_by_sector['delta_earnings']/earnings_by_sector['delta_earnings'].sum()
            
        
        ##############################################################################
        # Compute earnings and earnings changes by the worker's pre-shock iota
        if 1==1: 
            earnings_by_iota         = data_full.groupby(['iota'])['real_hrly_wage_dec_2014_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014_N':'2014'})
            earnings_by_iota['2009'] = data_full.groupby(['iota'])['real_hrly_wage_dec_2009_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009_N':'2009'})['2009']
            earnings_by_iota['emp_2009'] = pd.DataFrame(data_full.groupby(['iota'])['iota'].count()).rename(columns={'iota':'count'}).reset_index()['count']
            
            earnings_by_iota['delta_earnings'] = earnings_by_iota['2014'] - earnings_by_iota['2009']
            earnings_by_iota['delta_earnings_per_worker'] = (earnings_by_iota['2014'] - earnings_by_iota['2009'])/earnings_by_iota['emp_2009']
            earnings_by_iota['pct_delta_earnings'] = earnings_by_iota['2014'] / earnings_by_iota['2009']-1
            earnings_by_iota['iota_share_delta_earnings'] = earnings_by_iota['delta_earnings']/earnings_by_iota['delta_earnings'].sum()
            
        
        ##############################################################################
        # Compute earnings and earnings changes by the worker's pre-shock occ4
        if 1==1: 
            earnings_by_occ4         = data_full.groupby(['occ4_first_recode'])['real_hrly_wage_dec_2014_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014_N':'2014'})
            earnings_by_occ4['2009'] = data_full.groupby(['occ4_first_recode'])['real_hrly_wage_dec_2009_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009_N':'2009'})['2009']
            earnings_by_occ4['emp_2009'] = pd.DataFrame(data_full.groupby(['occ4_first_recode'])['occ4_first_recode'].count()).rename(columns={'occ4_first_recode':'count'}).reset_index()['count']
            
            earnings_by_occ4['delta_earnings'] = earnings_by_occ4['2014'] - earnings_by_occ4['2009']
            earnings_by_occ4['delta_earnings_per_worker'] = (earnings_by_occ4['2014'] - earnings_by_occ4['2009'])/earnings_by_occ4['emp_2009']
            earnings_by_occ4['pct_delta_earnings'] = earnings_by_occ4['2014'] / earnings_by_occ4['2009']-1
            earnings_by_occ4['occ4_share_delta_earnings'] = earnings_by_occ4['delta_earnings']/earnings_by_occ4['delta_earnings'].sum()
            
        
        
        
        
        ##############
        # Restricting to workers employed in both pre-shock and post-shock period
        if 1==1:
            data_full_emp_both = data_full.loc[(np.isnan(data_full.real_hrly_wage_dec_2009)==False) & (np.isnan(data_full.real_hrly_wage_dec_2014)==False)]
            
            earnings_by_sector_emp_both         = data_full_emp_both.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2014'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014':'2014'})
            earnings_by_sector_emp_both['2009'] = data_full_emp_both.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2009'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009':'2009'})['2009']
            earnings_by_sector_emp_both['emp_2009'] = pd.DataFrame(data_full_emp_both.groupby(['sector_IBGE_2009'])['sector_IBGE_2009'].count()).rename(columns={'sector_IBGE_2009':'count'}).reset_index()['count']
            
            earnings_by_sector_emp_both['delta_earnings'] = earnings_by_sector_emp_both['2014'] - earnings_by_sector_emp_both['2009']
            earnings_by_sector_emp_both['delta_earnings_per_worker'] = (earnings_by_sector_emp_both['2014'] - earnings_by_sector_emp_both['2009'])/earnings_by_sector_emp_both['emp_2009']
            earnings_by_sector_emp_both['pct_delta_earnings'] = earnings_by_sector_emp_both['2014'] / earnings_by_sector_emp_both['2009']-1
            earnings_by_sector_emp_both['sector_share_delta_earnings'] = earnings_by_sector_emp_both['delta_earnings']/earnings_by_sector_emp_both['delta_earnings'].sum()
            
          
        
        
        
        emp_by_sector         = data_full.groupby(['sector_IBGE_2009'])['employed_2014'].sum().reset_index().rename(columns={'employed_2014':'2014'})
        emp_by_sector['2009'] = data_full.groupby(['sector_IBGE_2009'])['employed_2009'].sum().reset_index().rename(columns={'employed_2009':'2009'})['2009']
        emp_by_sector['delta_emp'] = emp_by_sector['2014'] - emp_by_sector['2009']
        emp_by_sector['pct_delta_emp'] = emp_by_sector['2014'] / emp_by_sector['2009']-1
        emp_by_sector['sector_share_delta_emp'] = emp_by_sector['delta_emp']/emp_by_sector['delta_emp'].sum()
        
        
        
        
        
        #######################################################################################################################################stargazer
        # At the aggregate level
        #######################################################################################################################################
        
        
        agg_iota = (data_full.groupby(['iota'])['employed_2014'].mean() / data_full.groupby(['iota'])['employed_2009'].mean()-1).reset_index().rename(columns={0:'delta_employed_dec'})
        agg_iota_ln_wage = (data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2014'].mean() / data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2009'].mean()-1).reset_index().rename(columns={0:'delta_ln_real_hrly_wage_dec'})
        agg_iota_ln_wage_N = (data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2014_N'].mean() / data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2009_N'].mean()-1).reset_index().rename(columns={0:'delta_ln_real_hrly_wage_dec_N'})
        agg_iota_emp_2009 = data_full.groupby(['iota'])['employed_2009'].sum().reset_index()
        agg_iota = agg_iota.merge(agg_iota_ln_wage, on='iota')
        agg_iota = agg_iota.merge(agg_iota_ln_wage_N, on='iota')
        agg_iota = agg_iota.merge(iota_exposure, on='iota')
        agg_iota = agg_iota.merge(iota_exposure_l_g, on='iota')
        agg_iota = agg_iota.merge(agg_iota_emp_2009, on='iota')
        
        
        reg = sm.ols('delta_ln_real_hrly_wage_dec_N ~ iota_exposure_l_g', data=agg_iota).fit()
        
        append_df = pd.DataFrame(data=[[iota_p, gamma_p, reg.params[1], reg.rsquared]], columns = ['iota_p','gamma_p','coef','r2'], index=[0])
        df_gamma = df_gamma.append(append_df)
        
        
        # fig, ax = plt.subplots()
        # ax.scatter(agg_iota.iota_exposure_l_g,agg_iota.delta_ln_real_hrly_wage_dec_N, s=3)
        # ax.set_title('$\gamma$, p='+str(gamma_p))
        # ax.set_xlabel('Exposure')
        # ax.set_ylabel('$\Delta$ log earnings')
        # ax.set_xlim(-8,2)
        # ax.set_ylim(-.1,.15)
        
            
        fig = plt.figure(figsize=(16, 10))
        left, width = 0.1, 0.65
        bottom, height = 0.1, 0.65
        spacing = 0.01
        
        rect_scatter = [left, bottom, width, height]
        rect_histx = [left, bottom + height + spacing, width, 0.18]
        rect_histy = [left + width + spacing, bottom, 0.2, height]
        
        ax_scatter = plt.axes(rect_scatter)
        ax_scatter.tick_params(direction='in', top=True, right=True)
        ax_histx = plt.axes(rect_histx)
        ax_histx.tick_params(direction='in', labelleft=False, labelbottom=False)
        ax_histy = plt.axes(rect_histy)
        ax_histy.tick_params(direction='in', labelleft=False, labelbottom=False)
        
        plot = ax_scatter.scatter(agg_iota.iota_exposure_l_g,agg_iota.delta_ln_real_hrly_wage_dec_N, s=3)
        ax_scatter.set_xlabel('Exposure', fontsize=24)
        ax_scatter.set_ylabel('$\Delta$ log earnings', fontsize=24)
        ax_scatter.set_xlim(-.2,.1)
        ax_histx.set_xlim(-.2,.1)
        ax_scatter.set_ylim(-.1,.15)
        ax_histy.set_ylim(-.1,.15)
        ax_scatter.tick_params(labelsize=20)
        ax_scatter.spines["top"].set_position(("outward", 3))
        ax_scatter.spines["left"].set_position(("outward", 3))
        ax_scatter.spines["top"].set_visible(False)
        ax_scatter.spines["right"].set_visible(False)
        ax_scatter.tick_params(top=False, right=False)
        
        ax_histx.hist(agg_iota.iota_exposure_l_g, bins=40, density=True)
        ax_histy.hist(agg_iota.delta_ln_real_hrly_wage_dec_N, orientation='horizontal', bins=40, density=True)
        ax_histx.text(-.2, .2, r'S.D.='+str(np.round(np.std(agg_iota.iota_exposure_l_g), decimals=3)), fontsize=24)
        ax_histy.text(.2, .12, r'S.D.='+str(np.round(np.std(agg_iota.delta_ln_real_hrly_wage_dec_N), decimals=3)), fontsize=24)
        ax_scatter.axhline(y=0, color='k')
        ax_histy.axhline(y=0, color='k')
        fig.suptitle('$\gamma$, p='+str(gamma_p), fontsize=30)
            
        fig.savefig(figuredir + 'error_analysis_shock_' + shock_source + '_g_' + str(gamma_p) + '.png')
        plt.show()
        
        print(df_gamma)
        
    
    fig, ax = plt.subplots()
    ax.plot(df_gamma.gamma_p, df_gamma.coef)
    ax.set_title('Coefficients, $\gamma$')
    ax.set_xlabel('Share of jobs misclassified')
    ax.set_ylabel('Regression coefficient')
    
    fig, ax = plt.subplots()
    ax.plot(df_gamma.gamma_p, df_gamma.r2)
    ax.set_title("$R^2$, $\gamma$")
    ax.set_xlabel('Share of jobs misclassified')
    ax.set_ylabel('$R^2$')


pickle.dump(df_gamma,   open(root + "Results/df_gamma_"+ shock_source + ".p", "wb"))
df_gamma = pickle.load( open(root + "Results/df_gamma_"+ shock_source + ".p", "rb"))











if 1==1:
    df_ig = pd.DataFrame(columns = ['iota_p','gamma_p','coef','r2'], index=[])
    for iota_p in np.arange(0,105,5):
        for gamma_p in np.arange(0,105,5):
            print(gamma_p)
            
            # Load DGP data and restrict to obs with an iota and a gamma
            data_full = pd.read_csv(fake_data_china_filename)
            data_full = data_full.loc[(data_full['gamma']!=-1) & (data_full['iota']!=-1)]
            data_full = data_full.loc[(data_full.year==2009) | (data_full.year==2014)]
            
                # Define the shocks
            if shock_source=='model':
                shock_s    = (equi_shock['y_s']/equi_pre['y_s']-1).numpy()
                shock_l_g  = (equi_shock['l_g']/equi_pre['l_g']-1).numpy()
            
            if shock_source=='data':
                shock_s = y_ts.loc[y_ts.index==2014].values/y_ts.loc[y_ts.index==2009].values-1
                
                gamma_sums_2009    = data_full.loc[data_full.year==2009].groupby(['gamma'])['real_hrly_wage_dec'].sum().reset_index().rename(columns={'real_hrly_wage_dec':'w_2009'})
                gamma_sums_2014    = data_full.loc[data_full.year==2014].groupby(['gamma'])['real_hrly_wage_dec'].sum().reset_index().rename(columns={'real_hrly_wage_dec':'w_2014'})
                gamma_sums = gamma_sums_2009.merge(gamma_sums_2014, on='gamma', how='left')
                gamma_sums = gamma_sums.loc[gamma_sums.gamma!=0]
                shock_l_g = np.array(gamma_sums.w_2014/gamma_sums.w_2009 - 1)
                    
                
            
            # Shuffle iotas   
            data_full['iota_rand'] = np.random.randint(1, 291, size=data_full.shape[0])
            draw =  np.random.uniform(0,100,size=data_full.shape[0])
            data_full['iota'].loc[draw < iota_p] = data_full['iota_rand'].loc[draw < iota_p] 
                
            
            # Shuffle gammas   
            data_full['gamma_rand'] = np.random.randint(1, 428, size=data_full.shape[0])
            draw =  np.random.uniform(0,100,size=data_full.shape[0])
            data_full['gamma'].loc[draw < gamma_p] = data_full['gamma_rand'].loc[draw < gamma_p] 
                
            
            # iota exposure measures
            crosstab_iota_sector = pd.crosstab(index = data_full.iota.loc[data_full.year==2009], columns = data_full.sector_IBGE.loc[data_full.year==2009])
            sector_shares_iota = crosstab_iota_sector.div(crosstab_iota_sector.sum(axis=1),axis=0)
            crosstab_iota_gamma = pd.crosstab(index = data_full.iota.loc[(data_full.year==2009) & (data_full.gamma>0)], columns = data_full.gamma.loc[(data_full.year==2009) & (data_full.gamma>0)])   # Drop non-employment from these shares
            gamma_shares_iota = crosstab_iota_gamma.div(crosstab_iota_gamma.sum(axis=1),axis=0)
            
            iota_exposure = sector_shares_iota.multiply(shock_s, axis=1).sum(axis=1).reset_index().rename(columns={0:'iota_exposure'})
            iota_exposure_l_g = gamma_shares_iota.multiply(shock_l_g, axis=1).sum(axis=1).reset_index().rename(columns={0:'iota_exposure_l_g'})
            
            iota_exposure['iota_exposure_std']         = (iota_exposure['iota_exposure']-iota_exposure['iota_exposure'].mean())/iota_exposure['iota_exposure'].std()
            iota_exposure_l_g['iota_exposure_l_g_std'] = (iota_exposure_l_g['iota_exposure_l_g']-iota_exposure_l_g['iota_exposure_l_g'].mean())/iota_exposure_l_g['iota_exposure_l_g'].std()
            
            
            
            # Reshape to wide
            data_full['employed'] = ((data_full.gamma>0)*1).astype(float)
            data_full['year'] = data_full.year.astype(str)
            data_full = data_full.pivot(index='wid_masked', columns='year', values=['employed','real_hrly_wage_dec','ln_real_hrly_wage_dec','iota','occ4_first_recode','sector_IBGE'])
            
            data_full.columns = ['_'.join(col) for col in data_full.columns.values]
            data_full = data_full.drop(columns=['iota_2014','occ4_first_recode_2014']).rename(columns={'iota_2009':'iota','occ4_first_recode_2009':'occ4_first_recode'})
            
            data_full = data_full.merge(iota_exposure, on='iota')
            data_full = data_full.merge(iota_exposure_l_g, on='iota')
            
            
            data_full['delta_ln_real_hrly_wage_dec'] = data_full.ln_real_hrly_wage_dec_2014-data_full.ln_real_hrly_wage_dec_2009
            data_full['delta_ln_real_hrly_wage_dec_N'] = data_full.ln_real_hrly_wage_dec_2014.fillna(0)-data_full.ln_real_hrly_wage_dec_2009.fillna(0)
            data_full['delta_employed_dec'] = data_full.employed_2014-data_full.employed_2009
            
            # converting the Y variables to float objects
            data_full.ln_real_hrly_wage_dec_2009 = data_full.ln_real_hrly_wage_dec_2009.astype(float)
            data_full.ln_real_hrly_wage_dec_2014 = data_full.ln_real_hrly_wage_dec_2014.astype(float)
            data_full['ln_real_hrly_wage_dec_2009_N'] = data_full.ln_real_hrly_wage_dec_2009.fillna(0)
            data_full['ln_real_hrly_wage_dec_2014_N'] = data_full.ln_real_hrly_wage_dec_2014.fillna(0)
            data_full['real_hrly_wage_dec_2009_N'] = data_full.real_hrly_wage_dec_2009.fillna(0)
            data_full['real_hrly_wage_dec_2014_N'] = data_full.real_hrly_wage_dec_2014.fillna(0)
            data_full['delta_ln_real_hrly_wage_dec'] = data_full['delta_ln_real_hrly_wage_dec'].astype(float)
            data_full['delta_ln_real_hrly_wage_dec_N'] = data_full['delta_ln_real_hrly_wage_dec_N'].astype(float)
            data_full['delta_employed_dec'] = data_full['delta_employed_dec'].astype(float)
            
            
                
            ##############################################################################
            # Compute earnings and earnings changes by the worker's pre-shock sector
            if 1==1: 
                earnings_by_sector         = data_full.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2014_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014_N':'2014'})
                earnings_by_sector['2009'] = data_full.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2009_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009_N':'2009'})['2009']
                earnings_by_sector['emp_2009'] = pd.DataFrame(data_full.groupby(['sector_IBGE_2009'])['sector_IBGE_2009'].count()).rename(columns={'sector_IBGE_2009':'count'}).reset_index()['count']
                
                earnings_by_sector['delta_earnings'] = earnings_by_sector['2014'] - earnings_by_sector['2009']
                earnings_by_sector['delta_earnings_per_worker'] = (earnings_by_sector['2014'] - earnings_by_sector['2009'])/earnings_by_sector['emp_2009']
                earnings_by_sector['pct_delta_earnings'] = earnings_by_sector['2014'] / earnings_by_sector['2009']-1
                earnings_by_sector['sector_share_delta_earnings'] = earnings_by_sector['delta_earnings']/earnings_by_sector['delta_earnings'].sum()
                
            
            ##############################################################################
            # Compute earnings and earnings changes by the worker's pre-shock iota
            if 1==1: 
                earnings_by_iota         = data_full.groupby(['iota'])['real_hrly_wage_dec_2014_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014_N':'2014'})
                earnings_by_iota['2009'] = data_full.groupby(['iota'])['real_hrly_wage_dec_2009_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009_N':'2009'})['2009']
                earnings_by_iota['emp_2009'] = pd.DataFrame(data_full.groupby(['iota'])['iota'].count()).rename(columns={'iota':'count'}).reset_index()['count']
                
                earnings_by_iota['delta_earnings'] = earnings_by_iota['2014'] - earnings_by_iota['2009']
                earnings_by_iota['delta_earnings_per_worker'] = (earnings_by_iota['2014'] - earnings_by_iota['2009'])/earnings_by_iota['emp_2009']
                earnings_by_iota['pct_delta_earnings'] = earnings_by_iota['2014'] / earnings_by_iota['2009']-1
                earnings_by_iota['iota_share_delta_earnings'] = earnings_by_iota['delta_earnings']/earnings_by_iota['delta_earnings'].sum()
                
            
            ##############################################################################
            # Compute earnings and earnings changes by the worker's pre-shock occ4
            if 1==1: 
                earnings_by_occ4         = data_full.groupby(['occ4_first_recode'])['real_hrly_wage_dec_2014_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014_N':'2014'})
                earnings_by_occ4['2009'] = data_full.groupby(['occ4_first_recode'])['real_hrly_wage_dec_2009_N'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009_N':'2009'})['2009']
                earnings_by_occ4['emp_2009'] = pd.DataFrame(data_full.groupby(['occ4_first_recode'])['occ4_first_recode'].count()).rename(columns={'occ4_first_recode':'count'}).reset_index()['count']
                
                earnings_by_occ4['delta_earnings'] = earnings_by_occ4['2014'] - earnings_by_occ4['2009']
                earnings_by_occ4['delta_earnings_per_worker'] = (earnings_by_occ4['2014'] - earnings_by_occ4['2009'])/earnings_by_occ4['emp_2009']
                earnings_by_occ4['pct_delta_earnings'] = earnings_by_occ4['2014'] / earnings_by_occ4['2009']-1
                earnings_by_occ4['occ4_share_delta_earnings'] = earnings_by_occ4['delta_earnings']/earnings_by_occ4['delta_earnings'].sum()
                
            
            
            
            
            ##############
            # Restricting to workers employed in both pre-shock and post-shock period
            if 1==1:
                data_full_emp_both = data_full.loc[(np.isnan(data_full.real_hrly_wage_dec_2009)==False) & (np.isnan(data_full.real_hrly_wage_dec_2014)==False)]
                
                earnings_by_sector_emp_both         = data_full_emp_both.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2014'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2014':'2014'})
                earnings_by_sector_emp_both['2009'] = data_full_emp_both.groupby(['sector_IBGE_2009'])['real_hrly_wage_dec_2009'].sum().reset_index().rename(columns={'real_hrly_wage_dec_2009':'2009'})['2009']
                earnings_by_sector_emp_both['emp_2009'] = pd.DataFrame(data_full_emp_both.groupby(['sector_IBGE_2009'])['sector_IBGE_2009'].count()).rename(columns={'sector_IBGE_2009':'count'}).reset_index()['count']
                
                earnings_by_sector_emp_both['delta_earnings'] = earnings_by_sector_emp_both['2014'] - earnings_by_sector_emp_both['2009']
                earnings_by_sector_emp_both['delta_earnings_per_worker'] = (earnings_by_sector_emp_both['2014'] - earnings_by_sector_emp_both['2009'])/earnings_by_sector_emp_both['emp_2009']
                earnings_by_sector_emp_both['pct_delta_earnings'] = earnings_by_sector_emp_both['2014'] / earnings_by_sector_emp_both['2009']-1
                earnings_by_sector_emp_both['sector_share_delta_earnings'] = earnings_by_sector_emp_both['delta_earnings']/earnings_by_sector_emp_both['delta_earnings'].sum()
                
              
            
            
            
            emp_by_sector         = data_full.groupby(['sector_IBGE_2009'])['employed_2014'].sum().reset_index().rename(columns={'employed_2014':'2014'})
            emp_by_sector['2009'] = data_full.groupby(['sector_IBGE_2009'])['employed_2009'].sum().reset_index().rename(columns={'employed_2009':'2009'})['2009']
            emp_by_sector['delta_emp'] = emp_by_sector['2014'] - emp_by_sector['2009']
            emp_by_sector['pct_delta_emp'] = emp_by_sector['2014'] / emp_by_sector['2009']-1
            emp_by_sector['sector_share_delta_emp'] = emp_by_sector['delta_emp']/emp_by_sector['delta_emp'].sum()
            
            
            
            
            
            #######################################################################################################################################stargazer
            # At the aggregate level
            #######################################################################################################################################
            
            
            agg_iota = (data_full.groupby(['iota'])['employed_2014'].mean() / data_full.groupby(['iota'])['employed_2009'].mean()-1).reset_index().rename(columns={0:'delta_employed_dec'})
            agg_iota_ln_wage = (data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2014'].mean() / data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2009'].mean()-1).reset_index().rename(columns={0:'delta_ln_real_hrly_wage_dec'})
            agg_iota_ln_wage_N = (data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2014_N'].mean() / data_full.groupby(['iota'])['ln_real_hrly_wage_dec_2009_N'].mean()-1).reset_index().rename(columns={0:'delta_ln_real_hrly_wage_dec_N'})
            agg_iota_emp_2009 = data_full.groupby(['iota'])['employed_2009'].sum().reset_index()
            agg_iota = agg_iota.merge(agg_iota_ln_wage, on='iota')
            agg_iota = agg_iota.merge(agg_iota_ln_wage_N, on='iota')
            agg_iota = agg_iota.merge(iota_exposure, on='iota')
            agg_iota = agg_iota.merge(iota_exposure_l_g, on='iota')
            agg_iota = agg_iota.merge(agg_iota_emp_2009, on='iota')
            
            
            reg = sm.ols('delta_ln_real_hrly_wage_dec_N ~ iota_exposure_l_g', data=agg_iota).fit()
            
            append_df = pd.DataFrame(data=[[iota_p, gamma_p, reg.params[1], reg.rsquared]], columns = ['iota_p','gamma_p','coef','r2'], index=[0])
            df_ig = df_ig.append(append_df)
            
            
            # fig, ax = plt.subplots()
            # ax.scatter(agg_iota.iota_exposure_l_g,agg_iota.delta_ln_real_hrly_wage_dec_N, s=3)
            # ax.set_title('$\gamma$, p='+str(gamma_p))
            # ax.set_xlabel('Exposure')
            # ax.set_ylabel('$\Delta$ log earnings')
            # ax.set_xlim(-8,2)
            # ax.set_ylim(-.1,.15)
            
                
            fig = plt.figure(figsize=(16, 10))
            left, width = 0.1, 0.65
            bottom, height = 0.1, 0.65
            spacing = 0.01
            
            rect_scatter = [left, bottom, width, height]
            rect_histx = [left, bottom + height + spacing, width, 0.18]
            rect_histy = [left + width + spacing, bottom, 0.2, height]
            
            ax_scatter = plt.axes(rect_scatter)
            ax_scatter.tick_params(direction='in', top=True, right=True)
            ax_histx = plt.axes(rect_histx)
            ax_histx.tick_params(direction='in', labelleft=False, labelbottom=False)
            ax_histy = plt.axes(rect_histy)
            ax_histy.tick_params(direction='in', labelleft=False, labelbottom=False)
            
            plot = ax_scatter.scatter(agg_iota.iota_exposure_l_g,agg_iota.delta_ln_real_hrly_wage_dec_N, s=3)
            ax_scatter.set_xlabel('Exposure', fontsize=24)
            ax_scatter.set_ylabel('$\Delta$ log earnings', fontsize=24)
            ax_scatter.set_xlim(-.2,.1)
            ax_histx.set_xlim(-.2,.1)
            ax_scatter.set_ylim(-.1,.15)
            ax_histy.set_ylim(-.1,.15)
            ax_scatter.tick_params(labelsize=20)
            ax_scatter.spines["top"].set_position(("outward", 3))
            ax_scatter.spines["left"].set_position(("outward", 3))
            ax_scatter.spines["top"].set_visible(False)
            ax_scatter.spines["right"].set_visible(False)
            ax_scatter.tick_params(top=False, right=False)
            
            ax_histx.hist(agg_iota.iota_exposure_l_g, bins=40, density=True)
            ax_histy.hist(agg_iota.delta_ln_real_hrly_wage_dec_N, orientation='horizontal', bins=40, density=True)
            ax_histx.text(-.2, .2, r'S.D.='+str(np.round(np.std(agg_iota.iota_exposure_l_g), decimals=3)), fontsize=24)
            ax_histy.text(.2, .12, r'S.D.='+str(np.round(np.std(agg_iota.delta_ln_real_hrly_wage_dec_N), decimals=3)), fontsize=24)
            ax_scatter.axhline(y=0, color='k')
            ax_histy.axhline(y=0, color='k')
            fig.suptitle('$p_{\iota}$='+str(iota_p) + ', $p_{\gamma}$='+str(gamma_p), fontsize=30)
                
            fig.savefig(figuredir + 'error_analysis_shock_' + shock_source + '_i_' + str(iota_p) + '_g_' + str(gamma_p) + '.png')
            plt.show()
            
            print(df_ig)
            
     
     
        
pickle.dump(df_ig,   open(root + "Results/df_ig_"+ shock_source + ".p", "wb"))
df_ig = pickle.load( open(root + "Results/df_ig_"+ shock_source + ".p", "rb"))




# Coefficient
df_ig_wide_coef = df_ig.pivot(index='iota_p', columns='gamma_p', values='coef')
   
fig = plt.figure()
ax = plt.axes(projection='3d')
ax.view_init(30, 120)
ax.contour3D(df_ig_wide_coef.index, df_ig_wide_coef.columns, np.array(df_ig_wide_coef), 50, cmap='viridis')
#ax.invert_xaxis()
ax.invert_yaxis()
ax.set_xlabel('Pct jobs misclassified',fontsize=8)
ax.set_ylabel('Pct workers misclassified',fontsize=8)
ax.set_zlabel(r'$\beta_1$');


x = np.tile(df_ig_wide_coef.index,(21,1))
y = np.transpose(x)
fig = plt.figure()
ax = plt.axes(projection='3d')
ax.view_init(30, 120)
ax.plot_surface(x, y, np.array(df_ig_wide_coef), cmap='viridis')
ax.set_xlabel('Pct jobs misclassified',fontsize=8)
ax.set_ylabel('Pct workers misclassified',fontsize=8)
ax.set_zlabel(r'$\beta_1$')
ax.figure.savefig(figuredir+'misclassification_demo_coef.png',bbox_inches='tight')




# R2
df_ig_wide_r2 = df_ig.pivot(index='iota_p', columns='gamma_p', values='r2')
   
fig = plt.figure()
ax = plt.axes(projection='3d')
ax.view_init(30, 120)
ax.contour3D(df_ig_wide_r2.index, df_ig_wide_r2.columns, np.array(df_ig_wide_r2), 50, cmap='viridis')
#ax.invert_xaxis()
ax.invert_yaxis()
ax.set_xlabel('Pct jobs misclassified',fontsize=8)
ax.set_ylabel('Pct workers misclassified',fontsize=8)
ax.set_zlabel('$R^2$');



x = np.tile(df_ig_wide_r2.index,(21,1))
y = np.transpose(x)
fig = plt.figure()
ax = plt.axes(projection='3d')
ax.view_init(30, 30)
ax.plot_surface(x, y, np.array(df_ig_wide_r2), cmap='viridis')
ax.set_xlabel('Pct jobs misclassified',fontsize=8)
ax.set_ylabel('Pct workers misclassified',fontsize=8)
ax.set_zlabel('$R^2$');
ax.figure.savefig(figuredir+'misclassification_demo_r2.png',bbox_inches='tight')





#When we perturb iota, we are reducing variation in both the LHS and RHS (shrinking towards the mean). When we perturb gamma, we're reducing variation on the RHS alone but still have the same amount of LHS variation, so would that explain increasing coefficients? Is this almost like the opposite of classical measurement error, which ADDS variation to the RHS?


