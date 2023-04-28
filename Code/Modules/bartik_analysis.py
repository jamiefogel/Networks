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


# This is what we do in model_fit_func. Maybe it explains why we are getting this issue when trying to use Stargazer
#import statsmodels.api as sm
#from statsmodels.formula.api import ols

def bartik_analysis(mle_data_filename, figuredir='', equi_shock=None, equi_pre=None, y_ts=None, shock_source='model', savefile_stub='', print_regs=True):
    
    upperlim = 800 # Drop very large wages
    shock_type = savefile_stub.replace('fake_data_', '')
    shock_type = shock_type.replace('_', ' ')
    
    sector_labels_abbr = ["Agriculture, livestock, forestry, fisheries and aquaculture",
                  "Extractive industries",
                  "Manufacturing industries",
                  "Utilities",
                  "Construction",
                  "Retail, Wholesale and Vehicle Repair",
                  "Transport, storage and mail",
                  "Accommodation and food",
                  "Information and communication",
                  "Financial, insurance and related services",
                  "Real estate activities",
                  "Professional, scientific and technical svcs",
                  "Public admin, defense, educ, health and soc security",
                  "Private health and education",
                  "Arts, culture, sports and recreation and other svcs"]
    
    
    if (shock_source!='model') & (shock_source!='data'):
        raise ValueError('You must specify "shock_source=model" or "shock_source=data".')

    
    # Load DGP data and restrict to obs with an iota and a gamma
    data_full = pd.read_csv(mle_data_filename)
    data_full = data_full.loc[(data_full['gamma']!=-1) & (data_full['iota']!=-1) & (data_full['occ4_first_recode']!=-1)]
    data_full = data_full.loc[(data_full.year==2009) | (data_full.year==2014)]
    drop = data_full.real_hrly_wage_dec>upperlim
    data_full = data_full.loc[drop==False]
    
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
            
        
    # iota exposure measures
    crosstab_iota_sector = pd.crosstab(index = data_full.iota.loc[data_full.year==2009], columns = data_full.sector_IBGE.loc[data_full.year==2009])
    sector_shares_iota = crosstab_iota_sector.div(crosstab_iota_sector.sum(axis=1),axis=0)
    crosstab_iota_gamma = pd.crosstab(index = data_full.iota.loc[(data_full.year==2009) & (data_full.gamma>0)], columns = data_full.gamma.loc[(data_full.year==2009) & (data_full.gamma>0)])   # Drop non-employment from these shares
    gamma_shares_iota = crosstab_iota_gamma.div(crosstab_iota_gamma.sum(axis=1),axis=0)
    
    iota_exposure = sector_shares_iota.multiply(shock_s, axis=1).sum(axis=1).reset_index().rename(columns={0:'iota_exposure'})
    iota_exposure_l_g = gamma_shares_iota.multiply(shock_l_g, axis=1).sum(axis=1).reset_index().rename(columns={0:'iota_exposure_l_g'})
    
    iota_exposure['iota_exposure_std']         = (iota_exposure['iota_exposure']-iota_exposure['iota_exposure'].mean())/iota_exposure['iota_exposure'].std()
    iota_exposure_l_g['iota_exposure_l_g_std'] = (iota_exposure_l_g['iota_exposure_l_g']-iota_exposure_l_g['iota_exposure_l_g'].mean())/iota_exposure_l_g['iota_exposure_l_g'].std()
    
    
    # occ4 exposure measures
    crosstab_occ4_sector = pd.crosstab(index = data_full.occ4_first_recode.loc[data_full.year==2009], columns = data_full.sector_IBGE.loc[data_full.year==2009])
    sector_shares_occ4 = crosstab_occ4_sector.div(crosstab_occ4_sector.sum(axis=1),axis=0)
    crosstab_occ4_gamma = pd.crosstab(index = data_full.occ4_first_recode.loc[(data_full.year==2009) & (data_full.gamma>0)], columns = data_full.gamma.loc[(data_full.year==2009) & (data_full.gamma>0)])   # Drop non-employment from these shares
    gamma_shares_occ4 = crosstab_occ4_gamma.div(crosstab_occ4_gamma.sum(axis=1),axis=0)
    
    occ4_exposure = sector_shares_occ4.multiply(shock_s, axis=1).sum(axis=1).reset_index().rename(columns={0:'occ4_exposure'})
    occ4_exposure_l_g = gamma_shares_occ4.multiply(shock_l_g, axis=1).sum(axis=1).reset_index().rename(columns={0:'occ4_exposure_l_g'})
    
    occ4_exposure['occ4_exposure_std']         = (occ4_exposure['occ4_exposure']-occ4_exposure['occ4_exposure'].mean())/occ4_exposure['occ4_exposure'].std()
    occ4_exposure_l_g['occ4_exposure_l_g_std'] = (occ4_exposure_l_g['occ4_exposure_l_g']-occ4_exposure_l_g['occ4_exposure_l_g'].mean())/occ4_exposure_l_g['occ4_exposure_l_g'].std()
    
    

    
    # Reshape to wide
    data_full['employed'] = ((data_full.gamma>0)*1).astype(float)
    data_full['year'] = data_full.year.astype(str)
    data_full = data_full.pivot(index='wid_masked', columns='year', values=['employed','real_hrly_wage_dec','ln_real_hrly_wage_dec','iota','occ4_first_recode','sector_IBGE','gamma'])
    
    data_full.columns = ['_'.join(col) for col in data_full.columns.values]
    data_full = data_full.drop(columns=['iota_2014','occ4_first_recode_2014']).rename(columns={'iota_2009':'iota','occ4_first_recode_2009':'occ4_first_recode'})
    
    data_full = data_full.merge(iota_exposure, on='iota')
    data_full = data_full.merge(iota_exposure_l_g, on='iota')
    data_full = data_full.merge(occ4_exposure, on='occ4_first_recode')
    data_full = data_full.merge(occ4_exposure_l_g, on='occ4_first_recode')
    
    
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
    
    
    # The previous version had a bunch of individual-level regressions but the R^2s were 0 because there is so much idiosyncratic variation. We didn't use them in the paper or talk so I cut them. Can refer back to the aug2021 version if we want them. 
    

    
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

    
    agg_occ4 = (data_full.groupby(['occ4_first_recode'])['employed_2014'].mean() / data_full.groupby(['occ4_first_recode'])['employed_2009'].mean()-1).reset_index().rename(columns={0:'delta_employed_dec'})
    agg_occ4_ln_wage = (data_full.groupby(['occ4_first_recode'])['ln_real_hrly_wage_dec_2014'].mean() / data_full.groupby(['occ4_first_recode'])['ln_real_hrly_wage_dec_2009'].mean()-1).reset_index().rename(columns={0:'delta_ln_real_hrly_wage_dec'})
    agg_occ4_ln_wage_N = (data_full.groupby(['occ4_first_recode'])['ln_real_hrly_wage_dec_2014_N'].mean() / data_full.groupby(['occ4_first_recode'])['ln_real_hrly_wage_dec_2009_N'].mean()-1).reset_index().rename(columns={0:'delta_ln_real_hrly_wage_dec_N'})
    agg_occ4_emp_2009 = data_full.groupby(['occ4_first_recode'])['employed_2009'].sum().reset_index()
    agg_occ4 = agg_occ4.merge(agg_occ4_ln_wage, on='occ4_first_recode')
    agg_occ4 = agg_occ4.merge(agg_occ4_ln_wage_N, on='occ4_first_recode')
    agg_occ4 = agg_occ4.merge(occ4_exposure, on='occ4_first_recode')
    agg_occ4 = agg_occ4.merge(occ4_exposure_l_g, on='occ4_first_recode')
    agg_occ4 = agg_occ4.merge(agg_occ4_emp_2009, on='occ4_first_recode')
    
    
    
    
    res_emp_iota = sm.ols('delta_employed_dec ~ iota_exposure_std', data=agg_iota).fit()
    res_emp_occ4 = sm.ols('delta_employed_dec ~ occ4_exposure_std', data=agg_occ4).fit()
    res_emp_iota_l_g = sm.ols('delta_employed_dec ~ iota_exposure_l_g_std', data=agg_iota).fit()
    res_emp_occ4_l_g = sm.ols('delta_employed_dec ~ occ4_exposure_l_g_std', data=agg_occ4).fit()
    
    res_ln_wage_iota = sm.ols('delta_ln_real_hrly_wage_dec ~ iota_exposure_std', data=agg_iota).fit()
    res_ln_wage_occ4 = sm.ols('delta_ln_real_hrly_wage_dec ~ occ4_exposure_std', data=agg_occ4).fit()
    
    res_ln_wage_N_iota = sm.ols('delta_ln_real_hrly_wage_dec_N ~ iota_exposure_std', data=agg_iota).fit()
    res_ln_wage_N_occ4 = sm.ols('delta_ln_real_hrly_wage_dec_N ~ occ4_exposure_std', data=agg_occ4).fit()
    res_ln_wage_N_iota_l_g = sm.ols('delta_ln_real_hrly_wage_dec_N ~ iota_exposure_l_g_std', data=agg_iota).fit()
    res_ln_wage_N_occ4_l_g = sm.ols('delta_ln_real_hrly_wage_dec_N ~ occ4_exposure_l_g_std', data=agg_occ4).fit()
    
    #Regress changes in employment on various exposure measures
    # - We do not use these in the final paper or talk
    if 1==0:
        #
        stargazer = Stargazer([res_emp_iota_l_g,res_emp_iota,res_emp_occ4,res_emp_occ4_l_g])
        stargazer.rename_covariates({'iota_exposure_l_g_std':'iota exposure (market)','iota_exposure_std':'iota exposure (sector)', 'occ4_exposure_std':'occ4 exposure (sector)', 'occ4_exposure_l_g_std':'occ4 exposure (market)'})
        stargazer.covariate_order(['Intercept','iota_exposure_l_g_std','iota_exposure_std','occ4_exposure_std','occ4_exposure_l_g_std'])
        stargazer.show_model_numbers(False)
        stargazer.dep_var_name = None
        stargazer.show_degrees_of_freedom(False)
        stargazer.show_f_statistic = False
        stargazer.show_residual_std_err=False
        stargazer.show_adj_r2 = False
        stargazer.significance_levels([10e-100, 10e-101, 10e-102])  # Suppress significance stars
        stargazer.append_notes(False)
        stargazer.add_line('Exposure:', ['Market', 'Sector', 'Sector', 'Market'], LineLocation.BODY_TOP)
        stargazer.add_line('Worker classification:', ['iota', 'iota', 'occ4', 'occ4'], LineLocation.BODY_TOP)
        stargazer.add_line('\hline', ['', '', '', ''], LineLocation.BODY_TOP)
        #stargazer.add_line('Preferred Specification', ['No', 'Yes', 'No', 'No'], LineLocation.FOOTER_TOP)
        with open(figuredir + savefile_stub + "_iota_occ4_exposure_regs_emp.tex", "w") as f:
            f.write(stargazer.render_latex(only_tabular=True ))
        
    
    if print_regs==True:
        dfoutput = summary_col([res_emp_iota,res_emp_iota_l_g,res_emp_occ4,res_emp_occ4_l_g],stars=True)
        print(dfoutput)
    
    # Regress changes in earnings on various exposure measures
    # This is the output we actually use
    stargazer = Stargazer([res_ln_wage_N_iota_l_g, res_ln_wage_N_iota, res_ln_wage_N_occ4_l_g, res_ln_wage_N_occ4])
    stargazer.rename_covariates({'iota_exposure_l_g_std':'iota exposure (market)','iota_exposure_std':'iota exposure (sector)', 'occ4_exposure_l_g_std':'occ4 exposure (market)', 'occ4_exposure_std':'occ4 exposure (sector)'})
    stargazer.covariate_order(['Intercept','iota_exposure_l_g_std','iota_exposure_std','occ4_exposure_l_g_std','occ4_exposure_std'])
    stargazer.show_model_numbers(False)
    stargazer.dep_var_name = None
    stargazer.show_degrees_of_freedom(False)
    stargazer.show_f_statistic = False
    stargazer.show_residual_std_err=False
    stargazer.show_adj_r2 = False
    stargazer.significance_levels([10e-100, 10e-101, 10e-102])  # Suppress significance stars
    stargazer.append_notes(False)
    stargazer.add_line('Exposure:', ['Market ($\gamma$)', 'Sector', 'Market ($\gamma$)', 'Sector'], LineLocation.BODY_TOP)
    stargazer.add_line('Worker classification:', ['Worker type ($\iota$)', 'Worker type ($\iota$)', 'Occ4', 'Occ4'], LineLocation.BODY_TOP)
    stargazer.add_line('\hline', ['', '', '', ''], LineLocation.BODY_TOP)
    #stargazer.add_line('Preferred Specification', ['No', 'Yes', 'No', 'No'], LineLocation.FOOTER_TOP)
    with open(figuredir + savefile_stub + "_iota_occ4_exposure_regs_ln_wage_N.tex", "w") as f:
        f.write(stargazer.render_latex(only_tabular=True ))
    


        
    if print_regs==True:
        dfoutput = summary_col([res_ln_wage_N_iota, res_ln_wage_N_iota_l_g, res_ln_wage_N_occ4, res_ln_wage_N_occ4_l_g],stars=True)
        print(dfoutput)
      
    if savefile_stub=="real_data":
        xmin=-8
        xmax=10
        ymin=-1
        ymax=3
            
    elif savefile_stub=="fake_data_rio":
        xmin=-4
        xmax=6
        ymin=-.5
        ymax=.5
        
    elif savefile_stub=="fake_data_pandemic":
        xmin=-4
        xmax=6
        ymin=-.5
        ymax=.5
        
    elif savefile_stub=="fake_data_china":
        xmax=6
        ymin=-.5
        ymax=.5
        
    elif savefile_stub=="fake_data_xxx":
        xmin=-4
        xmax=6
        ymin=-.5
        ymax=.5
        
    else:
        xmin=None
        xmax=None
        ymin=None
        ymax=None
    
      
# Visualizations of the 4 regressions specifications in the main table        
    fig, ax = plt.subplots()
    ax.scatter(agg_iota.iota_exposure_std, agg_iota.delta_ln_real_hrly_wage_dec_N, s=1)
    ax.set_ylabel('$\Delta$ log earnings')
    ax.set_xlabel('iota exposure (market)')
    intercept= np.round(res_ln_wage_N_iota.params[0],3)
    slope    = np.round(res_ln_wage_N_iota.params[1],3)
    std_err  = np.round(res_ln_wage_N_iota.bse[1],   3)
    r2       = np.round(res_ln_wage_N_iota.rsquared, 3)
    corr     = np.round(np.corrcoef(agg_iota.iota_exposure_std, agg_iota.delta_ln_real_hrly_wage_dec_N)[0,1],3)
    ax.set_xlim(None,None)
    ax.set_ylim(None,None)
    ax.text(0.05, 0.9, 'Slope = ' + str(slope) + ' \nStd. Err. = ' + str(std_err) + ' \n$R^2$ = ' + str(r2) +' \nCorr.= ' + str(corr), verticalalignment='top', transform=ax.transAxes) 
    axes = plt.gca()
    X_plot = np.linspace(axes.get_xlim()[0],axes.get_xlim()[1],100)
    ax.plot(X_plot, slope*X_plot + intercept, 'k-', label='Linear fit')
    ax.figure.savefig(figuredir + savefile_stub + "_scatter_ln_wage_N_iota.png", dpi=300, bbox_inches="tight")

    fig, ax = plt.subplots()
    ax.scatter(agg_iota.iota_exposure_l_g_std, agg_iota.delta_ln_real_hrly_wage_dec_N, s=1)
    ax.set_ylabel('$\Delta$ log earnings')
    ax.set_xlabel('iota exposure (market)')
    intercept= np.round(res_ln_wage_N_iota_l_g.params[0],3)
    slope    = np.round(res_ln_wage_N_iota_l_g.params[1],3)
    std_err  = np.round(res_ln_wage_N_iota_l_g.bse[1],   3)
    r2       = np.round(res_ln_wage_N_iota_l_g.rsquared, 3)
    corr     = np.round(np.corrcoef(agg_iota.iota_exposure_l_g_std, agg_iota.delta_ln_real_hrly_wage_dec_N)[0,1],3)
    # ax.set_xlim(xmin,xmax)
    # ax.set_ylim(ymin,ymax)
    ax.text(0.05, 0.9, 'Slope = ' + str(slope) + ' \nStd. Err. = ' + str(std_err) + ' \n$R^2$ = ' + str(r2) +' \nCorr.= ' + str(corr), verticalalignment='top', transform=ax.transAxes) 
    axes = plt.gca()
    X_plot = np.linspace(axes.get_xlim()[0],axes.get_xlim()[1],100)
    ax.plot(X_plot, slope*X_plot + intercept, 'k-', label='Linear fit')
    ax.figure.savefig(figuredir + savefile_stub + "_scatter_ln_wage_N_iota_l_g.png", dpi=300, bbox_inches="tight")
   
    fig, ax = plt.subplots()
    ax.scatter(agg_occ4.occ4_exposure_std, agg_occ4.delta_ln_real_hrly_wage_dec_N, s=1)
    ax.set_ylabel('$\Delta$ log earnings')
    ax.set_xlabel('occ4 exposure (market)')
    intercept= np.round(res_ln_wage_N_occ4.params[0],3)
    slope    = np.round(res_ln_wage_N_occ4.params[1],3)
    std_err  = np.round(res_ln_wage_N_occ4.bse[1],   3)
    r2       = np.round(res_ln_wage_N_occ4.rsquared, 3)
    corr     = np.round(np.corrcoef(agg_occ4.occ4_exposure_std, agg_occ4.delta_ln_real_hrly_wage_dec_N)[0,1],3)
    # ax.set_xlim(xmin,xmax)
    # ax.set_ylim(ymin,ymax)
    ax.text(0.05, 0.9, 'Slope = ' + str(slope) + ' \nStd. Err. = ' + str(std_err) + ' \n$R^2$ = ' + str(r2) +' \nCorr.= ' + str(corr), verticalalignment='top', transform=ax.transAxes) 
    axes = plt.gca()
    X_plot = np.linspace(axes.get_xlim()[0],axes.get_xlim()[1],100)
    ax.plot(X_plot, slope*X_plot + intercept, 'k-', label='Linear fit')
    ax.figure.savefig(figuredir + savefile_stub + "_scatter_ln_wage_N_occ4.png", dpi=300, bbox_inches="tight")
    
    fig, ax = plt.subplots()
    ax.scatter(agg_occ4.occ4_exposure_l_g_std, agg_occ4.delta_ln_real_hrly_wage_dec_N, s=1)
    ax.set_ylabel('$\Delta$ log earnings')
    ax.set_xlabel('occ4 exposure (market)')
    intercept= np.round(res_ln_wage_N_occ4_l_g.params[0],3)
    slope    = np.round(res_ln_wage_N_occ4_l_g.params[1],3)
    std_err  = np.round(res_ln_wage_N_occ4_l_g.bse[1],   3)
    r2       = np.round(res_ln_wage_N_occ4_l_g.rsquared, 3)
    corr     = np.round(np.corrcoef(agg_occ4.occ4_exposure_l_g_std, agg_occ4.delta_ln_real_hrly_wage_dec_N)[0,1],3)
    # ax.set_xlim(xmin,xmax)
    # ax.set_ylim(ymin,ymax)
    ax.text(0.05, 0.9, 'Slope = ' + str(slope) + ' \nStd. Err. = ' + str(std_err) + ' \n$R^2$ = ' + str(r2) +' \nCorr.= ' + str(corr), verticalalignment='top', transform=ax.transAxes) 
    axes = plt.gca()
    X_plot = np.linspace(axes.get_xlim()[0],axes.get_xlim()[1],100)
    ax.plot(X_plot, slope*X_plot + intercept, 'k-', label='Linear fit')
    ax.figure.savefig(figuredir + savefile_stub + "_scatter_ln_wage_N_occ4_l_g.png", dpi=300, bbox_inches="tight")


  
    
    
    r2_vec = []
    coef_vec = []
    se_vec = []
    for r in [res_ln_wage_N_occ4, res_ln_wage_N_occ4_l_g, res_ln_wage_N_occ4, res_ln_wage_N_occ4_l_g]:
        r2_vec.append(r.rsquared)
        coef_vec.append(r.params[1])
        se_vec.append(r.bse[1])
        

    return (data_full, r2_vec, coef_vec, se_vec)

