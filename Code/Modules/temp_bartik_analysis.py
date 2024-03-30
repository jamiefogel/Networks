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


''' Edits made on 11/7
- Added function arguments for firstyear and lastyear
- Replaced all instances of 2009 and 2014 with firstyear and lastyear, respectively. This avoids hard-coding the relevant years. 
'''

# Temp line that needs to be deleted
#bartik_analysis(mle_data_filename,  y_ts=y_ts, shock_source='data', figuredir=figuredir, savefile_stub='real_data') 
#shock_source='data'
#figuredir=figuredir
#savefile_stub='real_data'
#equi_shock=None
#equi_pre=None
#firstyear=2009
#lastyear=2014
#wtype = 'occ2Xmeso_first_recode'
#jtype = 'gamma'

def bartik_analysis(mle_data_filename, wtype, jtype, firstyear, lastyear, figuredir='', equi_shock=None, equi_pre=None, y_ts=None, shock_source='model', savefile_stub='', print_regs=True):
        
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
    
    
    # Load DGP data and restrict to obs with an iota and a gamma and non-missing wtype and jtype if they are not iota and gamma.
    data_full = pd.read_csv(mle_data_filename)
    data_full = data_full.loc[(data_full['gamma']!=-1) & (data_full['iota']!=-1) & (data_full[wtype]!=-1) & (data_full[jtype]!=-1)]
    data_full = data_full.loc[(data_full.year==firstyear) | (data_full.year==lastyear)]
    drop = data_full.real_hrly_wage_dec>upperlim
    data_full = data_full.loc[drop==False]
    
    # Define the shocks
    if shock_source=='model':
        shock_s    = (equi_shock['y_s']/equi_pre['y_s']-1).numpy()
        shock_jtype  = (equi_shock['l_g']/equi_pre['l_g']-1).numpy()
    
    if shock_source=='data':
        shock_s = y_ts.loc[y_ts.index==lastyear].values/y_ts.loc[y_ts.index==firstyear].values-1
        jtype_sums_firstyear    = data_full.loc[data_full.year==firstyear].groupby([jtype])['real_hrly_wage_dec'].sum().reset_index().rename(columns={'real_hrly_wage_dec':'w_firstyear'})
        jtype_sums_lastyear    = data_full.loc[data_full.year==lastyear].groupby([jtype])['real_hrly_wage_dec'].sum().reset_index().rename(columns={'real_hrly_wage_dec':'w_lastyear'})
        jtype_sums = jtype_sums_firstyear.merge(jtype_sums_lastyear, on=jtype, how='left')
        jtype_sums = jtype_sums.loc[jtype_sums[jtype]!=0]
        shock_jtype = np.array(jtype_sums.w_lastyear/jtype_sums.w_firstyear - 1)
            
    
    # Worker type exposure measures (edited to not hard code the worker type variable)
    # XX we also shouldn't hard code separate measures for sector vs gamma since in this case they're both just different ways of classifying jobs. I think I need to replace the pairs of lines below with single lines that reference 'jtype' instead of 'gamma' or 'sector'
    crosstab_wtype_sector = pd.crosstab(index = data_full.loc[data_full.year==firstyear                        ,wtype], columns = data_full.loc[data_full.year==firstyear, 'sector_IBGE'])
    # XX Need to think about if I need to make the cut data_full[jtype]>0 for jtypes other than gamma
    crosstab_wtype_jtype  = pd.crosstab(index = data_full.loc[(data_full.year==firstyear) & (data_full[jtype]>0),wtype], columns = data_full.loc[(data_full.year==firstyear) & (data_full[jtype]>0), jtype])   # Drop non-employment from these shares
    sector_shares_wtype = crosstab_wtype_sector.div(crosstab_wtype_sector.sum(axis=1),axis=0)
    jtype_shares_wtype  = crosstab_wtype_jtype.div( crosstab_wtype_jtype.sum(axis=1),axis=0)
    
    # Need to update this to allow me to specify how the shock is defined. Basically, is the shock at the sector level or the jtype level? And what if jtype=sector? Can I make it flexible enough to handle that?
    wtype_exposure = sector_shares_wtype.multiply(shock_s, axis=1).sum(axis=1).reset_index().rename(columns={0:'wtype_exposure'})
    wtype_exposure_jtype = jtype_shares_wtype.multiply(shock_jtype, axis=1).sum(axis=1).reset_index().rename(columns={0:'wtype_exposure_jtype'})
    
    wtype_exposure['wtype_exposure_std']         = (wtype_exposure[    'wtype_exposure'    ]-wtype_exposure[    'wtype_exposure'    ].mean())/wtype_exposure[    'wtype_exposure'    ].std()
    wtype_exposure_jtype['wtype_exposure_jtype_std'] = (wtype_exposure_jtype['wtype_exposure_jtype']-wtype_exposure_jtype['wtype_exposure_jtype'].mean())/wtype_exposure_jtype['wtype_exposure_jtype'].std()    
    
    # Reshape to wide
    data_full['employed'] = ((data_full[jtype]>0)*1).astype(float)
    data_full['year'] = data_full.year.astype(str)
    data_full = data_full.pivot(index='wid_masked', columns='year', values=['employed','real_hrly_wage_dec','ln_real_hrly_wage_dec',wtype,'sector_IBGE',jtype])
    
    data_full.columns = ['_'.join(col) for col in data_full.columns.values]
    data_full = data_full.drop(columns=[wtype + '_' + str(lastyear)]).rename(columns={wtype + '_' + str(firstyear) :wtype})
    
    
    # XX Should only need one of these: wtype_exposure
    data_full = data_full.merge(wtype_exposure,     on=wtype)
    data_full = data_full.merge(wtype_exposure_jtype, on=wtype)
    
    #data_full['delta_ln_real_hrly_wage_dec_N'] = data_full['ln_real_hrly_wage_dec_'+str(lastyear)].fillna(0)-data_full['ln_real_hrly_wage_dec_'+str(firstyear)].fillna(0)
    
    # converting the Y variables to float objects
    data_full['ln_real_hrly_wage_dec_' + str(firstyear) + '_N'] = data_full['ln_real_hrly_wage_dec_' + str(firstyear)].fillna(0)
    data_full['ln_real_hrly_wage_dec_' + str(lastyear)  + '_N'] = data_full['ln_real_hrly_wage_dec_' + str(lastyear) ].fillna(0)
    
    
    #######################################################################################################################################stargazer
    # At the aggregate level
    #######################################################################################################################################
    
    agg = (data_full.groupby([wtype])['ln_real_hrly_wage_dec_' + str(lastyear) + '_N'].mean() / data_full.groupby([wtype])['ln_real_hrly_wage_dec_' +str(firstyear) + '_N'].mean()-1).reset_index().rename(columns={0:'delta_ln_real_hrly_wage_dec_N'})
    agg = agg.merge(wtype_exposure, on=wtype)
    agg = agg.merge(wtype_exposure_jtype, on=wtype)
          
    res_ln_wage_N     = sm.ols('delta_ln_real_hrly_wage_dec_N ~ wtype_exposure_std'    , data=agg).fit()
    res_ln_wage_N_jtype = sm.ols('delta_ln_real_hrly_wage_dec_N ~ wtype_exposure_jtype_std', data=agg).fit()
    
    print(res_ln_wage_N.summary())
    print(res_ln_wage_N_jtype.summary())
    
    return (res_ln_wage_N, res_ln_wage_N_jtype, agg)


'''
Intercept             0.006061
wtype_exposure_std    0.006068
dtype: float64
>>> 0.05109229250661251
>>> Intercept                   0.005574
wtype_exposure_jtype_std    0.005580
dtype: float64
>>> 0.19763871792566712
'''

# Everything below this should probably be moved out of the function
'''    
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
    
    
    
    r2_vec = []
    coef_vec = []
    se_vec = []
    for r in [res_ln_wage_N_occ4, res_ln_wage_N_occ4_l_g, res_ln_wage_N_occ4, res_ln_wage_N_occ4_l_g]:
        r2_vec.append(r.rsquared)
        coef_vec.append(r.params[1])
        se_vec.append(r.bse[1])
        

    return (data_full, r2_vec, coef_vec, se_vec)

'''
