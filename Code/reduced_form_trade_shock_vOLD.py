#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 17:03:46 2021

In our 7/28/2021 meeting Matthew suggested I do the reduced form exercise for a bunch of different simulated shocks. I implement that here.

@author: jfogel
"""

'''
1. Load everything: mle data, mle data sums, mle estimates, psi and k, model parameters (a_s and alphas)
2. Solve the model given demand shifters for a particular year (we have chosen 2009. Not sure why.)
3. Load the raw panel data. We do this because we're going to use the empirical distribution of iotas, gammas, and c's
4. Draw workers' choices of gamma, wages, and sectors using our model-implied DGP
'''

import matplotlib.pyplot as plt
import numpy as np
import time
from dgp_func import dgp
from bartik_analysis import bartik_analysis
from solve_model_functions import *
from stargazer.stargazer import Stargazer, LineLocation
import statsmodels.formula.api as sm
from statsmodels.iolib.summary2 import summary_col

obs = 1e5
homedir = os.path.expanduser("~")

t0 = time.time()

np.random.seed(20210316)


def compute_shares(var1, var2, data):
    crosstab_var1_var2 = pd.crosstab(index = data[var1], columns = data[var2])
    var2_shares_var1 = crosstab_var1_var2.div(crosstab_var1_var2.sum(axis=1),axis=0)
    print(f"Computed shares of {var2} within each value of {var1}" )
    return var2_shares_var1


def compute_exposure_measures(data, wtype, jtype, sector_var, dlnonetariff_1990_1995):
    sector_shares_of_wtype = compute_shares(wtype, sector_var, data)
    sector_shares_of_jtype = compute_shares(jtype, sector_var, data)
    jtype_shares_of_wtype = compute_shares(wtype, jtype, data)

    E_delta_tariff_by_jtype = sector_shares_of_jtype.dot(dlnonetariff_1990_1995)
    E_delta_tariff_by_sector = dlnonetariff_1990_1995

    wtype_exposure_sector = sector_shares_of_wtype.dot(E_delta_tariff_by_sector).reset_index().rename(columns={'dlnonetariff_1990_1995': f'{wtype}_exposure_sector'})
    wtype_exposure_jtype = jtype_shares_of_wtype.dot(E_delta_tariff_by_jtype).reset_index().rename(columns={'dlnonetariff_1990_1995': f'{wtype}_exposure_{jtype}'})

    return wtype_exposure_sector, wtype_exposure_jtype



########################################################################################################################
# 1. Load everything: mle data, mle data sums, mle estimates, psi and k, model parameters (a_s and alphas)
########################################################################################################################

#XX worker_type_var, job_type_var and filename_stub should probably all be arguments to a function

worker_type_var = 'iota'
job_type_var    = 'gamma'
wtype = worker_type_var
jtype = job_type_var
upperlim = 800 # Drop very large wages


wtypes ['iota','occ4_first']
jtypes = ['gamma']

# Load tariff changes and merge onto crosswalk between the two different subs_ibge codes. subsibge is the version in the trade shock data and subs_ibge is the version on RAIS.
df_subsibge_rais = pd.read_stata(root + "Code\DixCarneiro_Kovak_2017/Data_Other/subsibge_to_subsibge_rais.dta")
df_tariff_chg = pd.read_stata(root + "Code/DixCarneiro_Kovak_2017/Data/tariff_chg_kume_subsibge.dta")
df_tariff_chg = df_subsibge_rais.merge(df_tariff_chg[['subsibge', 'dlnonetariff_1990_1995']], on='subsibge', how='left', validate='1:1')
df_tariff_chg['dlnonetariff_1990_1995'].fillna(0, inplace=True)
dlnonetariff_1990_1995 = df_tariff_chg.set_index('subsibge')[['dlnonetariff_1990_1995']]

# Load DGP data and restrict to obs with an iota and a gamma and non-missing wtype and jtype if they are not iota and gamma.
data_full = pd.read_csv(mle_data_filename)
data_full = data_full.loc[(data_full['gamma']!=-1) & (data_full['iota']!=-1)]
drop = data_full.real_hrly_wage_dec>upperlim
data_full = data_full.loc[drop==False]

# Reshape to wide
data_full['employed'] = ((data_full[jtype]>0)*1).astype(float)
data_full['year_str'] = data_full.year.astype(str)
data_full_wide = data_full.pivot(index=['wid_masked','iota','occ4_first','occ2Xmeso_first'], columns='year_str', values=['employed','real_hrly_wage_dec','ln_real_hrly_wage_dec',sector_var,jtype])

data_full_wide.columns = ['_'.join(col) for col in data_full_wide.columns.values]
data_full_wide = data_full_wide.reset_index()
data_full_wide = data_full_wide.loc[data_full_wide.employed_1990==1]



# Create a second version of earnings vars filling in NANs with 0s
for year in [1990, 1995, 2000]:
    data_full_wide['ln_real_hrly_wage_dec_' + str(year) + '_N'] = data_full_wide['ln_real_hrly_wage_dec_' + str(year)].fillna(0)


### Compute exposure measures (RHS variables)
classification_list = [('iota','gamma'),('occ4_first','gamma'),('occ2Xmeso_first','gamma'),('iota','occ2Xmeso'),('occ4_first','occ2Xmeso'),('occ2Xmeso_first','occ2Xmeso')]
exposure_dict = {}
sample_cond = (data_full_wide.employed_1990==1) & (data_full_wide.employed_2000==1)
for idx in classification_list: #
    wtype = idx[0]
    jtype = idx[1]
    print(f"Processing: wtype={wtype}, jtype={jtype}")
    cond = (data_full.year==1990) & (data_full['gamma']>0) & (data_full[wtype]!=-1) & (data_full[jtype]!=-1)
    wtype_exposure_sector, wtype_exposure_jtype = compute_exposure_measures(data_full.loc[cond], wtype, jtype, sector_var, dlnonetariff_1990_1995)

    #ln_wage_1990_no_restrict = data_full_wide.groupby([wtype])['ln_real_hrly_wage_dec_1990_N'].mean()
    #ln_wage_2000_no_restrict = data_full_wide.groupby([wtype])['ln_real_hrly_wage_dec_2000_N'].mean()
    ln_wage_1990_employed_1990and2000 = data_full_wide.loc[sample_cond].groupby([wtype])['ln_real_hrly_wage_dec_1990_N'].mean()
    ln_wage_2000_employed_1990and2000 = data_full_wide.loc[sample_cond].groupby([wtype])['ln_real_hrly_wage_dec_2000_N'].mean()
    #ln_wage_1990_employed_1990 = data_full_wide.loc[(data_full_wide.employed_1990==1)].groupby([wtype])['ln_real_hrly_wage_dec_1990_N'].mean()
    #ln_wage_2000_employed_1990 = data_full_wide.loc[(data_full_wide.employed_1990==1)].groupby([wtype])['ln_real_hrly_wage_dec_2000_N'].mean()
    
    d_ln_wage_1990_2000 = (ln_wage_2000 - ln_wage_1990).reset_index().rename(columns={0:'d_ln_wage_1990_2000'})
    #d_ln_wage_1990_2000_employed_1990 = (ln_wage_2000_employed_1990 - ln_wage_1990_employed_1990).reset_index().rename(columns={0:'d_ln_wage_1990_2000_employed_1990'})
    #d_ln_wage_1990_2000_no_restrict = (ln_wage_2000_no_restrict - ln_wage_1990_no_restrict).reset_index().rename(columns={0:'d_ln_wage_1990_2000_no_restrict'})
    
    reg_data = d_ln_wage_1990_2000_employed_1990and2000.merge(d_ln_wage_1990_2000_no_restrict, on=wtype, how='inner', validate='1:1')
    #reg_data = reg_data.merge(d_ln_wage_1990_2000_employed_1990, on=wtype, how='inner', validate='1:1')
    reg_data = reg_data.merge(wtype_exposure_sector, on=wtype, how='inner', validate='1:1')
    reg_data = reg_data.merge(wtype_exposure_jtype, on=wtype, how='inner', validate='1:1')
    reg_data.rename(columns={f'{wtype}_exposure_sector':'Exposure (Sector)'})
    reg_data.rename(columns={f'{wtype}_exposure_{jtype}':'Exposure (Job Type)'})

    #reg_wtype_sector_employed_1990          = sm.ols(f'd_ln_wage_1990_2000_employed_1990 ~ {wtype}_exposure_sector', data=reg_data).fit()
    #reg_wtype_jtype_employed_1990           = sm.ols(f'd_ln_wage_1990_2000_employed_1990 ~ {wtype}_exposure_{jtype}' , data=reg_data).fit()
    reg_wtype_sector_employed_1990and2000   = sm.ols(f'd_ln_wage_1990_2000_employed_1990and2000 ~ {wtype}_exposure_sector', data=reg_data).fit()
    reg_wtype_jtype_employed_1990and2000    = sm.ols(f'd_ln_wage_1990_2000_employed_1990and2000 ~ {wtype}_exposure_{jtype}' , data=reg_data).fit()
    #reg_wtype_sector_no_restrict            = sm.ols(f'd_ln_wage_1990_2000_no_restrict ~ {wtype}_exposure_sector', data=reg_data).fit()
    #reg_wtype_jtype_no_restrict             = sm.ols(f'd_ln_wage_1990_2000_no_restrict ~ {wtype}_exposure_{jtype}' , data=reg_data).fit()
    
    '''   
    reg_wtype_sector_employed_1990          = sm.ols("Q('d_ln_wage_1990_2000_employed_1990') ~ Q('Exposure (Sector)')", data=reg_data).fit()
    reg_wtype_jtype_employed_1990           = sm.ols("Q('d_ln_wage_1990_2000_employed_1990') ~ Q('Exposure (Job Type)')", data=reg_data).fit()
    reg_wtype_sector_employed_1990and2000   = sm.ols("Q('d_ln_wage_1990_2000_employed_1990and2000') ~ Q('Exposure (Sector)')", data=reg_data).fit()
    reg_wtype_jtype_employed_1990and2000    = sm.ols("Q('d_ln_wage_1990_2000_employed_1990and2000') ~ Q('Exposure (Job Type)')", data=reg_data).fit()
    reg_wtype_sector_no_restrict            = sm.ols("Q('d_ln_wage_1990_2000_no_restrict') ~ Q('Exposure (Sector)')", data=reg_data).fit()
    reg_wtype_jtype_no_restrict             = sm.ols("Q('d_ln_wage_1990_2000_no_restrict') ~ Q('Exposure (Job Type)')", data=reg_data).fit()
    '''
    
    print(idx, ' sector')
    print(reg_wtype_sector_employed_1990and2000.summary())
    print(idx, ' jtype')
    print(reg_wtype_jtype_employed_1990and2000.summary()) 

    exposure_dict[idx] = {
        'sector_exposure':wtype_exposure_sector,
        'jtype_exposure':wtype_exposure_jtype,
        'd_ln_wage_1990_2000_employed_1990and2000':d_ln_wage_1990_2000_employed_1990and2000,
        'd_ln_wage_1990_2000_employed_1990':d_ln_wage_1990_2000_employed_1990,
        'd_ln_wage_1990_2000_no_restrict':d_ln_wage_1990_2000_no_restrict,
        'reg_data':reg_data,
        'reg_wtype_sector_employed_1990'	:reg_wtype_sector_employed_1990,
        'reg_wtype_jtype_employed_1990'		:reg_wtype_jtype_employed_1990,
        'reg_wtype_sector_employed_1990and2000'	:reg_wtype_sector_employed_1990and2000,
        'reg_wtype_jtype_employed_1990and2000'	:reg_wtype_jtype_employed_1990and2000,
        'reg_wtype_sector_no_restrict'		:reg_wtype_sector_no_restrict,
        'reg_wtype_jtype_no_restrict'		:reg_wtype_jtype_no_restrict,
        'jtype':jtype,
        'wtype':wtype
        }


#######################################################################################################################################starg
# Output regressions 
#######################################################################################################################################

def add_models_to_stargazer(model_indices, exposure_labels, classification_labels):
    models_to_add = []
    exposure_header = []
    classification_header = []
    
    for idx in model_indices:
        models_to_add.append(exposure_dict[idx]['reg_model'])  # Assuming you store models with a key 'reg_model'
        exposure_header.append(exposure_labels[idx])
        classification_header.append(classification_labels[idx])
    
    stargazer = Stargazer(models_to_add)
    stargazer.add_line('Exposure:', exposure_header, LineLocation.BODY_TOP)
    stargazer.add_line('Worker classification:', classification_header, LineLocation.BODY_TOP)
    stargazer.show_model_numbers(False)
    stargazer.dep_var_name = None
    stargazer.show_degrees_of_freedom(False)
    stargazer.show_f_statistic = False
    stargazer.show_residual_std_err = False
    stargazer.show_adj_r2 = False
    stargazer.significance_levels([10e-100, 10e-101, 10e-102])  # Suppress significance stars
    stargazer.append_notes(False)
    
    return stargazer




stargazer = Stargazer([exposure_dict[('iota', 'gamma')]['reg_wtype_jtype_employed_1990and2000'], 
                       exposure_dict[('iota', 'gamma')]['reg_wtype_sector_employed_1990and2000'], 
                       exposure_dict[('occ4_first', 'gamma')]['reg_wtype_jtype_employed_1990and2000'], 
                       exposure_dict[('occ4_first', 'gamma')]['reg_wtype_sector_employed_1990and2000'], 
                       exposure_dict[('occ2Xmeso_first', 'gamma')]['reg_wtype_jtype_employed_1990and2000'], 
                       exposure_dict[('occ2Xmeso_first', 'gamma')]['reg_wtype_sector_employed_1990and2000']])
stargazer.show_model_numbers(False)
stargazer.dep_var_name = None
stargazer.show_degrees_of_freedom(False)
stargazer.show_f_statistic = False
stargazer.show_residual_std_err=False
stargazer.show_adj_r2 = False
stargazer.significance_levels([10e-100, 10e-101, 10e-102])  # Suppress significance stars
stargazer.append_notes(False)
stargazer.add_line('Exposure:', ['Market ($\gamma$)', 'Sector', 'Market ($\gamma$)', 'Sector', 'Market ($\gamma$)', 'Sector'], LineLocation.BODY_TOP)
stargazer.add_line('Worker classification:', ['Worker type ($\iota$)', 'Worker type ($\iota$)', 'Occ4', 'Occ4', 'Occ2 $\\times$ Meso','Occ2 $\\times$ Meso'], LineLocation.BODY_TOP)
stargazer.add_line('\hline', ['', '', '', '', '', ''], LineLocation.BODY_TOP)
with open(figuredir + 'trade_shock_regs.tex', "w") as f:
    f.write(stargazer.render_latex(only_tabular=True ))


    