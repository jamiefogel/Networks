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



def run_regressions(data_full, data_wide, wtype, jtype, sample_name, sample_cond, depvar='d_ln_wage_1990_2000'):

    print(f"Processing: wtype={wtype}, jtype={jtype}")
    cond = (data_full.year==1990) & (data_full['gamma']>0) & (data_full[wtype]!=-1) & (data_full[jtype]!=-1)
    wtype_exposure_sector, wtype_exposure_jtype = compute_exposure_measures(data_full.loc[cond], wtype, jtype, sector_var, dlnonetariff_1990_1995)

    #ln_wage_1990_no_restrict = data_full_wide.groupby([wtype])['ln_real_hrly_wage_dec_1990_N'].mean()
    #ln_wage_2000_no_restrict = data_full_wide.groupby([wtype])['ln_real_hrly_wage_dec_2000_N'].mean()
    ln_wage_1990 = data_full_wide.loc[sample_cond].groupby([wtype])['ln_real_hrly_wage_dec_1990_N'].mean()
    ln_wage_1995 = data_full_wide.loc[sample_cond].groupby([wtype])['ln_real_hrly_wage_dec_1995_N'].mean()
    ln_wage_2000 = data_full_wide.loc[sample_cond].groupby([wtype])['ln_real_hrly_wage_dec_2000_N'].mean()
    employed_1995 = data_full_wide.loc[sample_cond].groupby([wtype])['employed_1995'].mean()
    employed_2000 = data_full_wide.loc[sample_cond].groupby([wtype])['employed_2000'].mean()
    #ln_wage_1990_employed_1990 = data_full_wide.loc[(data_full_wide.employed_1990==1)].groupby([wtype])['ln_real_hrly_wage_dec_1990_N'].mean()
    #ln_wage_2000_employed_1990 = data_full_wide.loc[(data_full_wide.employed_1990==1)].groupby([wtype])['ln_real_hrly_wage_dec_2000_N'].mean()
    
    d_ln_wage_1990_2000 = (ln_wage_2000 - ln_wage_1990).reset_index().rename(columns={0:'d_ln_wage_1990_2000'})
    d_ln_wage_1995_2000 = (ln_wage_2000 - ln_wage_1995).reset_index().rename(columns={0:'d_ln_wage_1995_2000'})
    d_ln_wage_1990_1995 = (ln_wage_1995 - ln_wage_1990).reset_index().rename(columns={0:'d_ln_wage_1990_1995'})
    #d_ln_wage_1990_2000_employed_1990 = (ln_wage_2000_employed_1990 - ln_wage_1990_employed_1990).reset_index().rename(columns={0:'d_ln_wage_1990_2000_employed_1990'})
    #d_ln_wage_1990_2000_no_restrict = (ln_wage_2000_no_restrict - ln_wage_1990_no_restrict).reset_index().rename(columns={0:'d_ln_wage_1990_2000_no_restrict'})
    
    reg_data = d_ln_wage_1990_2000.merge(wtype_exposure_sector, on=wtype, how='inner', validate='1:1')
    reg_data = reg_data.merge(wtype_exposure_jtype, on=wtype, how='inner', validate='1:1')
    reg_data = reg_data.merge(d_ln_wage_1990_1995, on=wtype, how='inner', validate='1:1')
    reg_data = reg_data.merge(d_ln_wage_1995_2000, on=wtype, how='inner', validate='1:1')
    reg_data = reg_data.merge(employed_1995, on=wtype, how='inner', validate='1:1')
    reg_data = reg_data.merge(employed_2000, on=wtype, how='inner', validate='1:1')
    reg_data.rename(columns={f'{wtype}_exposure_sector':'Exposure_Sector'}, inplace=True)
    reg_data.rename(columns={f'{wtype}_exposure_{jtype}':'Exposure_Job_Type'}, inplace=True)

    #reg_wtype_sector_employed_1990          = sm.ols(f'd_ln_wage_1990_2000_employed_1990 ~ {wtype}_exposure_sector', data=reg_data).fit()
    #reg_wtype_jtype_employed_1990           = sm.ols(f'd_ln_wage_1990_2000_employed_1990 ~ {wtype}_exposure_{jtype}' , data=reg_data).fit()
    reg_wtype_sector  = sm.ols(f'{depvar} ~ Exposure_Sector' , data=reg_data).fit()
    reg_wtype_jtype   = sm.ols(f'{depvar} ~ Exposure_Job_Type', data=reg_data).fit()
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
    print(reg_wtype_sector.summary())
    print(idx, ' jtype')
    print(reg_wtype_jtype.summary()) 

    return_dict_sector = {
        'reg_data':reg_data,
        'reg'	:reg_wtype_sector,
        'exposure_type':'Sector',
        'jtype':jtype,
        'wtype':wtype,
        'sample_name':sample_name,
        'sample_cond':sample_cond,
        'depvar':depvar
        }
    return_dict_jtype = {
        'reg_data':reg_data,
        'reg'	:reg_wtype_jtype,
        'exposure_type':'Job Type',
        'jtype':jtype,
        'wtype':wtype,
        'sample_name':sample_name,
        'sample_cond':sample_cond,
        'depvar':depvar
        }
    return return_dict_sector, return_dict_jtype

import pandas as pd
from stargazer.stargazer import Stargazer, LineLocation
import statsmodels.api as sm
import statsmodels.formula.api as smf

def output_regs(spec_dicts, savefile_tex, savefile_csv):
    reg_list = []
    regression_data = []

    for s in spec_dicts:
        # Assuming 'data' is a DataFrame and 'depvar', 'wtype', etc., are column names
        formula = f"{s['depvar']} ~ {s['exposure_type']}"
        reg = smf.ols(formula, data=s['data']).fit()
        reg_list.append(reg)

        # Collect data for CSV export
        regression_summary = {
            'Model': s['sample_name'],  # or any other identifier
            'R-squared': reg.rsquared,
            'Adj. R-squared': reg.rsquared_adj,
            'F-statistic': reg.fvalue,
            'P-value (F-stat)': reg.f_pvalue,
            'Num. obs': int(reg.nobs),
            **{f'coef_{key}': val for key, val in reg.params.items()},
            **{f'pval_{key}': val for key, val in reg.pvalues.items()},
            'Worker Type': s['wtype'],
            'Job Type': s['jtype'],
            'Exposure Type': s['exposure_type']
        }
        regression_data.append(regression_summary)
    
    # Create DataFrame from collected data and save to CSV
    df_regressions = pd.DataFrame(regression_data)
    df_regressions.to_csv(savefile_csv, index=False)

    # Stargazer for LaTeX output
    stargazer = Stargazer(reg_list)
    stargazer.custom_columns([s['sample_name'] for s in spec_dicts], [1]*len(spec_dicts))
    stargazer.show_model_numbers(False)
    stargazer.dep_var_name = None
    stargazer.show_degrees_of_freedom(False)
    stargazer.show_f_statistic = False
    stargazer.show_residual_std_err = False
    stargazer.show_adj_r2 = False
    stargazer.significance_levels([0.05, 0.01, 0.001])  # You can adjust significance levels
    stargazer.add_line('Exposure:', [s['exposure_type'] for s in spec_dicts], LineLocation.BODY_TOP)
    stargazer.add_line('Job Classification:', [s['jtype'] for s in spec_dicts], LineLocation.BODY_TOP)
    stargazer.add_line('Worker classification:', [s['wtype'] for s in spec_dicts], LineLocation.BODY_TOP)

    # Write LaTeX to file
    with open(savefile_tex, "w") as f:
        f.write(stargazer.render_latex())

# Example usage:
spec_dicts = [{
    'data': your_dataframe,
    'depvar': 'dependent_variable',
    'wtype': 'worker_type_column',
    'jtype': 'job_type_column',
    'exposure_type': 'exposure_variable',
    'sample_name': 'Sample 1'
}, {
    'data': your_dataframe,
    'depvar': 'dependent_variable',
    'wtype': 'worker_type_column',
    'jtype': 'job_type_column',
    'exposure_type': 'exposure_variable',
    'sample_name': 'Sample 2'
}]

output_regs(spec_dicts, 'output.tex', 'output.csv')


def output_regs(spec_dicts, savefile):
    reg_list = []
    worker_type_list = []
    job_type_list = []
    exposure_type_list = []
    whitespace_list = []
    sample_list = []
    depvar_list = []
    
    for s in spec_dicts:
        reg_list.append(s['reg'])
        worker_type_list.append(s['wtype'])
        job_type_list.append(s['jtype'])
        exposure_type_list.append(s['exposure_type'])
        whitespace_list.append('')
        sample_list.append(s['sample_name'])
        depvar_list.append(s['depvar'])
    
    stargazer = Stargazer(reg_list)
    stargazer.show_model_numbers(False)
    stargazer.dep_var_name = None
    stargazer.show_degrees_of_freedom(False)
    stargazer.show_f_statistic = False
    stargazer.show_residual_std_err=False
    stargazer.show_adj_r2 = False
    stargazer.significance_levels([10e-100, 10e-101, 10e-102])  # Suppress significance stars
    stargazer.append_notes(False)
    stargazer.add_line('Exposure:', exposure_type_list, LineLocation.BODY_TOP)
    stargazer.add_line('Job Classification:', job_type_list, LineLocation.BODY_TOP)
    stargazer.add_line('Worker classification:',worker_type_list, LineLocation.BODY_TOP)
    stargazer.add_line('Dependent Variable:',depvar_list, LineLocation.BODY_TOP)
    stargazer.add_line('\hline', whitespace_list, LineLocation.BODY_TOP)
    stargazer.add_line('Sample:', sample_list, LineLocation.FOOTER_BOTTOM)
    stargazer.rename_covariates({'Exposure_Job_Type':'Exposure (Job Type)','Exposure_Sector':'Exposure (Sector)'})
    with open(savefile, "w") as f:
        f.write(stargazer.render_latex(only_tabular=True ))


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
samples_dict =  {'Emp. 90 \& 00':(data_full_wide.employed_1990==1) & (data_full_wide.employed_2000==1),
                 'Emp. 90'      :(data_full_wide.employed_1990==1),
                 'Emp. 90 \& 95':(data_full_wide.employed_1990==1) & (data_full_wide.employed_1995==1),
                                 }

results_dict = {}
for idx in classification_list: 
    for sample_name in samples_dict.keys():
        for depvar in ['employed_2000','employed_1995','d_ln_wage_1990_1995','d_ln_wage_1990_2000','d_ln_wage_1995_2000']:
            print(idx, sample_name, depvar)
            return_val_sector, return_val_jtype = run_regressions(data_full, data_full_wide, idx[0], idx[1], sample_name, samples_dict[sample_name], depvar=depvar)
            results_dict[(idx[0],idx[1],sample_name,'sector',depvar)] = return_val_sector
            results_dict[(idx[0],idx[1],sample_name,'jtype', depvar)] = return_val_jtype


pickle.dump(results_dict, open(root + 'Results/trade_shock/trade_shock_regression_results.p', 'wb'))


sample_cond = (data_full_wide.employed_1990==1) & (data_full_wide.employed_2000==1)
sample_name ='Emp. 90 \& 00'
d1_sector, d1_jtype = run_regressions(data_full, data_full_wide, 'iota', 'gamma', sample_name, sample_cond)
d2_sector, d2_jtype = run_regressions(data_full, data_full_wide, 'iota', 'occ2Xmeso', sample_name, sample_cond)


#######################################################################################################################################starg
# Output regressions 
#######################################################################################################################################
spec_dicts_employed_1990_2000 = []
spec_dicts_employed_1990 = []

for s in results_dict:
    if s[2]=='Emp. 90 \\& 00':
        spec_dicts_employed_1990_2000.append(results_dict[s])
    elif s[2]=='Emp. 90':
        spec_dicts_employed_1990.append(results_dict[s])
    

    
output_regs(spec_dicts_employed_1990_2000, root + 'Results/trade_shock_regs_employed_1990_2000.tex') 
output_regs(spec_dicts_employed_1990, root + 'Results/trade_shock_regs_employed_1990.tex') 
    


