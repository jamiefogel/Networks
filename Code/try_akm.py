# -*- coding: utf-8 -*-
"""
Created on Fri May 17 10:46:06 2024

@author: p13861161
"""

from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import gc
import platform
import sys
import getpass
import pytwoway as tw
import bipartitepandas as bpd


homedir = os.path.expanduser('~')
os_name = platform.system()
if getpass.getuser()=='p13861161':
    if os_name == 'Windows':
        print("Running on Windows") 
        root = "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/"
        rais = "//storage6/bases/DADOS/RESTRITO/RAIS/"
        sys.path.append(root + 'Code/Modules')
    elif os_name == 'Linux':
        print("Running on Linux") 
        root = "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/"
        rais = "~/rais/RAIS/"
        sys.path.append(root + 'Code/Modules')
        # These all require torch
        import torch
        from torch_mle import torch_mle
        import bisbm
        from mle_load_fulldata import mle_load_fulldata
        from normalization_k import normalization_k
        from alphas_func import load_alphas
        import solve_model_functions as smf
        from correlogram import correlogram

if getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'
    sys.path.append(root + 'Code/Modules')

# Read directly from the 'balanced' data frame because the version output as the 'earnings_panel' doesn't include id_estab (and thus we can't construct cnpj_raiz either)
modelname = '3states_2013to2016_new'
filename_stub = 'panel_3states_2013to2016_new'
'''
balanced = pd.read_pickle(root + '/Data/derived/panel_'+modelname+'.p')

balanced = balanced[['wid', 'year', 'cbo2002', 'clas_cnae20', 'codemun', 'id_estab', 'idade', 'ind2',
       'jid', 'occ4', 'rem_dez_r', 'sector_IBGE','yob', 
       'ln_real_hrly_wage_dec', 'iota_level_0','gamma_level_0', 'code_meso', 'occ2Xmeso', 'occ2',
       'cbo2002_first', 'cbo2002_recode', 'cbo2002_first_recode', 'occ2_first',
       'occ2_recode', 'occ2_first_recode', 'occ4_first', 'occ4_recode',
       'occ4_first_recode', 'clas_cnae20_first', 'clas_cnae20_recode',
       'clas_cnae20_first_recode', 'occ2Xmeso_first', 'occ2Xmeso_recode',
       'occ2Xmeso_first_recode','wid_masked', 'jid_masked']]


# Create the new variable cnpj_raiz by chopping off the last 6 digits
balanced['cnpj_raiz'] = balanced['id_estab'].str[:-6]
balanced['cbo2002'] =balanced['cbo2002'].astype(int)
balanced[['wid', 'jid', 'year', 'cbo2002', 'clas_cnae20', 'codemun', 'id_estab', 'cnpj_raiz', 'idade', 'ind2',
    'rem_dez_r', 'ln_real_hrly_wage_dec', 'iota_level_0','gamma_level_0', 'code_meso']].to_stata(root + '/Data/derived/akm_data_for_stata.dta')

'''

clean_params = bpd.clean_params(
    {
        'connectedness': 'leave_out_spell',
        'collapse_at_connectedness_measure': True,
        'drop_single_stayers': True,
        'drop_returns': 'returners',
        'copy': False
    }
)


# FE
# Regular AKM
fe_params1 = tw.fecontrol_params(
    {
        'he': False,
        'ho': False,
        'attach_fe_estimates': True,
        #'categorical_controls': 'cat_control',
        #'continuous_controls': 'cts_control',
        'Q_var': [
            tw.Q.VarCovariate('psi'),
            tw.Q.VarCovariate('alpha'),
            #tw.Q.VarCovariate('cat_control'),
            #tw.Q.VarCovariate('cts_control'),
            tw.Q.VarCovariate(['psi', 'alpha']),
            #tw.Q.VarCovariate(['cat_control', 'cts_control'])
                 ],
    
        'ncore': 8
    }
)

# Including categorical controls
fe_params2 = tw.fecontrol_params(
    {
        'he': False,
        'ho': False,
        'attach_fe_estimates': True,
        'categorical_controls': 'cat_control',
        #'continuous_controls': 'cts_control',
        'Q_var': [
            tw.Q.VarCovariate('psi'),
            tw.Q.VarCovariate('alpha'),
            tw.Q.VarCovariate('cat_control'),
            #tw.Q.VarCovariate('cts_control'),
            tw.Q.VarCovariate(['psi', 'alpha']),
            #tw.Q.VarCovariate(['cat_control', 'cts_control'])
                 ],
        'Q_cov': [
            tw.Q.CovCovariate('psi', 'alpha'),
            #tw.Q.CovCovariate('cat_control', 'cts_control'),
            tw.Q.CovCovariate(['psi'], ['cat_control']),
            tw.Q.CovCovariate(['alpha'], ['cat_control']),
            tw.Q.CovCovariate(['psi', 'alpha'], ['cat_control'])
        ],
        'ncore': 8
    }
)


# Including categorical controls; No Q
fe_params3 = tw.fecontrol_params(
    {
        'he': False,
        'ho': False,
        'attach_fe_estimates': True,
        #'categorical_controls': 'cat_control',
        #'continuous_controls': 'cts_control',
        'Q_var': None,
        'Q_cov': None,
        'ncore': 8
    }
)

fe_params4 = tw.fecontrol_params(
    {
        'he': False,
        'ho': False,
        'attach_fe_estimates': True,
        'categorical_controls': 'cat_control',
        #'continuous_controls': 'cts_control',
        'Q_var': None,
        'Q_cov': None,
        'ncore': 8
    }
)


for firmvar in ['gamma_level_0']: #,'firmid_masked']:  #
    print(firmvar)
    '''
    data = pd.DataFrame({'i':balanced.wid_masked,'j':balanced.cnpj_raiz, 'cat_controls':balanced[firmvar],'y':balanced.ln_real_hrly_wage_dec,'t':balanced.year})
    data['i'] = data['i'].astype(int)
    data['j'] = pd.to_numeric(data['j'], errors='coerce').fillna(-1).astype(int)
    data = data.loc[data['j'] != -1]
    data = data.loc[data['cat_controls'] != -1]
    data['cat_controls'] = data['cat_controls'].astype(int)
    data.dropna(axis=0, inplace=True)
    data = data.loc[(data.i>0) & (data.j>0) & (data.cat_controls>0) ]
    data = bpd.BipartiteLong(data)
    
    
    data.to_pickle(root + '/Data/derived/temp.p')

    '''
    data = pd.read_pickle(root + '/Data/derived/temp.p')
    # For the example, we simulate data
    #sim_data = bpd.SimBipartite().sim_network()
    #display(sim_data)
    
    bdf = bpd.BipartiteDataFrame(data)
    bdf = bdf.clean(clean_params)
    bdf.to_stata(root + '/Data/derived/bdf_akm_data_for_stata.dta')




    bdf3 = bpd.BipartiteDataFrame(data)
    # Clean and collapse
    bdf3 = bdf3.clean(clean_params)

    bdf4 = bpd.BipartiteDataFrame(data)
    # Clean and collapse
    bdf4 = bdf4.clean(clean_params)
    #####
    # Regular AKM
    
    fe_estimator1 = tw.FEEstimator(bdf, fe_params1)
    fe_estimator1.fit()
    print(fe_estimator1.summary)
    

    #####
    # Controlling for market
    
    fe_estimator2 = tw.FEEstimator(bdf, fe_params2)
    fe_estimator2.fit()
    print(fe_estimator2.summary)

    #####
    # Controlling for market, no Q
    
    fe_estimator3 = tw.FEEstimator(bdf3, fe_params3)
    fe_estimator3.fit()
    print(fe_estimator3.summary)

    fe_estimator4 = tw.FEEstimator(bdf4, fe_params3)
    fe_estimator4.fit()
    print(fe_estimator4.summary)


# XX This code automatically keeps the highest paying job in a given year. Therefore, we should be able to run this on much rawer data than our balanced earnings panel


'''


    summary = fe_estimator.summary
    # compute variance of worker FEs (alpha)
    summary['var(alpha)_fe'] = summary['var(y)'] - summary['var(psi)_fe'] - 2 * summary['cov(psi, alpha)_fe'] - summary['var(eps)_fe']
    
    var_y = summary['var(y)']
    var_alpha_fe = summary['var(alpha)_fe']
    var_eps_fe = summary['var(eps)_fe']
    var_psi_fe = summary['var(psi)_fe']
    cov_psi_alpha_fe = summary['cov(psi, alpha)_fe']

    # Variance decomposition
    total_variance = var_y
    worker_effects_variance = var_alpha_fe
    firm_effects_variance = var_psi_fe
    residual_variance = var_eps_fe
    
    # Calculate proportions
    proportion_worker_effects = worker_effects_variance / total_variance
    proportion_firm_effects = firm_effects_variance / total_variance
    proportion_residuals = residual_variance / total_variance
    covariance_effects = cov_psi_alpha_fe / total_variance
    
    proportion_worker_effects + proportion_firm_effects + proportion_residuals + covariance_effects
    
    # Print results
    print(f"Proportion of variance explained by worker effects: {proportion_worker_effects:.2%}")
    print(f"Proportion of variance explained by firm effects: {proportion_firm_effects:.2%}")
    print(f"Proportion of variance explained by residuals: {proportion_residuals:.2%}")
    print(f"Covariance between worker and firm effects: {covariance_effects:.2%}")

    
    
firm_level   = pickle.load(open(homedir + "/Networks/Code/aug2021/results/akm_estimates_firmid_masked.p", "rb"))
gamma_level  = pickle.load(open(homedir + "/Networks/Code/aug2021/results/akm_estimates_gamma.p", "rb"))


firm_share  =  firm_level['var_fe']/ firm_level['var_y']
firm_cov_share = firm_level['cov_fe']/ firm_level['var_y']
gamma_share = gamma_level['var_fe']/gamma_level['var_y']
gamma_cov_share = gamma_level['cov_fe']/ gamma_level['var_y']

print('Firm-level:')
print('Share of variance explained by firm effects: '+str(round(firm_share,3)))
print('Share of variance explained by worker FE--firm FE covariance: '+str(round(firm_cov_share,3)))


print('Gamma-level:')
print('Share of variance explained by gamma effects: '+str(round(gamma_share,3)))
print('Share of variance explained by worker FE--gamma FE covariance: '+str(round(gamma_cov_share,3)))



########################################################
# Simulating AKM with controls (see https://tlamadon.github.io/pytwoway/notebooks/fe_example.html#FE-WITH-CONTROLS)

### 
# Defining parameters

# Cleaning
clean_params = bpd.clean_params(
    {
        'connectedness': 'leave_out_spell',
        'collapse_at_connectedness_measure': True,
        'drop_single_stayers': True,
        'drop_returns': 'returners',
        'copy': False
    }
)
# Simulating
nl = 3
nk = 4
n_control = 2 
sim_cat_params = tw.sim_categorical_control_params({
    'n': n_control,
    'worker_type_interaction': False,
    'stationary_A': True, 'stationary_S': True
})
sim_cts_params = tw.sim_continuous_control_params({
    'worker_type_interaction': False,
    'stationary_A': True, 'stationary_S': True
})
sim_blm_params = tw.sim_blm_params({
    'nl': nl,
    'nk': nk,
    'categorical_controls': {
        'cat_control': sim_cat_params
    },
    'continuous_controls': {
        'cts_control': sim_cts_params
    },
    'stationary_A': True, 'stationary_S': True,
    'linear_additive': True
})

###
# Generating data
blm_true = tw.SimBLM(sim_blm_params)
sim_data = blm_true.simulate(return_parameters=False)
jdata, sdata = sim_data['jdata'], sim_data['sdata']
sim_data = pd.concat([jdata, sdata]).rename({'g': 'j', 'j': 'g'}, axis=1, allow_optional=True, allow_required=True)[['i', 'j1', 'j2', 'y1', 'y2', 'cat_control1', 'cat_control2', 'cts_control1', 'cts_control2']].construct_artificial_time(is_sorted=True, copy=False)



###
# Prepare data

# Convert into BipartitePandas DataFrame
bdf = bpd.BipartiteDataFrame(sim_data)
# Clean and collapse
bdf = bdf.clean(clean_params)
# Convert to long format
bdf = bdf.to_long(is_sorted=True, copy=False)


###
# Initialize and run FE estimator



# FE
fecontrol_params1 = tw.fecontrol_params(
    {
        #'he': True,
        'attach_fe_estimates': True,
        'categorical_controls': 'cat_control',
        'continuous_controls': 'cts_control',
        'Q_var': [
            tw.Q.VarCovariate('psi'),
            tw.Q.VarCovariate('alpha'),
            tw.Q.VarCovariate('cat_control'),
            tw.Q.VarCovariate('cts_control'),
            tw.Q.VarCovariate(['psi', 'alpha']),
            tw.Q.VarCovariate(['cat_control', 'cts_control'])
                 ],
        'Q_cov': [
            tw.Q.CovCovariate('psi', 'alpha'),
            tw.Q.CovCovariate('cat_control', 'cts_control'),
            tw.Q.CovCovariate(['psi', 'alpha'], ['cat_control', 'cts_control'])
        ],
        'ncore': 8
    }
)


# FE
fecontrol_params2 = tw.fecontrol_params(
    {
        'he': False,
        'ho': False,
        'attach_fe_estimates': True,
        'categorical_controls': 'cat_control',
        #'continuous_controls': 'cts_control',
        'Q_var': [
            tw.Q.VarCovariate('psi'),
            tw.Q.VarCovariate('alpha'),
            tw.Q.VarCovariate('cat_control'),
            #tw.Q.VarCovariate('cts_control'),
            tw.Q.VarCovariate(['psi', 'alpha']),
            #tw.Q.VarCovariate(['cat_control', 'cts_control'])
                 ],
        'Q_cov': [
            tw.Q.CovCovariate('psi', 'alpha'),
            #tw.Q.CovCovariate('cat_control', 'cts_control'),
            tw.Q.CovCovariate(['psi'], ['cat_control']),
            tw.Q.CovCovariate(['alpha'], ['cat_control']),
            tw.Q.CovCovariate(['psi', 'alpha'], ['cat_control'])
        ],
        'ncore': 8
    }
)

# Initialize FE estimator
fe_estimator = tw.FEControlEstimator(bdf, fecontrol_params2)
# Fit FE estimator
fe_estimator.fit()

summary = fe_estimator.summary

# Confirm that I'm getting the decomposition right. It's not exactly right but very close
var_y_hat = summary['var(eps)_fe'] + summary['var(alpha)_fe'] + summary['var(cat_control)_fe'] + summary['var(psi)_fe'] + 2*summary['cov(alpha, cat_control)_fe'] + 2*summary['cov(psi, alpha)_fe'] + summary['cov(psi, cat_control)_fe']
print(var_y_hat)
print(summary['var(y)'])
'''