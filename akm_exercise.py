#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 21 13:29:20 2021

@author: jfogel
"""

# See https://mybinder.org/v2/gh/tlamadon/pytwoway/HEAD?filepath=docs%2Fsource%2Fnotebooks%2Ffe_example.ipynb


import pytwoway as tw
import bipartitepandas as bpd

import os
import pandas as pd
#import torch
import numpy as np
import pickle
from datetime import datetime


full = pd.read_csv("~/Networks/RAIS_exports/earnings_panel/akm_vars.csv")

## Optional Parameters ##
# clean_params = {
#     'connectedness': 'connected', # When computing largest connected set of firms:
#         # If 'connected', keep observations in the largest connected set of firms;
#         # If 'biconnected', keep observations in the largest biconnected set of firms;
#         # If None, keep all observations
#     'i_t_how': 'max', # How to handle worker-year duplicates
#         # If 'max', keep max paying job;
#         # If 'sum', sum over duplicate worker-firm-year observations,
#             # then take the highest paying worker-firm sum;
#         # If 'mean', average over duplicate worker-firm-year observations,
#             # then take the highest paying worker-firm average.
#         # Note that if multiple time and/or firm columns are included
#             # (as in event study format), then data is converted to long,
#             # cleaned, then reconverted to its original format
#     'data_validity': True, # If True, run data validity checks; much faster if set to False
#     'copy': False # If False, avoid copy
# }
clean_params = bpd.clean_params(
    {
        'connectedness': 'leave_out_spell',
        'collapse_at_connectedness_measure': True,
        'drop_single_stayers': True,
        'drop_returns': 'returners',
        'copy': False
    }
)
    

## Optional Parameters ##
# fe_params = {
#     'ncore': 1, # Number of cores to use
#     'batch': 1, # Batch size to send in parallel
#     'ndraw_pii': 50, # Number of draws to use in approximation for leverages
#     'levfile': '', # File to load precomputed leverages
#     'ndraw_tr': 5, # Number of draws to use in approximation for traces
#     'he': True, # If True, compute heteroskedastic correction
#     'out': 'res_fe_wid_firmid.json', # Outputfile where results are saved
#     'statsonly': False, # If True, return only basic statistics
#     'feonly': False, # If True, compute only fixed effects and not variances
#     'Q': 'cov(alpha, psi)', # Which Q matrix to consider. Options include 'cov(alpha, psi)' and 'cov(psi_t, psi_{t+1})'
#     'seed': None # NumPy RandomState seed
# }
    
fe_params = tw.fe_params(
    {
        'he': True
    }
)

for firmvar in ['gamma','firmid_masked']:  #
    print(firmvar)

    data = pd.DataFrame({'i':full.wid_masked,'j':full[firmvar],'y':full.ln_real_hrly_wage_dec,'t':full.year})
    data.dropna(axis=0, inplace=True)
    data = data.loc[(data.i>0) & (data.j>0) ]
    data = bpd.BipartiteLong(data)
    data['i'] = data['i'].astype('int')
    data['j'] = data['j'].astype('int')
    
    
    # For the example, we simulate data
    #sim_data = bpd.SimBipartite().sim_network()
    #display(sim_data)
    

    bdf = bpd.BipartiteDataFrame(data)
    # Clean and collapse
    bdf = bdf.clean(clean_params)

    
    # Initialize FE estimator
    fe_estimator = tw.FEEstimator(bdf, fe_params)
    # Fit FE estimator
    fe_estimator.fit()
    
    
    print(fe_estimator.summary)

        
    
    
    
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



