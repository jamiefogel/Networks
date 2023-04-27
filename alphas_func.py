#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 17:14:56 2021

@author: jfogel
"""
import pandas as pd
import torch


def compute_alphas(earnings_file, job_type_var, earnings_var, alphas_file=None):
    
    # These 5 lines taken from mle_load_fulldata_v2.py
    data_full = pd.read_csv(earnings_file)
    data_full['job_type']    = data_full[job_type_var]
    data_full.loc[data_full['gamma']==0, 'job_type'] = 0 #Make sure the job type is set to 0 for non-employed
    data_full = data_full[(data_full['gamma']!=-1) & (data_full['iota']!=-1)]  # JSF: I think we want to keep gamma and iota hard-coded here because we're going to want to keep using this restriction for sample definition, but subsequently we will allow the user to specify worker and job type vars.
    	
    gs_sum = data_full.groupby(['job_type','sector_IBGE'])[earnings_var].sum().to_frame().reset_index().rename(columns={earnings_var:"gs"})
    s_sum = data_full.groupby(['sector_IBGE'])[earnings_var].sum().to_frame().reset_index().rename(columns={earnings_var:"s"})
    temp = gs_sum.merge(s_sum, on='sector_IBGE')
    temp['alpha'] = temp['gs']/temp['s']
    alphas = temp[['job_type','sector_IBGE','alpha']]
    
    if alphas_file is not None:
        alphas.to_pickle(alphas_file)
        
        
    

################################################################################
# Load alphas  
################################################################################

def load_alphas(alphas_file):
    param = pd.read_pickle(alphas_file)
    
    S = int(param['sector_IBGE'].max())
    G = int(param['job_type'].max())
    
    alphags = torch.zeros(G,S)
    
    alpha_missing = 10e-10
    
    for g in range(1, G+1):
        for s in range(1,S+1):
            if sum((param['job_type'] == g) & (param['sector_IBGE'] == s)) == 0:
                alphags[g-1,s-1] = alpha_missing
            else:
                alphags[g-1,s-1] = torch.tensor(param.loc[(param['job_type'] == g) & (param['sector_IBGE'] == s),'alpha'].values)
    
    return alphags