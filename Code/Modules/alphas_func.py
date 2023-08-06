#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 28 17:14:56 2021

@author: jfogel
"""
import pandas as pd
import torch

    

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