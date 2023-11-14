


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
    
    # Create a pivot table from the DataFrame
    pivot_table = param.pivot_table(index='job_type', columns='sector_IBGE', values='alpha', aggfunc='mean').fillna(10e-10)
    
    # Convert the pivot table to a tensor
    alphags = torch.tensor(pivot_table.values, dtype=torch.float64)  # Assuming the dtype of 'alpha' is float64
    
    return alphags
