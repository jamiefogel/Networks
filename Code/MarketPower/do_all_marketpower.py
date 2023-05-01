#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  1 13:53:05 2023

@author: jfogel
"""


import os
import pandas as pd
import torch
import numpy as np
import pickle
from datetime import datetime
import sys
import matplotlib.pyplot as plt


homedir = os.path.expanduser('~')
root = homedir + '/NetworksGit/'

sys.path.append(root + 'Code/Modules')
figuredir = root + 'Results/'



from torch_mle import torch_mle
from mle_load_fulldata import mle_load_fulldata


mle_data_filename      = root + "Data/RAIS_exports/earnings_panel/panel_rio_2009_2012_w_kmeans.csv"

inv_eta_mayara   = 0.985
inv_theta_mayara = 1.257

data_full = pd.read_csv(mle_data_filename)
data_full = data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1)]

# Mayara defines markets as occ2-micros. Since the current data we have is for Rio only, start with Occ2s. 

# Compute the 


# iota concentration
crosstab_iota_sector = pd.crosstab(index = data_full.iota, columns = data_full.sector_IBGE)
sector_probabilities_by_iota = crosstab_iota_sector.div(crosstab_iota_sector.sum(axis=1),axis=0).reset_index()
sector_probabilities_by_iota['hhi'] = sector_probabilities_by_iota.drop(columns='iota').pow(2).sum(axis=1)



# According to Mayara's equation (9) on page 11, the average markwodn in labor market 

#Felix, p. 27: Appendix C.2.4 shows that the country-level average markdown—that is, the country- level ratio of (employment-weighted) average MRPL to (employment-weighted) average wage—is a weighted average of the market-level markdowns in Proposition 1, where the weights are each market’s payroll share of the country’s total payroll.




'''
mle_data_sums_filename = root + "Data/mle_data_sums/panel_rio_2009_2012_mle_data_sums_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"
mle_estimates_filename = root + "MLE_estimates/panel_rio_2009_2012_mle_estimates_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"

psi_and_k_file         = root + "MLE_estimates/panel_rio_2009_2012_psi_normalized_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + "_eta_" + str(eta) + ".p"

'''


''' Ingredients for computing labor supply elasiticities according to our model:
 - $\Psi_{ig}$
 - 1/theta (our theta corresponds to Mayara's eta, her 1/eta=0.985)
 - 1/nu (our nu corresponds to Mayara's theta, her 1/theta=1.257)
 - s_ig = iota's share of market gamma employment
 - s_jg = job j's share of market gamma employment [Note: in overleaf, we call this s_ijg but I think the i subscript is confusing]
 - s_ij = s_ig*s_ijg = job j's share of type iota employment
'''



# Compute the each group B's share of group A
def a_probs_b(a,b,data, hhi=False):
    crosstab_b_a = pd.crosstab(index = data[b], columns = data[a])
    a_probs_by_b = crosstab_b_a.div(crosstab_b_a.sum(axis=1),axis=0).reset_index()
    if hhi==True:
        a_probs_by_b['hhi'] = a_probs_by_b.drop(columns=b).pow(2).sum(axis=1)
    return a_probs_by_b

s_ij = a_probs_b('iota','jid_masked',   data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1) & (data_full.gamma!=-0)])
s_ig = a_probs_b('iota','gamma',        data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1) & (data_full.gamma!=-0)])
s_jg = a_probs_b('jid_masked','gamma',  data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1) & (data_full.gamma!=-0)])
