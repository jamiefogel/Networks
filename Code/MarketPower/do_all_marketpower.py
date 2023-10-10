#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  1 13:53:05 2023

@author: jfogel
"""


import os
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import sys
import matplotlib.pyplot as plt
import getpass
from scipy.sparse import lil_matrix
from scipy.sparse import coo_matrix
from scipy.sparse import csr_matrix


from IPython.display import display, Math, Latex
display((Math(r'P(Purchase|Male)= \frac{Numero\ total\ de\ compras\ hechas\ por\ hombres\}{Total\ de\ hombres\ en\ el\ grupo\} = \frac{Purchase\cap Male}{Male}')))


homedir = os.path.expanduser('~')
if getpass.getuser()=='p13861161':
    if os.name == 'nt':
        homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
        root = homedir + '/labormkt_rafaelpereira/NetworksGit/'
        print("Running on IPEA Windows")
    else:
        root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
elif getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'

sys.path.append(root + 'Code/Modules')
figuredir = root + 'Results/'




# Pull region codes
region_codes = pd.read_csv(root + '/Data/raw/munic_microregion_rm.csv', encoding='latin1')
muni_micro_cw = pd.DataFrame({'code_micro':region_codes.code_micro,'codemun':region_codes.code_munic//10})


mle_data_filename      = root + "Data/derived/earnings_panel/panel_3states_2013to2016_new_level_0.csv"

inv_eta_mayara   = 0.985
inv_theta_mayara = 1.257


usecols = ['wid_masked', 'jid_masked', 'year', 'iota', 'gamma', 'real_hrly_wage_dec', 'ln_real_hrly_wage_dec', 'codemun', 'occ2', 'occ2_first', 'code_meso', 'occ2Xmeso', 'occ2Xmeso_first']

data_full = pd.read_csv(mle_data_filename, usecols=usecols)
data_full = data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1) & (data_full.jid_masked!=-1)]
data_full = data_full.merge(muni_micro_cw, on='codemun', how='left', validate='m:1', indicator='_merge')

# Mayara defines markets as occ2-micros. Since the current data we have is for Rio only, start with Occ2s. 




# According to Mayara's equation (9) on page 11, the average markdown in labor market 

#Felix, p. 27: Appendix C.2.4 shows that the country-level average markdown—that is, the country- level ratio of (employment-weighted) average MRPL to (employment-weighted) average wage—is a weighted average of the market-level markdowns in Proposition 1, where the weights are each market’s payroll share of the country’s total payroll.




''' Ingredients for computing labor supply elasiticities according to our model:
 - $\Psi_{ig}$
 - 1/theta (our theta corresponds to Mayara's eta, her 1/eta=0.985)
 - 1/nu (our nu corresponds to Mayara's theta, her 1/theta=1.257)
 - s_ig = iota's share of market gamma employment
 - s_jg = job j's share of market gamma employment [Note: in overleaf, we call this s_ijg but I think the i subscript is confusing]
 - s_ij = s_ig*s_ijg = job j's share of type iota employment
'''



eta = 1/.985
theta = 1/1.257


def compute_payroll_weighted_share(df, firm_col, market_col, pay_col):
    # Compute total pay for each firm within each market
    firm_total_pay = df.groupby([market_col, firm_col])[pay_col].sum().reset_index()
    # Compute total pay for each market
    market_total_pay = df.groupby(market_col)[pay_col].sum().reset_index()
    # Merge the two DataFrames on 'market'
    merged = pd.merge(firm_total_pay, market_total_pay, on=market_col, suffixes=('_firm', '_market'))
    # Compute the payroll-weighted share for each firm
    merged['payroll_weighted_share'] = merged[pay_col + '_firm'] / merged[pay_col + '_market']
    return merged[[firm_col, market_col, 'payroll_weighted_share']]

s_ij = compute_payroll_weighted_share(data_full, 'iota', 'jid_masked', 'real_hrly_wage_dec')
s_jg = compute_payroll_weighted_share(data_full, 'jid_masked', 'gamma', 'real_hrly_wage_dec')
s_gi = compute_payroll_weighted_share(data_full, 'gamma', 'iota', 'real_hrly_wage_dec')

# Calculate the squared payroll-weighted share for each firm
s_jg['squared_share'] = s_jg['payroll_weighted_share'] ** 2
# Sum up the squared shares within each market (gamma) to get the HHI
HHI = s_jg.groupby('gamma')['squared_share'].sum().reset_index()
HHI.columns = ['gamma', 'HHI']
markdown = 1 + 1/theta * HHI.HHI + 1/eta *(1-HHI.HHI)



def compute_shares_and_mean_wages(df, job_id, worker_type, job_type, wage_var):
    """
    Efficiently compute job j's share of type iota employment and the mean real wage within the iota-jid cell using only sparse matrices.
    Parameters:
    - df: DataFrame with columns 'iota', 'wid', 'jid', and 'real_wage'.
    Returns:
    - employment_share_matrix: Sparse matrix of job j's share of type iota employment of dimension JxI.
    - mean_wage_matrix: Sparse matrix of mean real wage within the iota-jid cell of dimension JxI.
    """
    # Convert jid and iota columns to categorical
    df[job_id] = df[job_id].astype('category')
    df[worker_type] = df[worker_type].astype('category')
    # Extract the category codes for jid and iota
    jid_codes = df[job_id].cat.codes.values
    iota_codes = df[worker_type].cat.codes.values
    wages = df[wage_var].values
    # Create the employment matrix as a sparse matrix
    employment_matrix = coo_matrix((np.ones_like(wages), (jid_codes, iota_codes)), shape=(len(df[job_id].cat.categories), len(df[worker_type].cat.categories)))
    employment_matrix = csr_matrix(employment_matrix)  # Convert to CSR format for efficient row-wise operations
    # Create the wage matrix as a sparse matrix
    wage_matrix = coo_matrix((wages, (jid_codes, iota_codes)), shape=(len(df[job_id].cat.categories), len(df[worker_type].cat.categories)))
    wage_matrix = csr_matrix(wage_matrix)  # Convert to CSR format for efficient row-wise operations
    # Convert employment counts to shares
    total_counts = employment_matrix.sum(axis=0)
    inverse_total_counts = 1 / total_counts.A  # Convert matrix to array for elementwise division
    employment_share_matrix = csr_matrix(employment_matrix.multiply(inverse_total_counts))
    # Convert total wages to mean wages
    # Convert B to COO format
    employment_matrix_coo = employment_matrix.tocoo()
    # Invert each non-zero element of B
    employment_matrix_coo.data = 1.0 / employment_matrix_coo.data
    # Convert back to CSR for efficient element-wise multiplication
    employment_matrix_inv_csr = employment_matrix_coo.tocsr()
    mean_wage_matrix = wage_matrix.multiply(employment_matrix_inv_csr)
    # Create a crosswalk between the job IDs and their job types, ensuring consistent order
    sorted_df = df.sort_values(by=job_id, key=lambda col: col.cat.codes)
    job_id_to_type_crosswalk = sorted_df.drop_duplicates(subset=[job_id])[[job_id,job_type]]
    return employment_share_matrix, mean_wage_matrix, job_id_to_type_crosswalk

employment_share_matrix, mean_wage_matrix, jid_gamma_cw = compute_shares_and_mean_wages(data_full, 'jid_masked', 'iota', 'gamma', 'real_hrly_wage_dec')

pickle.dump([employment_share_matrix, mean_wage_matrix, jid_gamma_cw], open( root + 'Data/derived/MarketPower/nested_logit.p', 'wb'))
