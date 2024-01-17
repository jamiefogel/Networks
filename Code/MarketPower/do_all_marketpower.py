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


# Compute mean wages within each jid-iota and reshape to long (p_jgi on Overleaf)
mean_wage_matrix_ji_long = pd.melt(pd.pivot_table(data_full, index=['jid_masked','gamma'], columns=['iota'], values='real_hrly_wage_dec', aggfunc='mean', fill_value=0).reset_index(), id_vars=['jid_masked','gamma'], var_name='iota', value_name='mean_wage_ji')


employment_share_matrix_ji_long = pd.melt( data_full.groupby(['iota'])['jid_masked'].value_counts(normalize=True).unstack(fill_value=0).transpose().reset_index(), id_vars=['jid_masked'], var_name='iota', value_name='employment_share_ji')

del data_full

df_emp_wage_ji = mean_wage_matrix_ji_long.merge(employment_share_matrix_ji_long, on=['jid_masked','iota'], validate='1:1')
df_emp_wage_ji = df_emp_wage_ji.loc[df_emp_wage_ji.employment_share_ji>0]
df_emp_wage_ji['iota'] = df_emp_wage_ji['iota'].astype(str) # Not sure why this is necessary but before I did it there was some sort of null value in iota that was causing problems with to_stata
df_emp_wage_ji.to_stata(root + 'Data/derived/MarketPower/df_emp_wage_ji.dta')       






##############################################################################################################################
# Shouldn't need anything below this either
##############################################################################################################################




employment_share_matrix_gi = data_full.groupby(['iota'])['gamma'].value_counts(normalize=True).unstack(fill_value=0).transpose()

# Compute j probabilities within each iota-gamma
employment_share_matrix_ji = data_full.groupby(['iota'])['jid_masked'].value_counts(normalize=True).unstack(fill_value=0).transpose()
employment_count_matrix_ji = pd.pivot_table(data_full, index=['jid_masked','gamma'], columns=['iota'], values='real_hrly_wage_dec', aggfunc='size', fill_value=0)
employment_count_matrix_gi = pd.pivot_table(data_full, index=['gamma'], columns=['iota'], values='real_hrly_wage_dec', aggfunc='size', fill_value=0)


employment_share_matrix_jgi = pd.DataFrame(index=employment_count_matrix_ji.index).reset_index(level='gamma')
for idx in range(employment_count_matrix_ji.shape[1]):
    iota_idx = idx+1
    print(iota_idx)
    tmp = employment_count_matrix_ji.iloc[:,idx].reset_index(level='gamma').rename(columns={iota_idx:'jid_count'})
    tmp = tmp.merge(employment_count_matrix_gi.iloc[:,idx].reset_index().rename(columns={iota_idx:'gamma_count'}), left_on='gamma',right_on='gamma')
    tmp[iota_idx] = tmp.jid_count/tmp.gamma_count
    employment_share_matrix_jgi = employment_share_matrix_jgi.merge(tmp[iota_idx], left_index=True, right_index=True, how='left')

employment_share_matrix_jgi.fillna(0, inplace=True)





#mean_wage_matrix_ji.to_stata(root + 'Data/derived/MarketPower/mean_wage_matrix_ji.dta')       
#employment_share_matrix_ji.to_stata(root + 'Data/derived/MarketPower/employment_share_matrix_ji.dta')



print('Here')

tmp2 = pd.DataFrame(index=employment_count_matrix_ji.index).reset_index(level='gamma')
tmp2 = tmp2.merge(employment_count_matrix_gi.reset_index(), left_index=True, left_on='gamma', right_on='gamma', validate='m:1')

tmp3 = (employment_share_matrix_jgi.reset_index() * tmp2.reset_index()).drop(columns=['gamma','index','jid_masked'])

(employment_share_matrix_ji.values==tmp3.values).mean()

# These aren't equal
diff = tmp3.values - employment_share_matrix_ji.values
diff.mean()
0.008682353583573122
diff.max()
101685.0




, mean_wage_matrix, jid_gamma_cw = compute_shares_and_mean_wages(data_full, 'jid_masked', 'iota', 'gamma', 'real_hrly_wage_dec')

# Compute gamma shares within each iota (1st term on RHS of Overleaf eq 4)
employment_shares_iota_gamma = compute_shares_and_mean_wages(data_full, 'gamma', 'iota', 'gamma', 'real_hrly_wage_dec')[0]

# What we need to do is apply this function separately for all unique values of iota. Each of these runs we will get a JxG matrix where one column is the j shares of that gamma and the rest of the columns are 0 or missing (not sure which). We then will extract the sole non-0 column for each iota and as we loop over iotas stack these horizontally to get a JxI matrix that we want.
for i in range(0, length(iota)):
    # This will produce a JxG matrix where each element is job j's employment share of the gamma it belongs to. So each row (jid) will have exactly one non-zero element. Therefore, I can collapse the matrix into a column vector by taking the row sums. This will give P(j|gamma,iota=i) for a specific i. The next step will be to horizontally concatenate all I such column vectors to create a JxI matrix.
    employment_shares_jid_gamma, _, _, jid_cw, _ = compute_shares_and_mean_wages(data_full.loc[data_full.iota==i], 'jid_masked', 'gamma', 'gamma', 'real_hrly_wage_dec')
    jids = data_full.loc[data_full.iota==i]['jid']

    
pickle.dump([employment_share_matrix, mean_wage_matrix, jid_gamma_cw], open( root + 'Data/derived/MarketPower/nested_logit.p', 'wb'))


########################################################################################################################################################################
# Old functions using sparse matrices that I think can be deleted
########################################################################################################################################################################


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
    worker_type_codes = df[worker_type].cat.codes.values
    job_id_crosswalk = pd.DataFrame({'line_number':df[job_id].cat.codes.unique(),job_id:df[job_id].unique()})
    worker_type_crosswalk = pd.DataFrame({'line_number':df[worker_type].cat.codes.unique(),worker_type:df[worker_type].unique()})
    wages = df[wage_var].values
    # Create the employment matrix as a sparse matrix
    employment_matrix = coo_matrix((np.ones_like(wages), (jid_codes, worker_type_codes)), shape=(len(df[job_id].cat.categories), len(df[worker_type].cat.categories)))
    employment_matrix = csr_matrix(employment_matrix)  # Convert to CSR format for efficient row-wise operations
    # Create the wage matrix as a sparse matrix
    wage_matrix = coo_matrix((wages, (jid_codes, worker_type_codes)), shape=(len(df[job_id].cat.categories), len(df[worker_type].cat.categories)))
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
    return employment_share_matrix, mean_wage_matrix, job_id_to_type_crosswalk, job_id_crosswalk, worker_type_crosswalk
