#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on August 13 2024
- Adapted from NetworksGit/Code/MarketPower/do_all_marketpower.py
- Goalis to delete unnecessary code to focus on simply computing distributions of HHIs and markdowns across different market definitions

@author: jfogel
"""


import os
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import getpass
from scipy.sparse import lil_matrix
from scipy.sparse import coo_matrix
from scipy.sparse import csr_matrix
import statsmodels.formula.api as smf
import statsmodels.formula.api as smf
import statsmodels.api as sm
from linearmodels.panel import PanelOLS
from linearmodels.panel import compare
import time
import subprocess

homedir = os.path.expanduser('~')
if getpass.getuser()=='p13861161':
    if os.name == 'nt':
        homedir = os.path.expanduser('//storage6/usuarios')  # for running this code on windows
        root = homedir + '/labormkt_rafaelpereira/NetworksGit/'
        print("Running on IPEA Windows")
        sys.path.append(r'C:\ProgramData\anaconda3\Lib\site-packages\src')
        #import pystata
        #import stata_setup
        #stata_sysdir = 'C:\Program Files (x86)\Stata14'
        #stata_setup.config(stata_sysdir, 'mp')
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

# The 2013 to 2016 panel doesn't keep cnpj_raiz, which we need to compute HHIs following Mayara. I could probably re-run the create_earnings_panel to get it, but don't want to deal with that now 
#mle_data_filename      = root + "Data/derived/earnings_panel/panel_3states_2013to2016_new_level_0.csv"
mle_data_filename      = root + "Data/derived/earnings_panel/panel_3states_2009to2011_level_0.csv"


usecols = ['wid_masked', 'jid_masked', 'year', 'iota', 'gamma', 'cnpj_raiz','id_estab', 'real_hrly_wage_dec', 'ln_real_hrly_wage_dec', 'codemun', 'occ2', 'occ2_first', 'code_meso', 'occ2Xmeso', 'occ2Xmeso_first']

data_full = pd.read_csv(mle_data_filename, usecols=usecols)
data_full = data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1) & (data_full.jid_masked!=-1)]
data_full = data_full.merge(muni_micro_cw, on='codemun', how='left', validate='m:1', indicator='_merge')

# Mayara defines markets as occ2-micros. 

# According to Mayara's equation (9) on page 11, the average markdown in labor market 

#Felix, p. 27: Appendix C.2.4 shows that the country-level average markdown—that is, the country- level ratio of (employment-weighted) average MRPL to (employment-weighted) average wage—is a weighted average of the market-level markdowns in Proposition 1, where the weights are each market’s payroll share of the country’s total payroll.




''' Ingredients for computing labor supply elasiticities according to our model:
 - $\Phi_{ig}$
 - 1/theta (our theta corresponds to Mayara's eta, her 1/eta=0.985)
 - 1/nu (our nu corresponds to Mayara's theta, her 1/theta=1.257)
 - s_ig = iota's share of market gamma employment
 - s_jg = job j's share of market gamma employment [Note: in overleaf, we call this s_ijg but I think the i subscript is confusing]
 - s_ij = s_ig*s_ijg = job j's share of type iota employment
'''


inv_eta_mayara   = 0.985
inv_theta_mayara = 1.257

eta_mayara = 1/inv_eta_mayara
theta_mayara = 1/inv_theta_mayara

eta_bhm = 7.14
theta_bhm = .45


# Create variable for Mayara's market definitions
data_full['mkt_mayara'] = data_full.groupby(['occ2', 'code_micro']).ngroup()


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


def compute_payroll_weighted_share(df, firm_col, market_col, pay_col):
    '''
    # I don't think we actually need this to be balanced
    # Get all unique firms and markets
    all_firms = df[firm_col].unique()
    all_markets = df[market_col].unique()
    # Create a DataFrame with all possible firm-market combinations
    all_combinations = pd.DataFrame([(firm, market) for firm in all_firms for market in all_markets], columns=[firm_col, market_col])
    '''
    # Compute total pay for each firm within each market
    firm_total_pay = df.groupby([market_col, firm_col])[pay_col].sum().reset_index()
    # Compute total pay for each market
    market_total_pay = df.groupby(market_col)[pay_col].sum().reset_index()

    '''
    # Merge the all_combinations with firm_total_pay
    merged = pd.merge(all_combinations, firm_total_pay, on=[market_col, firm_col], how='left')
    # Fill NaN values with 0 for firms that don't exist in certain markets
    merged[pay_col] = merged[pay_col].fillna(0)
    # Merge with market_total_pay
    merged = pd.merge(merged, market_total_pay, on=market_col, suffixes=('_firm', '_market'))
    # Compute the payroll-weighted share for each firm
    merged['payroll_weighted_share'] = merged[pay_col + '_firm'] / merged[pay_col + '_market']
    # Replace NaN values with 0 (this handles cases where market total pay is 0)
    '''
    merged = firm_total_pay.merge(market_total_pay, on=market_col, suffixes=('_firm', '_market'), how='left', validate='m:1')
    merged['payroll_weighted_share'] = merged[pay_col + '_firm'] / merged[pay_col + '_market']

    return merged[[firm_col, market_col, 'payroll_weighted_share']]



s_ij = compute_payroll_weighted_share(data_full, 'iota', 'jid_masked', 'real_hrly_wage_dec')
s_jg = compute_payroll_weighted_share(data_full, 'jid_masked', 'gamma', 'real_hrly_wage_dec')
s_fm = compute_payroll_weighted_share(data_full, 'cnpj_raiz', 'mkt_mayara', 'real_hrly_wage_dec')
s_gi = compute_payroll_weighted_share(data_full, 'gamma', 'iota', 'real_hrly_wage_dec')

# This is the quantity Ben and Bernardo derived on the white board on 8/14
# - Numerator: for each iota compute the total (hourly) earnings for that iota in job j. Raise this to (1+eta). Then sum these quantities over all jobs j in market gamma and raise this quantity to the (1+theta)/(1+eta).
# Denominator: Compute the numerator for each market gamma and sum over all markets gamma
# - The result will be one value for each iota, all of which sum to 1.

def compute_s_gammaiota(data_full, eta, theta):
    '''
    # I don't think we actually need this to be balanced
    # Get all unique firms and markets
    all_iotas  = data_full['iota'].unique()
    all_gammas = data_full['gamma'].unique()
    # Create a DataFrame with all possible firm-market combinations
    all_combinations = pd.DataFrame([(iota, gamma) for iota in all_iotas for gamma in all_gammas], columns=['iota', 'gamma'])
    '''
    # Group by iota and job, and sum hourly earnings
    job_earnings = data_full.groupby(['iota', 'jid_masked','gamma'])['real_hrly_wage_dec'].sum().reset_index()
    # Compute the (1+eta) power of earnings
    job_earnings['earnings_powered'] = job_earnings['real_hrly_wage_dec'] ** (1 + eta)
    # Group by iota and market (gamma), and sum the powered earnings
    market_earnings = job_earnings.groupby(['iota', 'gamma'])['earnings_powered'].sum().reset_index()
    # Compute the (1+theta)/(1+eta) power of the sum
    market_earnings['market_sum_powered'] = market_earnings['earnings_powered'] ** ((1 + theta) / (1 + eta))
    numerator = market_earnings[['iota', 'gamma', 'market_sum_powered']]
    # Compute the denominator: sum of numerators over all gammas within each iota
    denominator = numerator.groupby('iota')['market_sum_powered'].sum().reset_index()

    '''
    # Merge the all_combinations with numerator on iota and gamma 
    merged = pd.merge(all_combinations, numerator, on=['iota', 'gamma'], how='left')
    merged['market_sum_powered'] = merged['market_sum_powered'].fillna(0)
    # Merge with denominator on iota
    merged = pd.merge(merged, denominator, on='iota', suffixes=('_gi', '_i'))
    # Compute the payroll-weighted share for each gamma-iota
    merged['s_gammaiota'] = merged['market_sum_powered_gi'] / merged['market_sum_powered_i']
    '''
    merged = pd.merge(numerator, denominator, on='iota', suffixes=('_gi', '_i'))
    # Compute the payroll-weighted share for each gamma-iota
    merged['s_gammaiota'] = merged['market_sum_powered_gi'] / merged['market_sum_powered_i']
    return merged[['iota', 'gamma', 's_gammaiota']]


# Just confirming distributions look reasonable
s_gi_hat = compute_s_gammaiota(data_full, eta_bhm, theta_bhm)
print(s_gi_hat['s_gammaiota'].sum())
print(s_gi_hat['s_gammaiota'].sort_values().tail(446).describe())
test = compute_s_gammaiota(data_full, 0, 0)
print(test['s_gammaiota'].sum())
print(test['s_gammaiota'].sort_values().tail(446).describe())

# Delete empty rows to save memory
s_ij = s_ij.loc[s_ij.payroll_weighted_share>0]


jid_gamma_cw = data_full[['jid_masked', 'gamma']].drop_duplicates()
# Note that s_ij corresponds to pi_{j \iota} on OVerleaf. Need to clean up notation.
product = s_ij.merge(jid_gamma_cw, on='jid_masked', how='left', validate='m:1', indicator='_merge1')
product = product.merge(s_gi_hat, on=['iota','gamma'], how='left', validate='m:1', indicator='_merge2')
product['product'] = product['s_gammaiota'] * product['payroll_weighted_share']
sum_product = product.groupby('jid_masked')['product'].sum()

# This is critical to make sure the columns being summed have the same index
s_jg = s_jg.set_index('jid_masked', verify_integrity=True)

epsilon_j_bhm = eta_bhm * (1 - s_jg['payroll_weighted_share']) + theta_bhm * s_jg['payroll_weighted_share'] * (1 - sum_product)


# Display distribution of elasticities 
print(epsilon_j_bhm.describe())

markdown_w_iota = epsilon_j_bhm / (1 + epsilon_j_bhm)

''' 
# This is an old version that should probably be deleted

# Back out the Phi_ig implied by wages and elasticities (given choices of eta and theta)

temp = data_full[['iota','gamma','wid_masked','jid_masked','real_hrly_wage_dec']].merge(epsilon_j_bhm.reset_index(name='epsilon_j_bhm'), on='jid_masked', how='outer', validate='m:1', indicator=True)
temp['phi_ij'] = temp['real_hrly_wage_dec'] * (1 + temp['epsilon_j_bhm']**(-1))
# Need to collapse to the iota-gamma level rather than iota-jid level. 

phi_ig_hat = temp.groupby(['iota','gamma'])['phi_ij'].mean().reset_index(name='phi_ig_hat')
del temp 

# Calculate w_j by dividing hourly log earnings by phi for each individual and then taking the mean within each job. This follows from equation (36) on Overleaf. But not entirely sure this is the right approach. In particular some values of w_j are > 1, which is inconsistent with w_j being a pure markdown. 

temp = data_full[['iota','gamma','jid_masked','real_hrly_wage_dec']].merge(phi_ig_hat, on=['iota','gamma'], how='outer', validate='m:1', indicator=True)
temp['w_j'] = temp['real_hrly_wage_dec'] / temp['phi_ig_hat']

w_j = temp.groupby('jid_masked')['w_j'].mean().reset_index(name='w_j')

print(w_j.w_j.describe(percentiles=[.01, .05, .1, .25, .5, .75, .9, .95, .99]))
'''

data_full = data_full.merge(pd.DataFrame(markdown_w_iota).reset_index().rename(columns={0:'markdown_w_iota'}), on='jid_masked', how='outer',validate='m:1', indicator='_m_md')

data_full._m_md.value_counts()
data_full.drop(columns='_m_md', inplace=True)

data_full.markdown_w_iota.describe()

data_full['y_tilde'] = data_full.ln_real_hrly_wage_dec + np.log(data_full.markdown_w_iota)

reg_df = data_full[['iota','gamma','wid_masked','jid_masked','y_tilde','ln_real_hrly_wage_dec','markdown_w_iota']]

reg_df['iota_gamma_id'] = reg_df.groupby(['iota', 'gamma']).ngroup()


reg_df['iota_gamma'] = reg_df.iota.astype(int).astype(str) + '_' + reg_df.gamma.astype(int).astype(str)




# Save dataframe as .dta file
dta_path        = root + 'Data/derived/MarketPower_reghdfe_data.dta'
results_path    = root + 'Data/derived/MarketPower_reghdfe_results.dta'
reg_df.to_stata(dta_path, write_index=False)

### This works!
# Paths
stata_path = r"C:\Program Files (x86)\Stata14\StataMP-64.exe"  # Adjust this path
do_file_path = root + r"Code\MarketPower\run_twoway_fes.do"  # Adjust this path
log_file_path = root + r"Code\MarketPower\run_twoway_fes.log"

def write_stata_code(code, file_path):
    with open(file_path, 'w') as f:
        f.write(code)

def run_stata_with_realtime_log(stata_code, do_file_path, log_file_path, stata_path=r"C:\Program Files (x86)\Stata14\StataMP-64.exe"):
   

    # Write Stata code to .do file
    write_stata_code(stata_code, do_file_path)

    # Run Stata do-file
    process = subprocess.Popen([stata_path, "/e", "do", do_file_path], 
                               stdout=subprocess.PIPE, 
                               stderr=subprocess.STDOUT,
                               universal_newlines=True)

    # Initialize last_position
    last_position = 0

    while True:
        # Check if process has finished
        if process.poll() is not None:
            break

        # Check if log file exists
        if os.path.exists(log_file_path):
            with open(log_file_path, 'r') as log_file:
                # Move to last read position
                log_file.seek(last_position)
                
                # Read new content
                new_content = log_file.read()
                
                if new_content:
                    print(new_content, end='')
                    
                # Update last_position
                last_position = log_file.tell()

        # Wait a bit before checking again
        time.sleep(0.1)

    # Read any remaining content after process finishes
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as log_file:
            log_file.seek(last_position)
            remaining_content = log_file.read()
            if remaining_content:
                print(remaining_content, end='')

    # Check process return code
    if process.returncode != 0:
        print(f"Stata process exited with return code {process.returncode}")

# Your Stata code as a Python string
stata_code = f"""
clear
set more off
log using "{log_file_path}", replace
use "{dta_path}", clear
reghdfe y_tilde, absorb(jid_masked_fes=jid_masked iota_gamma_fes=iota_gamma_id, savefe) residuals(resid)

save "{results_path}", replace
"""

# Run the function with your Stata code
run_stata_with_realtime_log(stata_code, do_file_path, log_file_path)

results = pd.read_stata(results_path)


# Quickly check the variance decomposition
results.jid_masked_fes.var() / results.y_tilde.var()
#Out[35]: 0.846037664821534
results.iota_gamma_fes.var() / results.y_tilde.var()
#Out[36]: 0.0067746838565168055
results.resid.var() / results.y_tilde.var()
#Out[37]: 0.14825373239440456



collapsed_df = results.groupby(['iota', 'gamma', 'iota_gamma_id', 'jid_masked']).agg({
    'jid_masked_fes': 'first',          # These don't vary within the group, so we can take the first value
    'iota_gamma_fes': 'first',          # These don't vary within the group, so we can take the first value
    'markdown_w_iota': 'first',         # These don't vary within the group, so we can take the first value
    'ln_real_hrly_wage_dec': 'mean',    # Average of log earnings within the group
    'wid_masked': 'count'               # Count of rows in this group
}).reset_index()

# Rename the count column to something more descriptive
collapsed_df = collapsed_df.rename(columns={'wid_masked': 'iota_gamma_jid_count'})

# Display the first few rows of the collapsed dataframe
print(collapsed_df.head())

# Display information about the collapsed dataframe
print(collapsed_df.info())



# Generate a random shock following eq (44). The jid and iota-gamma shocks collectively form Z_j
jid_shock = {jid_masked: np.random.random() for jid_masked in collapsed_df['jid_masked'].unique()} # This is phi_j
collapsed_df['jid_masked_shock'] = collapsed_df['jid_masked'].map(jid_shock)

iota_gamma_shock = {iota_gamma_id: np.random.random() for iota_gamma_id in collapsed_df['iota_gamma_id'].unique()}
collapsed_df['iota_gamma_shock'] = collapsed_df['iota_gamma_id'].map(iota_gamma_shock)


collapsed_df['phi_iota_j_new'] = np.exp( \
    collapsed_df['iota_gamma_fes']   + collapsed_df['jid_masked_fes'] + \
    collapsed_df['iota_gamma_shock'] + collapsed_df['jid_masked_shock']) 

collapsed_df['wage_guess'] = collapsed_df['markdown_w_iota'] * collapsed_df['phi_iota_j_new']

collapsed_df['iota_count'] = collapsed_df.groupby('iota')['iota_gamma_jid_count'].transform('sum')


# Next steps:
#   1. Compute ell_iota_j following eq (45)
#   2. Then iterate through 45-50
#   3. Iterate until ell and w stabilize


collapsed_df['real_hrly_wage_dec'] = np.exp(collapsed_df['ln_real_hrly_wage_dec'])

# Restricting to jid-iotas with non-missing wages. We have 78862 missing values, all of which correspond to missing FEs and are iota_gamma_jid singletons.  
collapsed_df = collapsed_df.loc[collapsed_df['wage_guess'].notna()]

collapsed_df.to_pickle(root + 'Data/derived/tmp_collapsed_df.p')


# Equation 45 and 47 (because the term in 47 is part of 45)
def compute_ell(df, eta, theta):
    df['w_power'] = df['wage_guess'] ** (1 + eta)
    df['numerator'] = df.groupby(['iota','gamma'])['w_power'].transform('sum') ** ((1 + theta) / (1 + eta))
    df['denominator'] = df.groupby('iota')['numerator'].transform('sum') 
    df['s_gamma_iota'] =  df['numerator'] / df['denominator']
    df['second_term'] = df['w_power'] / df.groupby(['iota','gamma'])['w_power'].transform('sum') 
    df['ell_iota_j'] =  df['iota_count'] * df['s_gamma_iota'] * df['second_term']
    return df[['s_gamma_iota','ell_iota_j']]

# Equation 46
def compute_pi(df):
    df['denominator'] = df.groupby(['jid_masked'])['ell_iota_j'].transform('sum')
    return df['ell_iota_j'] / df['denominator']

# Equation 48 - Job j's payrolls share of market gamma (summing across all iotas)
def compute_s_j_gamma(df):
    df['wl'] = df['wage_guess'] * df['ell_iota_j']
    df['numerator']   = df.groupby('jid_masked')['wl'].transform('sum')
    df['denominator'] = df.groupby('gamma')['wl'].transform('sum')
    return df['numerator'] / df['denominator']
    

# Equation 49: compute markdown
def compute_epsilon_j(df, eta, theta):
    df['pi_times_s'] = df['pi_iota_j'] * df['s_gamma_iota']
    df['weighted_share'] = df.groupby('jid_masked')['pi_times_s'].transform('sum')
    df['epsilon_j'] = eta *(1-df['s_j_gamma']) + theta * df['s_j_gamma'] * (1 - df['weighted_share'])
    return df['epsilon_j'] 




# Now we need to do some sort of iterate until convergence
diff = 1
tol = .0001
max_iter = 100
iter = 0
while (diff > tol) and (iter < max_iter):
    
    collapsed_df[['s_gamma_iota','ell_iota_j']] = compute_ell(collapsed_df, eta_bhm, theta_bhm)
    collapsed_df['pi_iota_j'] = compute_pi(collapsed_df)
    collapsed_df['s_j_gamma'] = compute_s_j_gamma(collapsed_df)
    collapsed_df['epsilon_j'] = compute_epsilon_j(collapsed_df, eta_bhm, theta_bhm)
    
    # Update wage_guess
    collapsed_df['wage_guess_new'] = collapsed_df['epsilon_j']/(1+collapsed_df['epsilon_j']) * collapsed_df['phi_iota_j_new']
    diff = np.abs(collapsed_df['wage_guess_new'] - collapsed_df['wage_guess']).sum()
    #diff_l = np.abs(collapsed_df['wage_guess_new'] - collapsed_df['wage_guess_new']).sum()
    collapsed_df['wage_guess'] = collapsed_df['wage_guess_new']
    if True: #iter%10==0:
        print(iter)
        print(diff)
    iter += 1

## This is exploding. We need to figure out why

'''
This is all old
collapsed_df['s_gamma'] = compute_s_gamma(collapsed_df)

# Equation 43
collapsed_df['s_j_gamma'] = collapsed_df['real_hrly_wage_dec'] * collapsed_df['ell'] / \
                            collapsed_df.groupby('gamma').apply(lambda x: (x['real_hrly_wage_dec'] * x['ell']).sum())

# Equation 44
def compute_epsilon(df):
    pi_s_gamma = (df['pi'] * df['s_gamma']).groupby('gamma').transform('sum')
    return eta * (1 - df['s_j_gamma']) + theta * df['s_j_gamma'] * (1 - pi_s_gamma)

collapsed_df['epsilon'] = compute_epsilon(collapsed_df)

# Equation 45
collapsed_df['w_updated'] = collapsed_df['real_hrly_wage_dec'] * \
                            (collapsed_df['epsilon'] / (1 + collapsed_df['epsilon']))

# Print the results
print(collapsed_df[['iota', 'gamma', 'jid_masked', 'ell', 'pi', 's_gamma', 's_j_gamma', 'epsilon', 'w_updated']])
'''


''' Sample code from run_mayara_regressions.py
    X = pd.get_dummies(df_year[['age_cat','educ_cat','female']], drop_first=True)
    
    # Specify the dependent variable and independent variables
    y = df_year[['ln_rem_dez_sm']]
    
    # Fit the model using PanelOLS
    model = PanelOLS(y, X, entity_effects=True).fit()
    firm_market_year_fes = model.estimated_effects.copy().reset_index()
    firm_market_year_fes.drop_duplicates(subset=['firm_market_fe_year','year'],keep='last', inplace=True) # Previously I tried dropping duplicates based on all variables but because of precision issues some values of estimated_effects vary slightly within a firm_market_fe_year. The amount of variation is trivial, but technically non-zero, so it was preventing certain rows that are essentially duplicates from being dropped. 

results = model.fit()

# XX Should we be estimating firm-market-year FEs using all years in 1986-2000? See page 35 of Mayara's appendix.
dict_firm_market_year_fes = {}
for year in [fyear,lyear]:    
    df_year = df.loc[pd.IndexSlice[:,year],:]
    # Convert categorical variables to dummy variables
    X = pd.get_dummies(df_year[['age_cat','educ_cat','female']], drop_first=True)
    
    # Specify the dependent variable and independent variables
    y = df_year[['ln_rem_dez_sm']]
    
    # Fit the model using PanelOLS
    model = PanelOLS(y, X, entity_effects=True).fit()
    firm_market_year_fes = model.estimated_effects.copy().reset_index()
    firm_market_year_fes.drop_duplicates(subset=['firm_market_fe_year','year'],keep='last', inplace=True) # Previously I tried dropping duplicates based on all variables but because of precision issues some values of estimated_effects vary slightly within a firm_market_fe_year. The amount of variation is trivial, but technically non-zero, so it was preventing certain rows that are essentially duplicates from being dropped. 
    # Create the new 'firm_market_fe' column by removing the year suffix
    firm_market_year_fes['firm_market_fe'] = firm_market_year_fes['firm_market_fe_year'].str.replace(r':\d{4}$', '', regex=True)
    firm_market_year_fes.drop(columns='firm_market_fe_year', inplace=True)
    # Reshape to wide format on 'year'
    firm_market_year_fes.reset_index(drop=True, inplace=True)
    dict_firm_market_year_fes[year] = firm_market_year_fes.rename(columns={'estimated_effects':f'w_zm{year}'}).drop(columns='year')

# Print the summary of the regression
print(model.summary)

'''










######################################################################

# Calculate the squared payroll-weighted share for each firm
s_jg['squared_share'] = s_jg['payroll_weighted_share'] ** 2
s_fm['squared_share'] = s_fm['payroll_weighted_share'] ** 2


# Sum up the squared shares within each market (gamma) to get the HHI
HHI = s_jg.groupby('gamma')['squared_share'].sum().reset_index()
HHI.columns = ['gamma', 'HHI']
HHI['count'] = data_full.groupby(['gamma']).gamma.count()

HHI_mayara = s_fm.groupby('mkt_mayara')['squared_share'].sum().reset_index()
HHI_mayara.columns = ['mkt_mayara', 'HHI']
HHI_mayara['count'] = data_full.groupby(['mkt_mayara']).mkt_mayara.count()




def plot_markdowns_multiple(input_tuples):
    plt.figure(figsize=(12, 6))
    
    for i, (HHI, eta, theta, weights, label) in enumerate(input_tuples):

        markdown = 1 + 1/theta * HHI + 1/eta * (1-HHI)
        
        print(f"Markdown statistics for input {i+1}:")
        print(markdown.describe())
        print("\n")
        
        color = plt.cm.rainbow(i / len(input_tuples))
        
        if weights is None:
            sns.histplot(markdown, kde=True, color=color, alpha=0.5, label=label)
        else:
            sns.histplot(x=markdown, weights=weights, kde=True, color=color, alpha=0.5, label=label)
    
    plt.title('Markdowns Histogram')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.legend()
    
    plt.show()
    plt.savefig('markdowns_histogram_multiple.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return markdown

# Example usage:
# Assuming HHI, HHI_mayara, eta, and theta are defined

input_tuples1 = [
    (HHI.HHI, 1.015, 0.796, HHI['count'], 'Our HHI; Mayara parameters'),
    (HHI_mayara.HHI, 1.015, 0.796, HHI_mayara['count'], 'Mayara HHI; Mayara parameters'),
    # Add more tuples as needed
]

plot_markdowns_multiple(input_tuples1)


input_tuples2 = [
    (HHI.HHI, 7.14, .45, HHI['count'], 'Our HHI; BHM parameters'),
    (HHI.HHI, 1.015, 0.796, HHI['count'], 'Our HHI; Mayara parameters'),
    # Add more tuples as needed
]

markdowns = plot_markdowns_multiple(input_tuples2)


input_tuples3 = [
    (HHI.HHI,        7.14, .45, HHI['count'], 'Our HHI; BHM parameters'),
    (HHI_mayara.HHI, 7.14, .45, HHI_mayara['count'], 'Mayara HHI; BHM parameters'),
    # Add more tuples as needed
]
markdowns = plot_markdowns_multiple(input_tuples3)



# Unweighted
if 1==1:
    plt.figure(figsize=(12, 6))
    
    # Plot histogram for 'markdown'
    sns.histplot(HHI.HHI, kde=True, color='blue', alpha=0.5, label='HHI (gamma)')
    
    # Overlay histogram for 'markdown_mayara'
    sns.histplot(HHI_mayara.HHI, kde=True, color='red', alpha=0.5, label='HHI (Mayara)')
    
    plt.title('Histogram Comparison')
    plt.xlabel('Values')
    plt.ylabel('Frequency')
    plt.legend()
    
    # Save the figure
    plt.show()
    plt.savefig('histogram_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()


# Weighted
if 1==1:
    plt.figure(figsize=(12, 6))
    
    # Plot weighted histogram for HHI
    sns.histplot(x=HHI.HHI, weights=HHI['count'], kde=True, color='blue', alpha=0.5, label='HHI')
    
    # Overlay weighted histogram for HHI_mayara
    sns.histplot(x=HHI_mayara.HHI, weights=HHI_mayara['count'], kde=True, color='red', alpha=0.5, label='HHI Mayara')
    
    plt.title('Weighted Histogram Comparison of HHI')
    plt.xlabel('HHI Values')
    plt.ylabel('Weighted Frequency')
    plt.legend()
    
    # Optionally, set x-axis limits if you want to focus on a specific range
    # plt.xlim(0, 10000)  # Adjust these values as needed
    
    # Save the figure
    plt.show()
    plt.savefig('weighted_hhi_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()





#################################
# Simulations to show the effect of misclassification on estimates of eta and theta
# - Use 2009-2011 to estimate Phi
# - Use BHM's estimates of eta and theta:





