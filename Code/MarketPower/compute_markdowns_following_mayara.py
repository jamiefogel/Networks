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
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import getpass
import time
import subprocess
import tempfile
import ast

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


inv_eta_mayara   = 0.985
inv_theta_mayara = 1.257

eta_mayara = 1/inv_eta_mayara
theta_mayara = 1/inv_theta_mayara

eta_bhm = 7.14
theta_bhm = .45

np.random.seed(734)

###################################
# Define functions to be used below

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

def compute_s_gammaiota(data_full, eta, theta):
    '''
    # This is the quantity Ben and Bernardo derived on the white board on 8/14
    # - Numerator: for each iota compute the total (hourly) earnings for that iota in job j. Raise this to (1+eta). Then sum these quantities over all jobs j in market gamma and raise this quantity to the (1+theta)/(1+eta).
    # Denominator: Compute the numerator for each market gamma and sum over all markets gamma
    # - The result will be one value for each iota, all of which sum to 1.

    
    
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


def run_stata_code(reg_df, stata_code, dta_path=None, results_path=None, do_file_path=None, log_file_path=None, scalar_results_path=None, stata_path=None):
    # Create temporary directory if needed
    temp_dir = tempfile.mkdtemp() if any(path is None for path in [dta_path, results_path, do_file_path, log_file_path, scalar_results_path]) else None

    # Track which files are temporary
    temp_files = []

    # Use provided paths or create temporary ones
    if dta_path is None:
        dta_path = os.path.join(temp_dir, 'input_data.dta')
        temp_files.append(dta_path)
    if results_path is None:
        results_path = os.path.join(temp_dir, 'results.dta')
        temp_files.append(results_path)
    if do_file_path is None:
        do_file_path = os.path.join(temp_dir, 'stata_code.do')
        temp_files.append(do_file_path)
    if log_file_path is None:
        log_file_path = os.path.join(temp_dir, 'stata_log.log')
        temp_files.append(log_file_path)
    if scalar_results_path is None:
        scalar_results_path = os.path.join(temp_dir, 'scalar_results.txt')
        temp_files.append(scalar_results_path)


    # Read results
    print(temp_dir)
    if os.path.exists(temp_dir):
        print(f"The path {temp_dir} exists.")
    else:
        print(f"The path {temp_dir} does not exist.")

    try:
        # Save dataframe as .dta file
        reg_df.to_stata(dta_path, write_index=False)

        # Modify Stata code to include scalar results
        stata_code += f"""
        file open scalarfile using "{scalar_results_path}", write replace
        file write scalarfile "scalar_results = {{"
        local scalar_count: word count `scalars'
        forvalues i = 1/`scalar_count' {{
            local r: word `i' of `scalars'
            file write scalarfile "'`r'': " (`r')
            if `i' < `scalar_count' {{
                file write scalarfile ", "
            }}
        }}
        file write scalarfile "}}"
        file close scalarfile
        
        """

        # Replace placeholders in stata_code
        stata_code = stata_code.replace("dta_path", dta_path)
        stata_code = stata_code.replace("results_path", results_path)
        stata_code = stata_code.replace("log_file_path", log_file_path)

        print(stata_code)

        # Write Stata code to .do file
        with open(do_file_path, 'w') as f:
            f.write(stata_code)

        # Run Stata
        stata_path = stata_path or r"C:\Program Files (x86)\Stata14\StataMP-64.exe"  # Adjust this path if needed
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

        results_df = pd.read_stata(results_path)

        # Read scalar results
        with open(scalar_results_path, 'r') as f:
            scalar_results_str = f.read().strip()
        
        # Extract the dictionary part from the string
        scalar_dict_str = scalar_results_str.split('=', 1)[1].strip()
        
        # Use ast.literal_eval to safely evaluate the string
        scalar_results = ast.literal_eval(scalar_dict_str)

        # Read do file
        with open(do_file_path, 'r') as f:
            do_file_content = f.read()

        # Read log file
        with open(log_file_path, 'r') as f:
            log_file_content = f.read()

        # Return dictionary of results
        return {
            'results_df': results_df,
            'scalar_results': scalar_results,
            'do_file': do_file_content,
            'log_file': log_file_content
        }

    finally:
        # Clean up only temporary files
        for file in temp_files:
            if os.path.exists(file):
                os.remove(file)
        if temp_dir:
            os.rmdir(temp_dir)




###############################################################################
# Process data
###############################################################################


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
#Felix, p. 27: Appendix C.2.4 shows that the country-level average markdown—that is, the country- level ratio of (employment-weighted) average MRPL to (employment-weighted) average wage—is a weighted average of the market-level markdowns in Proposition 1, where the weights are each market’s payroll share of the country’s total payroll.


''' Ingredients for computing labor supply elasticities according to our model:
 - $\Phi_{ig}$
 - 1/theta (our theta corresponds to Mayara's eta, her 1/eta=0.985)
 - 1/nu (our nu corresponds to Mayara's theta, her 1/theta=1.257)
 - s_ig = iota's share of market gamma employment
 - s_jg = job j's share of market gamma employment [Note: in overleaf, we call this s_ijg but I think the i subscript is confusing]
 - s_ij = s_ig*s_ijg = job j's share of type iota employment
'''

# Create variable for Mayara's market definitions
data_full['mkt_mayara'] = data_full.groupby(['occ2', 'code_micro']).ngroup()

s_ij = compute_payroll_weighted_share(data_full, 'iota', 'jid_masked', 'real_hrly_wage_dec')
s_jg = compute_payroll_weighted_share(data_full, 'jid_masked', 'gamma', 'real_hrly_wage_dec')
s_fm = compute_payroll_weighted_share(data_full, 'cnpj_raiz', 'mkt_mayara', 'real_hrly_wage_dec')
s_gi = compute_payroll_weighted_share(data_full, 'gamma', 'iota', 'real_hrly_wage_dec')

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


data_full = data_full.merge(pd.DataFrame(markdown_w_iota).reset_index().rename(columns={0:'markdown_w_iota'}), on='jid_masked', how='outer',validate='m:1', indicator='_m_md')

data_full._m_md.value_counts()
data_full.drop(columns='_m_md', inplace=True)

data_full.markdown_w_iota.describe()

data_full['y_tilde'] = data_full.ln_real_hrly_wage_dec + np.log(data_full.markdown_w_iota)

reg_df = data_full[['wid_masked','jid_masked','iota','gamma', 'occ2', 'code_micro', 'y_tilde', 'ln_real_hrly_wage_dec', 'markdown_w_iota']]

reg_df['iota_gamma_id'] = reg_df.groupby(['iota', 'gamma']).ngroup()


reg_df['iota_gamma'] = reg_df.iota.astype(int).astype(str) + '_' + reg_df.gamma.astype(int).astype(str)


### Trim approximately top 1% of wages
reg_df = reg_df.loc[reg_df.ln_real_hrly_wage_dec<5]

# Save dataframe as .dta file
dta_path        = root + 'Data/derived/MarketPower_reghdfe_data.dta'
results_path    = root + 'Data/derived/MarketPower_reghdfe_results.dta'
reg_df.to_stata(dta_path, write_index=False)
reg_df.to_parquet(root + 'Data/derived/MarketPower_reghdfe_data.parquet')


### This works!
# Paths
stata_path = r"C:\Program Files (x86)\Stata14\StataMP-64.exe"  # Adjust this path
do_file_path = root + r"Code\MarketPower\run_twoway_fes.do"  # Adjust this path
log_file_path = root + r"Code\MarketPower\run_twoway_fes.log"


# Your Stata code as a Python string
stata_code = f"""
clear
set more off
log using "log_file_path", replace

use "dta_path", clear
reghdfe y_tilde, absorb(jid_masked_fes=jid_masked iota_gamma_fes=iota_gamma_id, savefe) residuals(resid)

save "results_path", replace
"""


# Run the function with your Stata code
results_reg1 = run_stata_code(reg_df, stata_code)
reg_df_w_FEs = results_reg1['results_df']
# Quickly check the variance decomposition
reg_df_w_FEs.jid_masked_fes.var() / reg_df_w_FEs.y_tilde.var()
#Out[35]: 0.846037664821534
reg_df_w_FEs.iota_gamma_fes.var() / reg_df_w_FEs.y_tilde.var()
#Out[36]: 0.0067746838565168055
reg_df_w_FEs.resid.var() / reg_df_w_FEs.y_tilde.var()
#Out[37]: 0.14825373239440456



collapsed_df = reg_df_w_FEs.groupby(['iota', 'gamma', 'iota_gamma_id', 'jid_masked']).agg({
    'jid_masked_fes': 'first',          # These don't vary within the group, so we can take the first value
    'iota_gamma_fes': 'first',          # These don't vary within the group, so we can take the first value
    'markdown_w_iota': 'first',         # These don't vary within the group, so we can take the first value
    'occ2': 'first',                    # These don't vary within the group, so we can take the first value
    'code_micro': pd.Series.mode,       # These rarely vary within the group, so we can take the mode
    'ln_real_hrly_wage_dec': 'mean',    # Average of log earnings within the group
    'wid_masked': 'count'               # Count of rows in this group
}).reset_index()

# Rename the count column to something more descriptive
collapsed_df = collapsed_df.rename(columns={'wid_masked': 'iota_gamma_jid_count'})

# Display the first few rows of the collapsed dataframe
print(collapsed_df.head())

# Display information about the collapsed dataframe
print(collapsed_df.info())


####################################################################################
####################################################################################
# Randomly generate shocks and then find new equilibrium
####################################################################################
####################################################################################


###########################
# This is the simple shock 


def generate_shocks(df, alpha=2, beta=5, delta=0):
    
    gamma_jid_masked_cw = df[['gamma','jid_masked']].drop_duplicates()
    # Draw a Bernoulli parameter for each gamma
    p_gamma = {gamma: np.random.beta(alpha, beta, 1)[0] for gamma in df['gamma'].unique()} # This is phi_j
    gamma_jid_masked_cw['p_gamma'] = gamma_jid_masked_cw['gamma'].map(p_gamma)
    
    # Draw Bernoulli samples for each row using the probability in p_gamma column
    gamma_jid_masked_cw['Z_j'] = np.random.binomial(1, gamma_jid_masked_cw['p_gamma'])
    df = df.merge(gamma_jid_masked_cw[['jid_masked','Z_j']], on='jid_masked', how='inner', validate='m:1', indicator=False)
  
    # Generate a random shock 
    # This is called "xi" on Overleaf
    jid_shock = {jid_masked: np.random.random() for jid_masked in df['jid_masked'].unique()} # This is phi_j
    df['jid_masked_shock'] = df['jid_masked'].map(jid_shock)
    # This is called "zeta" on Overleaf
    iota_gamma_shock = {iota_gamma_id: np.random.random() for iota_gamma_id in df['iota_gamma_id'].unique()}
    df['iota_gamma_shock'] = df['iota_gamma_id'].map(iota_gamma_shock)

    df['phi_iota_j_new'] = np.exp( \
        df['iota_gamma_fes']   + df['jid_masked_fes'] + \
        df['iota_gamma_shock'] + df['jid_masked_shock'] + \
        delta * df['Z_j']) 
    
    return df

delta = 0.01
collapsed_df_w_shock = generate_shocks(collapsed_df, delta=delta)
    
collapsed_df_w_shock['wage_guess_initial'] = collapsed_df_w_shock['markdown_w_iota'] * collapsed_df_w_shock['phi_iota_j_new']
collapsed_df_w_shock['wage_guess'] = collapsed_df_w_shock['wage_guess_initial']
collapsed_df_w_shock['iota_count'] = collapsed_df_w_shock.groupby('iota')['iota_gamma_jid_count'].transform('sum')



# Next steps:
#   1. Compute ell_iota_j following eq (45)
#   2. Then iterate through 45-50
#   3. Iterate until ell and w stabilize


collapsed_df_w_shock['real_hrly_wage_dec'] = np.exp(collapsed_df_w_shock['ln_real_hrly_wage_dec'])

# Restricting to jid-iotas with non-missing wages. We have 78862 missing values, all of which correspond to missing FEs and are iota_gamma_jid singletons.  
collapsed_df_w_shock = collapsed_df_w_shock.loc[collapsed_df_w_shock['wage_guess'].notna()]

collapsed_df_w_shock.to_pickle(root + 'Data/derived/tmp_collapsed_df_w_shock.p')
collapsed_df_w_shock = pd.read_pickle(root + 'Data/derived/tmp_collapsed_df_w_shock.p')


# Now we need to do some sort of iterate until convergence
diff = 1
tol = .0001
max_iter = 100
iter = 0
while (diff > tol) and (iter < max_iter):
    
    collapsed_df_w_shock[['s_gamma_iota','ell_iota_j']] = compute_ell(collapsed_df_w_shock, eta_bhm, theta_bhm)
    collapsed_df_w_shock['pi_iota_j'] = compute_pi(collapsed_df_w_shock)
    collapsed_df_w_shock['s_j_gamma'] = compute_s_j_gamma(collapsed_df_w_shock)
    collapsed_df_w_shock['epsilon_j'] = compute_epsilon_j(collapsed_df_w_shock, eta_bhm, theta_bhm)
    
    # Update wage_guess
    collapsed_df_w_shock['wage_guess_new'] = collapsed_df_w_shock['epsilon_j']/(1+collapsed_df_w_shock['epsilon_j']) * collapsed_df_w_shock['phi_iota_j_new']
    diff = np.abs(collapsed_df_w_shock['wage_guess_new'] - collapsed_df_w_shock['wage_guess']).sum()
    #diff_l = np.abs(collapsed_df_w_shock['wage_guess_new'] - collapsed_df_w_shock['wage_guess_new']).sum()
    collapsed_df_w_shock['wage_guess'] = collapsed_df_w_shock['wage_guess_new']
    if True: #iter%10==0:
        print(iter)
        print(diff)
    iter += 1

collapsed_df_w_shock['wage_post_shock'] = collapsed_df_w_shock['wage_guess']

# XX This seems to get close to converging but there are still a tiny number with non-trivial differences



##########################################################################################
##########################################################################################
# Two-step regressions to estimate eta and theta
##########################################################################################
##########################################################################################

run_two_step_ols = False
if run_two_step_ols == True:
    ##########
    # Step 1
    
    reg_df2 = collapsed_df_w_shock[['ell_iota_j','wage_post_shock','iota_gamma_id','iota']]
    reg_df2['ln_ell_iota_j']   = np.log(reg_df2['ell_iota_j'])
    reg_df2['ln_wage_post_shock']         = np.log(reg_df2['wage_post_shock'])
    
    
    # Your Stata code as a Python string
    stata_code = f"""
    clear
    set more off
    log using "log_file_path", replace
    use "dta_path", clear
    reghdfe ln_ell_iota_j ln_wage_post_shock, absorb(iota_gamma_id)
    
    scalar eta_hat =  _b[ln_wage_post_shock] - 1
    local scalars "eta_hat"
    
    save "results_path", replace
    
    """
    
    # Run the function with your Stata code
    results = run_stata_code(reg_df2, stata_code)
    
    eta_hat = results['scalar_results']['eta_hat']
    
    
    ##########
    # Step 2
    reg_df2['wage_1PlusEta'] = reg_df2['wage_post_shock'] ** (1+eta_hat)
    # Collapse to iota-gamma level
    reg_df3 = reg_df2.groupby('iota_gamma_id').agg({
        'ell_iota_j': 'sum',  # Calculate the mean of this column
        'wage_1PlusEta': 'sum',        # Calculate the mean of this column
        'iota': 'first'           # Take the first value of this column
    }).reset_index()
    reg_df3['wage_ces_index'] = reg_df3['wage_1PlusEta'] ** (1 / (1+eta_hat))
    reg_df3['ln_ell_iota_gamma'] = np.log(reg_df3['ell_iota_j'])
    reg_df3['ln_wage_ces_index'] = np.log(reg_df3['wage_ces_index'])
    
    # Your Stata code as a Python string
    stata_code = f"""
    clear
    set more off
    log using "log_file_path", replace
    use "dta_path", clear
    reghdfe ln_ell_iota_gamma ln_wage_ces_index, absorb(iota)
    
    scalar theta_hat =  _b[ln_wage_ces_index] - 1
    local scalars "theta_hat"
    save "results_path", replace
    """
    
    # Run the function with your Stata code
    results = run_stata_code(reg_df3, stata_code)
    theta_hat = results['scalar_results']['theta_hat']
    
    
    print(eta_bhm, eta_hat)
    print(theta_bhm, theta_hat)


##########################################################################################
##########################################################################################
# Panel version of two-step regressions to estimate eta and theta 
##########################################################################################
##########################################################################################


run_two_step_panel_ols = False
if run_two_step_panel_ols == True:
    ##########
    # Step 1
    
    reg_df2 = collapsed_df_w_shock[['iota_gamma_jid_count', 'ell_iota_j','real_hrly_wage_dec','wage_post_shock','iota_gamma_id','iota']]
    reg_df2.rename(columns={'iota_gamma_jid_count':'ell_iota_j_pre_shock', 'ell_iota_j':'ell_iota_j_post_shock', 'real_hrly_wage_dec':'wage_pre_shock'}, inplace=True)
    
    for var in ['ell_iota_j_pre_shock', 'ell_iota_j_post_shock', 'wage_pre_shock', 'wage_post_shock']:
        reg_df2['ln_' + var]   = np.log(reg_df2[var])
    
    for var in ['ell_iota_j', 'wage']:
        reg_df2['diff_ln_' + var] = reg_df2['ln_' + var + '_post_shock'] - reg_df2['ln_' + var + '_pre_shock']
    
    # Your Stata code as a Python string
    stata_code = f"""
    clear
    set more off
    log using "log_file_path", replace
    use "dta_path", clear
    reghdfe diff_ln_ell_iota_j diff_ln_wage, absorb(iota_gamma_id)
    
    scalar eta_hat =  _b[diff_ln_wage] - 1
    local scalars "eta_hat"
    
    save "results_path", replace
    
    """
    
    # Run the function with your Stata code
    results = run_stata_code(reg_df2, stata_code)
    eta_hat = results['scalar_results']['eta_hat']
    

    ##########
    # Step 2
    reg_df2['wage_1PlusEta_pre_shock'] = reg_df2['wage_pre_shock'] ** (1+eta_hat)
    reg_df2['wage_1PlusEta_post_shock'] = reg_df2['wage_post_shock'] ** (1+eta_hat)
    # Collapse to iota-gamma level
    reg_df3 = reg_df2.groupby('iota_gamma_id').agg({
        'ell_iota_j_pre_shock': 'sum',        # Calculate the mean of this column
        'ell_iota_j_post_shock': 'sum',        # Calculate the mean of this column
        'wage_1PlusEta_pre_shock': 'sum',     # Calculate the mean of this column
        'wage_1PlusEta_post_shock': 'sum',     # Calculate the mean of this column
        'iota': 'first'             # Take the first value of this column
    }).reset_index()
    
    reg_df3['wage_ces_index_pre_shock']  = reg_df3['wage_1PlusEta_pre_shock'] ** (1 / (1+eta_hat))
    reg_df3['wage_ces_index_post_shock'] = reg_df3['wage_1PlusEta_post_shock'] ** (1 / (1+eta_hat))
    reg_df3['diff_ln_ell_iota_gamma']  = np.log(reg_df3['ell_iota_j_post_shock']) - np.log(reg_df3['ell_iota_j_pre_shock'])
    reg_df3['diff_ln_wage_ces_index']  = np.log(reg_df3['wage_ces_index_post_shock']) - np.log(reg_df3['wage_ces_index_pre_shock'])

    # Your Stata code as a Python string
    stata_code = f"""
    clear
    set more off
    log using "log_file_path", replace
    use "dta_path", clear
    reghdfe diff_ln_ell_iota_gamma diff_ln_wage_ces_index, absorb(iota)
    
    scalar theta_hat =  _b[diff_ln_wage_ces_index] - 1
    local scalars "theta_hat"
    save "results_path", replace
    """
    
    # Run the function with your Stata code
    results = run_stata_code(reg_df3, stata_code)
    theta_hat = results['scalar_results']['theta_hat']
    
    
    print(eta_bhm, eta_hat)
    print(theta_bhm, theta_hat)



##########################################################################################
##########################################################################################
# Panel version of two-step regressions to estimate eta and theta but instrument 
##########################################################################################
##########################################################################################


run_two_step_panel_iv = True
if run_two_step_panel_iv == True:
    
    ##########
    # Step 1
    
    reg_df2 = collapsed_df_w_shock[['iota_gamma_jid_count', 'ell_iota_j','real_hrly_wage_dec','wage_post_shock','iota_gamma_id','iota', 'jid_masked_shock', 'iota_gamma_shock','Z_j']]
    if delta==0:
        reg_df2['shock_iv'] = reg_df2['jid_masked_shock'] + reg_df2['iota_gamma_shock']
    else:
        reg_df2['shock_iv'] = reg_df2['Z_j']
    reg_df2.rename(columns={'iota_gamma_jid_count':'ell_iota_j_pre_shock', 'ell_iota_j':'ell_iota_j_post_shock', 'real_hrly_wage_dec':'wage_pre_shock'}, inplace=True)
    
    for var in ['ell_iota_j_pre_shock', 'ell_iota_j_post_shock', 'wage_pre_shock', 'wage_post_shock']:
        reg_df2['ln_' + var]   = np.log(reg_df2[var])
    
    for var in ['ell_iota_j', 'wage']:
        reg_df2['diff_ln_' + var] = reg_df2['ln_' + var + '_post_shock'] - reg_df2['ln_' + var + '_pre_shock']
    
    # Your Stata code as a Python string
    stata_code = f"""
    clear
    set more off
    log using "log_file_path", replace
    use "dta_path", clear
    ivreghdfe diff_ln_ell_iota_j (diff_ln_wage = shock_iv) , absorb(iota_gamma_id)
    
    scalar eta_hat =  _b[diff_ln_wage] - 1
    local scalars "eta_hat"
    
    save "results_path", replace
    
    """
    
    # Run the function with your Stata code
    results = run_stata_code(reg_df2, stata_code)
    eta_hat = results['scalar_results']['eta_hat']
    

    ##########
    # Step 2
    reg_df2['wage_1PlusEta_pre_shock'] = reg_df2['wage_pre_shock'] ** (1+eta_hat)
    reg_df2['wage_1PlusEta_post_shock'] = reg_df2['wage_post_shock'] ** (1+eta_hat)
    # Collapse to iota-gamma level
    reg_df3 = reg_df2.groupby('iota_gamma_id').agg({
        'ell_iota_j_pre_shock': 'sum',        # Calculate the mean of this column
        'ell_iota_j_post_shock': 'sum',        # Calculate the mean of this column
        'wage_1PlusEta_pre_shock': 'sum',     # Calculate the mean of this column
        'wage_1PlusEta_post_shock': 'sum',     # Calculate the mean of this column
        'shock_iv': 'mean',     # Calculate the mean of this column
        'iota': 'first'             # Take the first value of this column
    }).reset_index()
    
    reg_df3['wage_ces_index_pre_shock']  = reg_df3['wage_1PlusEta_pre_shock'] ** (1 / (1+eta_hat))
    reg_df3['wage_ces_index_post_shock'] = reg_df3['wage_1PlusEta_post_shock'] ** (1 / (1+eta_hat))
    reg_df3['diff_ln_ell_iota_gamma']  = np.log(reg_df3['ell_iota_j_post_shock']) - np.log(reg_df3['ell_iota_j_pre_shock'])
    reg_df3['diff_ln_wage_ces_index']  = np.log(reg_df3['wage_ces_index_post_shock']) - np.log(reg_df3['wage_ces_index_pre_shock'])

    # Your Stata code as a Python string
    stata_code = f"""
    clear
    set more off
    log using "log_file_path", replace
    use "dta_path", clear
    ivreghdfe diff_ln_ell_iota_gamma (diff_ln_wage_ces_index = shock_iv) , absorb(iota)

    
    scalar theta_hat =  _b[diff_ln_wage_ces_index] - 1
    local scalars "theta_hat"
    save "results_path", replace
    """
    
    # Run the function with your Stata code
    results = run_stata_code(reg_df3, stata_code)
    theta_hat = results['scalar_results']['theta_hat']
    
    
    print(eta_bhm, eta_hat)
    print(theta_bhm, theta_hat)





####################################################################################
####################################################################################
# Next steps:
#   1) Run panel OLS or first difference regressions using the same specs we did on the simulated data, we do the difference between the simulated post-shock data and the pre-shock data. See if this still gives us eta and theta. Probably won't be as clean as the OLS, but hopefully it's ok. We know that this should fit well in theory because the only variation between periods is the demand shock (no confounding labor supply shifts).
# - DONE but unsurprisingly gives bad estimates
#   2) Do the first difference regression above but instead instrument for the first differences with the shocks we generated. This should give us identical results to the first differenced regression because all the variation is exogenous
# - DONE. Gives almost exact estimates
#   3) Simulate a shock that has some coherent structure but let's only use one component of the shock (which would be an approximation of the tariff shock). We still only have exogenous shocks but we're adding noise because we're only using part of the shock as an instrument. 
#   - Append additional structure to the shock. Let's keep the same shock structure we already have (mean-zero iota-gamma shock and mean-zero jid shock) but imagine this is unobserved. Then we want an instrument akin to the tariff shock. Do a two-step sampling thing. 
#   Step 1: at iota-gamma level draw a probability between 0 and 1 from a beta distribution or something. This is a P_ig
#   Step 2: For each job within iota-gamma we draw a binary indicator from a Bernoulli(P_ig) distribution. We are saying that for each market a different fraction of jobs are shocked, but within a market we randomly assign which jobs are shocked.
#   - Given this we can play around with the mean and variance of the shock as well as the number of jobs shocked (how big/widespread the shock is). We know that the job-level shock is correlated with the firm-level demand shock.
#   This allows us to test robustness to an imperfect instrument, varying the level of imperfection. Can make the alpha very small to generate a weak instrument. 
#   DONE - For large enough delta (we tried 0.5) this gives us basically correct estimates. As we reduce delta, the estimates get worse. 
#   4) Do all of the above with different market definitions (assuming iota-gamma is the truth) and using iotas or not (to compute markdowns)
#   5) Do all of the above but add supply shocks (e.g. amenities) and test robustness of results to this confounding variation
#   6) Try to go back to real data with what we've learned. 
####################################################################################
####################################################################################





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





