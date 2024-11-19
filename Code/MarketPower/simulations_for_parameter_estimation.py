#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on August 13 2024
- Adapted from NetworksGit/Code/MarketPower/do_all_marketpower.py
- Goal is to delete unnecessary code to focus on simply computing distributions of HHIs and markdowns across different market definitions

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
import platform
import statsmodels.api as sm



# Global variables used by run_stata_code()
PRINT_STATA_LOG = False

homedir = os.path.expanduser('~')
os_name = platform.system()
if getpass.getuser()=='p13861161':
    if os_name == 'Windows':
        print("Running on Windows") 
        root = "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/"
        rais = "//storage6/bases/DADOS/RESTRITO/RAIS/"
        sys.path.append(root + 'Code/Modules')
        sys.path.append(r'C:\ProgramData\anaconda3\Lib\site-packages\src')
        stata_or_python = "Stata"
    elif os_name == 'Linux':
        print("Running on Linux") 
        root = "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/"
        rais = "/home/DLIPEA/p13861161/rais/RAIS/"
        sys.path.append(root + 'Code/Modules')
        import pyfixest as pf
        stata_or_python = "Python"

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

######
# XX Un-hardcode our job definition. Replace 'jid_masked' with jobvar. I can mostly ctrl-F it but will need to add it as an argument to various functions
# Try replacing jid with the intersection of gamma and firm
# I think Mayara's equivalent would be occ-firm or occ-estab

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

def compute_s_MarketWorkertype(df, jobvar, workertypevar, marketvar, eta, theta):
    '''
    # This is the quantity Ben and Bernardo derived on the white board on 8/14
    # - Numerator: for each iota compute the total (hourly) earnings for that iota in job j. Raise this to (1+eta). Then sum these quantities over all jobs j in market gamma and raise this quantity to the (1+theta)/(1+eta).
    # Denominator: Compute the numerator for each market gamma and sum over all markets gamma
    # - The result will be one value for each iota, all of which sum to 1.

   
   
    # I don't think we actually need this to be balanced
    # Get all unique firms and markets
    all_iotas  = df[workertypevar].unique()
    all_gammas = df[marketvar].unique()
    # Create a DataFrame with all possible firm-market combinations
    all_combinations = pd.DataFrame([(iota, gamma) for iota in all_iotas for gamma in all_gammas], columns=[workertypevar, marketvar])
    '''
    # Group by iota and job, and sum hourly earnings
    job_earnings = df.groupby([workertypevar, jobvar,marketvar])['real_hrly_wage_dec'].sum().reset_index()
    # Compute the (1+eta) power of earnings
    job_earnings['earnings_powered'] = job_earnings['real_hrly_wage_dec'] ** (1 + eta)
    # Group by iota and market (gamma), and sum the powered earnings
    market_earnings = job_earnings.groupby([workertypevar, marketvar])['earnings_powered'].sum().reset_index()
    # Compute the (1+theta)/(1+eta) power of the sum
    market_earnings['market_sum_powered'] = market_earnings['earnings_powered'] ** ((1 + theta) / (1 + eta))
    numerator = market_earnings[[workertypevar, marketvar, 'market_sum_powered']]
    # Compute the denominator: sum of numerators over all gammas within each iota
    denominator = numerator.groupby(workertypevar)['market_sum_powered'].sum().reset_index()

    '''
    # Merge the all_combinations with numerator on iota and gamma
    merged = pd.merge(all_combinations, numerator, on=[workertypevar, marketvar], how='left')
    merged['market_sum_powered'] = merged['market_sum_powered'].fillna(0)
    # Merge with denominator on iota
    merged = pd.merge(merged, denominator, on=workertypevar, suffixes=('_gi', '_i'))
    # Compute the payroll-weighted share for each gamma-iota
    merged['s_MarketWorkertype'] = merged['market_sum_powered_gi'] / merged['market_sum_powered_i']
    '''
    merged = pd.merge(numerator, denominator, on=workertypevar, suffixes=('_gi', '_i'))
    # Compute the payroll-weighted share for each gamma-iota
    merged['s_MarketWorkertype'] = merged['market_sum_powered_gi'] / merged['market_sum_powered_i']
    return merged[[workertypevar, marketvar, 's_MarketWorkertype']]

def run_stata_code(reg_df, stata_code, dta_path=None, results_path=None, do_file_path=None, log_file_path=None, scalar_results_path=None, stata_path=None):
    global PRINT_STATA_LOG 
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

    # Create a path for the success indicator file
    success_indicator_path = os.path.join(temp_dir, 'stata_success.tmp')
    temp_files.append(success_indicator_path)

    try:
        # Save dataframe as .dta file
        reg_df.to_stata(dta_path, write_index=False)

        # Modify Stata code to include scalar results and success indicator
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

        // Create success indicator file
        file open success using "{success_indicator_path}", write replace
        file write success "success"
        file close success
        """

        # Replace placeholders in stata_code
        stata_code = stata_code.replace("dta_path", dta_path)
        stata_code = stata_code.replace("results_path", results_path)
        stata_code = stata_code.replace("log_file_path", log_file_path)

        if PRINT_STATA_LOG:
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

            # Check if log file exists and PRINT_STATA_LOG is True
            if os.path.exists(log_file_path) and PRINT_STATA_LOG:
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

        # Read any remaining content after process finishes if PRINT_STATA_LOG is True
        if os.path.exists(log_file_path) and PRINT_STATA_LOG:
            with open(log_file_path, 'r') as log_file:
                log_file.seek(last_position)
                remaining_content = log_file.read()
                if remaining_content:
                    print(remaining_content, end='')

        # Check for success indicator file
        if not os.path.exists(success_indicator_path):
            with open(log_file_path, 'r') as log_file:
                log_content = log_file.read()
            raise RuntimeError(f"Stata code failed to execute successfully. Log file contents:\n{log_content}")

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

def compute_markdowns_w_iota(df, wagevar, jobvar, marketvar, workertypevar, eta, theta):

    
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
    
    s_ij = compute_payroll_weighted_share(data_full, workertypevar, jobvar,         wagevar)
    s_ij = s_ij.loc[s_ij.payroll_weighted_share>0]    # Delete empty rows to save memory
    
    s_jg = compute_payroll_weighted_share(data_full, jobvar,        marketvar,      wagevar)
    s_jg = s_jg.set_index(jobvar, verify_integrity=True)   # This is critical to make sure the columns being summed have the same index
     
    # If we want to use Mayara's definitions
    #s_fm = compute_payroll_weighted_share(data_full, 'cnpj_raiz', 'mkt_mayara', wagevar)
    
    s_gi_hat = compute_s_MarketWorkertype(df, jobvar, workertypevar, marketvar, eta, theta)
    
    job_market_cw = data_full[[jobvar, marketvar]].drop_duplicates()
    # Note that s_ij corresponds to pi_{j \iota} on OVerleaf. Need to clean up notation.
    product = s_ij.merge(job_market_cw, on=jobvar, how='left', validate='m:1', indicator='_merge1')
    product = product.merge(s_gi_hat, on=[workertypevar,marketvar], how='left', validate='m:1', indicator='_merge2')
    product['product'] = product['s_MarketWorkertype'] * product['payroll_weighted_share']
    sum_product = product.groupby(jobvar)['product'].sum()

    epsilon_j = eta * (1 - s_jg['payroll_weighted_share']) + theta * s_jg['payroll_weighted_share'] * (1 - sum_product)
    
    # Display distribution of elasticities 
    print(epsilon_j.describe())
    
    markdown_w_iota = epsilon_j / (1 + epsilon_j)
    return markdown_w_iota

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
    iota_gamma_shock = {iota_gamma: np.random.random() for iota_gamma in df['iota_gamma'].unique()}
    df['iota_gamma_shock'] = df['iota_gamma'].map(iota_gamma_shock)

    df['phi_iota_j_new'] = np.exp( \
        df['iota_gamma_fes']   + df['jid_masked_fes'] + \
        df['iota_gamma_shock'] + df['jid_masked_shock'] + \
        delta * df['Z_j']) 
    
    return df

def run_two_step_estimation(collapsed_df_w_shock, estimation_strategy, delta=0, workertypevar = ['iota'], mktvar = ['gamma'], stata_or_python='Python'):
    # Step 1: Data Preparation
    reg_df2 = prepare_step1_data(collapsed_df_w_shock, estimation_strategy, workertypevar = workertypevar, mktvar = mktvar, delta = delta)
    
    # Step 1: Estimate eta_hat
    eta_hat = estimate_eta_hat(reg_df2, estimation_strategy, stata_or_python)
    
    # Step 2: Data Preparation
    reg_df3 = prepare_step2_data(reg_df2, eta_hat, estimation_strategy)
    
    # Step 2: Estimate theta_hat
    theta_hat = estimate_theta_hat(reg_df3, estimation_strategy, stata_or_python)
    
    return eta_hat, theta_hat


def prepare_step1_data(collapsed_df_w_shock, estimation_strategy, workertypevar, mktvar, delta=0):
    for var in workertypevar + mktvar:
        collapsed_df_w_shock[var] = collapsed_df_w_shock[var].astype(str)
        
    collapsed_df_w_shock['worker_mkt_id'] = collapsed_df_w_shock.groupby(workertypevar + mktvar).ngroup()
    collapsed_df_w_shock['worker_type_id'] = collapsed_df_w_shock.groupby(workertypevar).ngroup()
        
    variables = [
        'ell_iota_j_pre_shock', 'ell_iota_j_post_shock', 'wage_pre_shock', 'wage_post_shock',
        'worker_mkt_id', 'worker_type_id'
    ]
    if estimation_strategy == 'panel_iv':
        variables += ['jid_masked_shock', 'iota_gamma_shock', 'Z_j']
    reg_df2 = collapsed_df_w_shock[variables].copy()
    
    for var in ['ell_iota_j_pre_shock', 'ell_iota_j_post_shock', 'wage_pre_shock', 'wage_post_shock']:
        reg_df2['ln_' + var]   = np.log(reg_df2[var])
    
    for var in ['ell_iota_j', 'wage']:
        reg_df2['diff_ln_' + var] = reg_df2['ln_' + var + '_post_shock'] - reg_df2['ln_' + var + '_pre_shock']
    
    # Instrument for IV estimation
    if estimation_strategy == 'panel_iv':
        if delta == 0:
            reg_df2['shock_iv'] = reg_df2['jid_masked_shock'] + reg_df2['iota_gamma_shock']
        else:
            reg_df2['shock_iv'] = reg_df2['Z_j']
    
    return reg_df2

def estimate_eta_hat(reg_df2, estimation_strategy, stata_or_python):
    if estimation_strategy == 'ols':
        regression_type = 'ols'
        dependent_var = 'ln_ell_iota_j_post_shock'
        independent_var = 'ln_wage_post_shock'
        absorb_var = 'worker_mkt_id'
    elif estimation_strategy == 'panel_ols':
        regression_type = 'ols'
        dependent_var = 'diff_ln_ell_iota_j'
        independent_var = 'diff_ln_wage'
        absorb_var = 'worker_mkt_id'
    elif estimation_strategy == 'panel_iv':
        regression_type = 'iv'
        dependent_var = 'diff_ln_ell_iota_j'
        independent_var = 'diff_ln_wage'
        instrument_var = 'shock_iv'
        absorb_var = 'worker_mkt_id'
    else:
        raise ValueError("Invalid estimation strategy.")
    
    if stata_or_python=='Stata':
        eta_hat = run_stata_regression(
            dataframe=reg_df2,
            regression_type=regression_type,
            dependent_var=dependent_var,
            independent_var=independent_var,
            instrument_var=instrument_var if estimation_strategy == 'panel_iv' else None,
            absorb_var=absorb_var,
            scalar_name='eta_hat'
        )
    if stata_or_python=='Python':
        if regression_type=='ols':
            fit = pf.feols(f"{dependent_var} ~ {independent_var} | {absorb_var}", data=reg_df2)
        elif regression_type=='iv':
            fit = pf.feols(f"{dependent_var} ~ 1 | {absorb_var} | {independent_var} ~ {instrument_var} ", data=reg_df2)
        eta_hat = fit.coef().loc[independent_var] - 1
    return eta_hat

def prepare_step2_data(reg_df2, eta_hat, estimation_strategy):
    for period in ['pre_shock', 'post_shock']:
        reg_df2[f'wage_1PlusEta_{period}'] = reg_df2[f'wage_{period}'] ** (1 + eta_hat)
    
    # XXBM: I swtiched iota per worker_type_id below, is that right?
    agg_dict = {
        'ell_iota_j_pre_shock': 'sum',
        'ell_iota_j_post_shock': 'sum',
        'wage_1PlusEta_pre_shock': 'sum',
        'wage_1PlusEta_post_shock': 'sum',
        'worker_type_id': 'first'
    }
    if estimation_strategy == 'panel_iv':
        agg_dict['shock_iv'] = 'mean'
        
    reg_df3 = reg_df2.groupby('worker_mkt_id').agg(agg_dict).reset_index()

    for period in ['pre_shock', 'post_shock']:
        reg_df3[f'wage_ces_index_{period}'] = reg_df3[f'wage_1PlusEta_{period}'] ** (1 / (1 + eta_hat))
    
    if estimation_strategy == 'ols':
        reg_df3['wage_ces_index_post_shock'] = reg_df3['wage_1PlusEta_post_shock'] ** (1 / (1 + eta_hat))
        reg_df3['ln_ell_iota_gamma_post_shock'] = np.log(reg_df3['ell_iota_j_post_shock'])
        reg_df3['ln_wage_ces_index_post_shock'] = np.log(reg_df3['wage_ces_index_post_shock'])
    elif estimation_strategy in ['panel_ols', 'panel_iv']:
        reg_df3['diff_ln_ell_iota_gamma'] = np.log(reg_df3['ell_iota_j_post_shock']) - np.log(reg_df3['ell_iota_j_pre_shock'])
        reg_df3['diff_ln_wage_ces_index'] = np.log(reg_df3['wage_ces_index_post_shock']) - np.log(reg_df3['wage_ces_index_pre_shock'])
    else:
        raise ValueError("Invalid estimation strategy.")
    
    return reg_df3

def estimate_theta_hat(reg_df3, estimation_strategy, stata_or_python):
    if estimation_strategy == 'ols':
        regression_type = 'ols'
        dependent_var = 'ln_ell_iota_gamma_post_shock'
        independent_var = 'ln_wage_ces_index_post_shock'
        absorb_var = 'worker_type_id'
    elif estimation_strategy == 'panel_ols':
        regression_type = 'ols'
        dependent_var = 'diff_ln_ell_iota_gamma'
        independent_var = 'diff_ln_wage_ces_index'
        absorb_var = 'worker_type_id'
    elif estimation_strategy == 'panel_iv':
        regression_type = 'iv'
        dependent_var = 'diff_ln_ell_iota_gamma'
        independent_var = 'diff_ln_wage_ces_index'
        instrument_var = 'shock_iv'
        absorb_var = 'worker_type_id'
    else:
        raise ValueError("Invalid estimation strategy.")
    if stata_or_python=='Stata':
        theta_hat = run_stata_regression(
            dataframe=reg_df3,
            regression_type=regression_type,
            dependent_var=dependent_var,
            independent_var=independent_var,
            instrument_var=instrument_var if estimation_strategy == 'panel_iv' else None,
            absorb_var=absorb_var,
            scalar_name='theta_hat'
        )
    if stata_or_python=='Python':
        if regression_type=='ols':
            fit = pf.feols(f"{dependent_var} ~ {independent_var} | {absorb_var}", data=reg_df3)
        elif regression_type=='iv':
            fit = pf.feols(f"{dependent_var} ~ 1 | {absorb_var} | {independent_var} ~ {instrument_var} ", data=reg_df3)
        theta_hat = fit.coef().loc[independent_var] - 1
    return theta_hat

def run_stata_regression(dataframe, regression_type, dependent_var, independent_var, instrument_var=None, absorb_var=None, scalar_name='parameter_hat'):
    # Build the regression command
    if regression_type == 'ols':
        regression_command = f"reghdfe {dependent_var} {independent_var}"
    elif regression_type == 'iv':
        regression_command = f"ivreghdfe {dependent_var} ({independent_var} = {instrument_var})"
    else:
        raise ValueError("Invalid regression type.")
    
    if absorb_var:
        regression_command += f", absorb({absorb_var})"
    else:
        regression_command += ", noabsorb"
    # Build the Stata code
    stata_code = f"""
    clear
    set more off
    log using "log_file_path", replace
    use "dta_path", clear
    {regression_command}
    
    scalar {scalar_name} = _b[{independent_var}] - 1
    local scalars "{scalar_name}"
    
    save "results_path", replace
    """
    
    # Run the regression and extract the parameter
    results = run_stata_code(dataframe, stata_code)
    parameter_hat = results['scalar_results'][scalar_name]
    return parameter_hat

def find_equilibrium(df, eta, theta, tol=1e-4, max_iter=100):
    diff = tol + 1
    iter_count = 0
    while diff > tol and iter_count < max_iter:
        df[['s_gamma_iota','ell_iota_j']] = compute_ell(df, eta_bhm, theta_bhm)
        df['pi_iota_j'] = compute_pi(df)
        df['s_j_gamma'] = compute_s_j_gamma(df)
        df['epsilon_j'] = compute_epsilon_j(df, eta, theta)
        
        # Update wage_guess
        df['wage_guess_new'] = df['epsilon_j']/(1+df['epsilon_j']) * df['phi_iota_j_new']
        diff = np.abs(df['wage_guess_new'] - df['wage_guess']).sum()
        #diff_l = np.abs(collapsed_df_w_shock['wage_guess_new'] - collapsed_df_w_shock['wage_guess_new']).sum()
        df['wage_guess'] = df['wage_guess_new']
        if True: #iter%10==0:
            print(iter_count)
            print(diff)
        iter_count += 1
    return df['wage_guess']

def introduce_misclassification_by_jid(df, misclassification_rate):
    # Create a copy of the dataframe to avoid modifying the original
    df_misclassified = df.copy()
    
    # Calculate the probability distribution of gammas
    gamma_probs = df_misclassified['gamma'].value_counts(normalize=True)
    
    # Get unique jid_masked values
    unique_jids = df_misclassified['jid_masked'].unique()
    
    # Generate new gammas for all unique jids
    new_gammas = np.random.choice(gamma_probs.index, size=len(unique_jids), p=gamma_probs.values)
    
    # Create a mask for jids to be misclassified
    misclassify_mask = np.random.random(len(unique_jids)) < misclassification_rate
    
    # Create a dictionary mapping jids to their new gammas (only for misclassified jids)
    new_gamma_dict = dict(zip(unique_jids[misclassify_mask], new_gammas[misclassify_mask]))
    
    # Apply new gammas to misclassified jids
    df_misclassified['gamma_error'] = df_misclassified['jid_masked'].map(new_gamma_dict).fillna(df_misclassified['gamma'])
    
    return df_misclassified

def compute_market_hhi(df, market_col='gamma'):
    # Compute market shares
    market_shares = df[market_col].value_counts(normalize=True)
    
    # Compute HHI
    hhi = (market_shares**2).sum()
    
    return hhi




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

# Create variable for Mayara's market definitions
data_full['mkt_mayara'] = data_full.groupby(['occ2', 'code_micro']).ngroup()


# Compute Markdowns and merge on

markdown_w_iota = compute_markdowns_w_iota(data_full, 'real_hrly_wage_dec', 'jid_masked', 'gamma', 'iota', eta_bhm, theta_bhm)
data_full = data_full.merge(pd.DataFrame(markdown_w_iota).reset_index().rename(columns={0:'markdown_w_iota'}), on='jid_masked', how='outer',validate='m:1', indicator=False)

reg_df = data_full[['wid_masked','jid_masked','iota','gamma', 'occ2', 'code_micro', 'ln_real_hrly_wage_dec', 'markdown_w_iota']]

reg_df['y_tilde'] = reg_df.ln_real_hrly_wage_dec + np.log(reg_df.markdown_w_iota)
#reg_df['iota_gamma_id'] = reg_df.groupby(['iota', 'gamma']).ngroup()
reg_df['iota_gamma'] = reg_df.iota.astype(int).astype(str) + '_' + reg_df.gamma.astype(int).astype(str)

### Trim approximately top 1% of wages
reg_df = reg_df.loc[reg_df.ln_real_hrly_wage_dec<5]

# Save dataframe as .dta file
dta_path        = root + 'Data/derived/MarketPower_reghdfe_data.dta'
results_path    = root + 'Data/derived/MarketPower_reghdfe_results.dta'
reg_df.to_parquet(root + 'Data/derived/MarketPower_reghdfe_data.parquet')



#######################################################################
# Estimate iota-gamma and jid FEs

# Run the function with your Stata code
if stata_or_python=="Stata":
    # Your Stata code as a Python string
    stata_code = """
    clear
    set more off
    log using "log_file_path", replace
    
    use "dta_path", clear
    reghdfe y_tilde, absorb(jid_masked_fes=jid_masked iota_gamma_fes=iota_gamma, savefe) residuals(resid)
    
    save "results_path", replace
    """

    results_reg1 = run_stata_code(reg_df, stata_code)
    reg_df_w_FEs = results_reg1['results_df']

if stata_or_python=="Python":
    # Perform the regression with fixed effects for 'jid_masked' and 'iota_gamma'
    fit_ols = pf.feols("y_tilde ~ 1 | jid_masked + iota_gamma", data=reg_df)
    # Extract the fixed effects as a dictionary
    fixed_effects = fit_ols.fixef()
    # Loop through each fixed effect group and add it to the original DataFrame
    i = 1
    for fe_var, fe_values in fixed_effects.items():
        print(fe_var)
        fe_varname = fe_var[fe_var.find("(") + 1 : fe_var.find(")")]
        fe_df = pd.DataFrame(list(fixed_effects[fe_var].items()), columns=[fe_varname,f'{fe_varname}_fes'])
        fe_df[fe_varname] = fe_df[fe_varname].astype(reg_df[fe_varname].dtype)
        # Merge the fixed effect estimates back to the original DataFrame
        reg_df = reg_df.merge(fe_df, on=fe_varname, how='left', validate='m:1', indicator=f'_merge_{fe_varname}')
    reg_df_w_FEs = reg_df.copy()
    
    
collapsed_df = reg_df_w_FEs.groupby(['iota', 'gamma', 'iota_gamma', 'jid_masked']).agg({
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


####################################################################################
####################################################################################
# Randomly generate shocks and then find new equilibrium
####################################################################################
####################################################################################

###########################
# This is the simple shock 


delta = 0.5
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


collapsed_df_w_shock['wage_post_shock'] = find_equilibrium(collapsed_df_w_shock, eta_bhm, theta_bhm)

collapsed_df_w_shock.rename(columns={'iota_gamma_jid_count':'ell_iota_j_pre_shock', 'ell_iota_j':'ell_iota_j_post_shock', 'real_hrly_wage_dec':'wage_pre_shock'}, inplace=True)


collapsed_df_w_shock.to_pickle(root + 'Data/derived/tmp_collapsed_df_w_shock.p')
collapsed_df_w_shock = pd.read_pickle(root + 'Data/derived/tmp_collapsed_df_w_shock.p')


# Run the two-step estimation 


# "Oracle" spec: use the full instrument   reg_df2['shock_iv'] = reg_df2['jid_masked_shock'] + reg_df2['iota_gamma_shock']
eta_hat, theta_hat = run_two_step_estimation(collapsed_df_w_shock, 'panel_iv', delta=0, stata_or_python=stata_or_python)
panel_iv_oracle = (eta_hat, theta_hat)
# Print the results
print(f"Estimated eta_hat: {eta_hat}")
print(f"Estimated theta_hat: {theta_hat}")

# "Realistic" spec: use the partial instrument   reg_df2['shock_iv'] = reg_df2['Z_j']
eta_hat, theta_hat = run_two_step_estimation(collapsed_df_w_shock, 'panel_iv', delta=None, stata_or_python=stata_or_python)
panel_iv = (eta_hat, theta_hat)
# Print the results
print(f"Estimated eta_hat: {eta_hat}")
print(f"Estimated theta_hat: {theta_hat}")

eta_hat, theta_hat = run_two_step_estimation(collapsed_df_w_shock, 'panel_ols', delta=0, stata_or_python=stata_or_python)
panel_ols = (eta_hat, theta_hat)
# Print the results
print(f"Estimated eta_hat: {eta_hat}")
print(f"Estimated theta_hat: {theta_hat}")

eta_hat, theta_hat = run_two_step_estimation(collapsed_df_w_shock, 'ols', delta=0, stata_or_python=stata_or_python)
ols = (eta_hat, theta_hat)
# Print the results
print(f"Estimated eta_hat: {eta_hat}")
print(f"Estimated theta_hat: {theta_hat}")

########################
# Other market definitons
wkr = ['iota']
for mkt in [['gamma'], ['occ2'], ['code_micro'], ['occ2', 'code_micro']]:
    eta_hat, theta_hat = run_two_step_estimation(collapsed_df_w_shock, 'panel_iv', delta=None, workertypevar=wkr, mktvar=mkt, stata_or_python=stata_or_python)
    print('WORKERTYPE = '+ str(wkr) +', MKT =' + str(mkt) + ', eta = ' + str(eta_hat) + ', theta = ', str(theta_hat))
    
wkr = ['occ2']
for mkt in [['code_micro']]:
    eta_hat, theta_hat = run_two_step_estimation(collapsed_df_w_shock, 'panel_iv', delta=0, workertypevar=wkr, mktvar=mkt, stata_or_python=stata_or_python)
    print('WORKERTYPE = '+ str(wkr) +', MKT =' + str(mkt) + ', eta = ' + str(eta_hat) + ', theta = ', str(theta_hat))


##################
# Experiment with misclassification

# First, let's get the true estimates from the correctly classified data
eta_hat_true, theta_hat_true = run_two_step_estimation(collapsed_df_w_shock, 'ols', delta=0, workertypevar=['iota'], mktvar=['gamma'], stata_or_python=stata_or_python)

print(f"True eta_hat: {eta_hat_true}")
print(f"True theta_hat: {theta_hat_true}")


# Create lists to store our results
misclassification_rates = np.arange(0,1.1,0.1)
eta_estimates = []
theta_estimates = []
eta_theoretical = []
tilde_beta_1_list = []
tilde_beta_2_list = []
theta_theoretical = []
hhi_values = []
hhi_w_error_values = []

# Now, let's modify the loop to collect data
for r in misclassification_rates:
    collapsed_df_w_shock_w_error = introduce_misclassification_by_jid(collapsed_df_w_shock, r)
    eta_hat_e, theta_hat_e = run_two_step_estimation(collapsed_df_w_shock_w_error, 'ols', delta=0,  workertypevar=['iota'], mktvar=['gamma_error'], stata_or_python=stata_or_python)
    
    eta_estimates.append(eta_hat_e)
    theta_estimates.append(theta_hat_e)
    
    # Calculate HHI
    hhi = compute_market_hhi(collapsed_df_w_shock_w_error, market_col='gamma')
    hhi_w_error = compute_market_hhi(collapsed_df_w_shock_w_error, market_col='gamma_error')

    hhi_values.append(hhi)
    hhi_w_error_values.append(hhi_w_error)
    
    # Calculate theoretical elasticities based on the derivations we came up with on 10/28/2024    
    collapsed_df_w_shock_w_error['ln_wage_post_shock'] = np.log(collapsed_df_w_shock_w_error['wage_post_shock'])
    collapsed_df_w_shock_w_error['ln_wage_demeaned_within_iota_gamma'] = collapsed_df_w_shock_w_error['ln_wage_post_shock'] - collapsed_df_w_shock_w_error.groupby(['iota','gamma'])['ln_wage_post_shock'].transform('mean')
    collapsed_df_w_shock_w_error['ln_wage_demeaned_within_iota_m']     = collapsed_df_w_shock_w_error['ln_wage_post_shock'] - collapsed_df_w_shock_w_error.groupby(['iota','gamma_error'])['ln_wage_post_shock'].transform('mean')
    
    collapsed_df_w_shock_w_error['temp2'] = np.exp( collapsed_df_w_shock_w_error['ln_wage_post_shock'] * (1+eta_hat_true) )
    collapsed_df_w_shock_w_error['mu_iota_gamma_j'] = np.log( collapsed_df_w_shock_w_error.groupby(['iota','gamma'])['temp2'].transform('sum') )  / (1 + eta_hat_true)
    collapsed_df_w_shock_w_error['wbar_minus_mu_ig'] = collapsed_df_w_shock_w_error.groupby(['iota','gamma'])['ln_wage_post_shock'].transform('mean') - collapsed_df_w_shock_w_error['mu_iota_gamma_j']
    

    # Fit the model and get the coefficient in fewer steps
    tilde_beta_1 = sm.OLS(collapsed_df_w_shock_w_error['ln_wage_demeaned_within_iota_gamma'], sm.add_constant(collapsed_df_w_shock_w_error['ln_wage_demeaned_within_iota_m'])).fit().params[1]
    # Beta_1 measures the correlation between the deviation of my wage from my gamma average and the deviation of my wage from my miscalssified market wage. 
    tilde_beta_2 = sm.OLS(collapsed_df_w_shock_w_error['wbar_minus_mu_ig'], sm.add_constant(collapsed_df_w_shock_w_error['ln_wage_demeaned_within_iota_m'])).fit().params[1]
    bias = (theta_hat_true-eta_hat_true) * (1 - tilde_beta_1 - tilde_beta_2)
    eta_theo = eta_hat_true + bias
    eta_theoretical.append(eta_theo)
    tilde_beta_1_list.append(tilde_beta_1)
    tilde_beta_2_list.append(tilde_beta_2)
    

    # Print the results (optional, for checking)
    print(f"\nMisclassification rate: {r}")
    print(f"Estimated eta_hat: {eta_hat_e:.4f}")
    print(f"Estimated theta_hat: {theta_hat_e:.4f}")
    print(f"Theoretical eta: {eta_theo:.4f}")
    print(f"tilde beta 1: {tilde_beta_1:.4f}")
    print(f"tilde beta 2: {tilde_beta_2:.4f}")
    print(f"HHI: {hhi:.4f}")
    print(f"HHI w/ error: {hhi_w_error:.4f}")

# Create a DataFrame with the results
misclassification_ests = pd.DataFrame({
    'Misclassification_Rate': misclassification_rates,
    'Estimated_Eta': eta_estimates,
    'Estimated_Theta': theta_estimates,
    'Theoretical_Eta': eta_theoretical,
    'Tilde Beta 1': tilde_beta_1_list,
    'Tilde Beta 2': tilde_beta_2_list,
    #'Theoretical_Theta': theta_theoretical,
    'HHI': hhi_values,
    'HHI w/ error': hhi_w_error_values
})
# Create the plot

# Create the first plot for elasticity estimates
plt.figure(figsize=(12, 6))
plt.axhline(y=eta_hat_true, color='b', linestyle=':', label='True eta')
plt.plot(misclassification_ests['Misclassification_Rate'], misclassification_ests['Estimated_Eta'], 'b-o', label='Estimated eta')
plt.plot(misclassification_ests['Misclassification_Rate'], misclassification_ests['Theoretical_Eta'], color='darkorange', linestyle=':', linewidth=2.5, label='Theoretical eta')
plt.axhline(y=theta_hat_true, color='r', linestyle=':', label='True theta')
plt.plot(misclassification_ests['Misclassification_Rate'], misclassification_ests['Estimated_Theta'], 'r-o', label='Estimated theta')
plt.xlabel('Misclassification Rate')
plt.ylabel('Elasticity Estimate')
plt.title('Elasticity Estimates vs Misclassification Rate')
plt.legend()
plt.grid(True)
plt.show()




