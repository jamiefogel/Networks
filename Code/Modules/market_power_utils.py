# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 17:07:24 2024

@author: p13861161
"""


import os
import pandas as pd
import numpy as np
import time
import subprocess
import tempfile
import ast
import statsmodels.api as sm
import shutil



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
def compute_s_j_mkt(df, wagevar='wage_guess', emp_counts='ell_iota_j',jobvar='jid_masked', marketvar='gamma'):
    df['wl'] = df[wagevar] * df[emp_counts]
    df['numerator']   = df.groupby(jobvar)['wl'].transform('sum')
    df['denominator'] = df.groupby(marketvar)['wl'].transform('sum')
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

def run_stata_code(reg_df, 
                   stata_code,
                   dta_path=None,
                   results_path=None, 
                   do_file_path=None, 
                   log_file_path=None, 
                   scalar_results_path=None, 
                   stata_path=None, 
                   PRINT_STATA_LOG=False,
                   temp_dir="/home/DLIPEA/p13861161/tmp"  # fixed directory for temp files
):
   

    # Ensure we have a clean temp directory
    shutil.rmtree(temp_dir, ignore_errors=True)
    os.makedirs(temp_dir, exist_ok=True)

    temp_files = []

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

    success_indicator_path = os.path.join(temp_dir, 'stata_success.tmp')
    temp_files.append(success_indicator_path)

    try:
        reg_df.to_stata(dta_path, write_index=False)

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

        file open success using "{success_indicator_path}", write replace
        file write success "success"
        file close success
        """

        stata_code = stata_code.replace("dta_path", dta_path)
        stata_code = stata_code.replace("results_path", results_path)
        stata_code = stata_code.replace("log_file_path", log_file_path)

        if PRINT_STATA_LOG:
            print(stata_code)

        with open(do_file_path, 'w') as f:
            f.write(stata_code)

        stata_path = stata_path or r"/usr/local/stata18/stata-mp"
        process = subprocess.Popen([stata_path, "-e", "do", do_file_path],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   universal_newlines=True)

        last_position = 0

        while True:
            if process.poll() is not None:
                break

            if os.path.exists(log_file_path) and PRINT_STATA_LOG:
                with open(log_file_path, 'r') as log_file:
                    log_file.seek(last_position)
                    new_content = log_file.read()
                    if new_content:
                        print(new_content, end='')
                    last_position = log_file.tell()
            time.sleep(0.1)

        if os.path.exists(log_file_path) and PRINT_STATA_LOG:
            with open(log_file_path, 'r') as log_file:
                log_file.seek(last_position)
                remaining_content = log_file.read()
                if remaining_content:
                    print(remaining_content, end='')

        if not os.path.exists(success_indicator_path):
            with open(log_file_path, 'r') as log_file:
                log_content = log_file.read()
            raise RuntimeError(f"Stata code failed to execute successfully. Log file contents:\n{log_content}")

        results_df = pd.read_stata(results_path)

        with open(scalar_results_path, 'r') as f:
            scalar_results_str = f.read().strip()

        scalar_dict_str = scalar_results_str.split('=', 1)[1].strip()
        scalar_results = ast.literal_eval(scalar_dict_str)

        with open(do_file_path, 'r') as f:
            do_file_content = f.read()

        with open(log_file_path, 'r') as f:
            log_file_content = f.read()

        return {
            'results_df': results_df,
            'scalar_results': scalar_results,
            'do_file': do_file_content,
            'log_file': log_file_content
        }

    finally:
        if temp_dir:
            for file in temp_files:
                if os.path.exists(file):
                    os.remove(file)
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
    
    s_ij = compute_payroll_weighted_share(df, workertypevar, jobvar,         wagevar)
    s_ij = s_ij.loc[s_ij.payroll_weighted_share>0]    # Delete empty rows to save memory
    
    s_jg = compute_payroll_weighted_share(df, jobvar,        marketvar,      wagevar)
    s_jg = s_jg.set_index(jobvar, verify_integrity=True)   # This is critical to make sure the columns being summed have the same index
     
    # If we want to use Mayara's definitions
    #s_fm = compute_payroll_weighted_share(data_full, 'cnpj_raiz', 'mkt_mayara', wagevar)
    
    s_gi_hat = compute_s_MarketWorkertype(df, jobvar, workertypevar, marketvar, eta, theta)
    
    job_market_cw = df[[jobvar, marketvar]].drop_duplicates()
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

def run_two_step_estimation(reg_df_w_FEs_w_shock, estimation_strategy, delta=0, workertypevar = ['iota'], mktvar = ['gamma'], stata_or_python='Python'):
    # Step 1: Data Preparation
    reg_df2 = prepare_step1_data(reg_df_w_FEs_w_shock, estimation_strategy, workertypevar = workertypevar, mktvar = mktvar, delta = delta)
    
    # Step 1: Estimate eta_hat
    eta_hat = estimate_eta_hat(reg_df2, estimation_strategy, stata_or_python)
    
    # Step 2: Data Preparation
    reg_df3 = prepare_step2_data(reg_df2, eta_hat, estimation_strategy)
    
    # Step 2: Estimate theta_hat
    theta_hat = estimate_theta_hat(reg_df3, estimation_strategy, stata_or_python)
    
    return eta_hat, theta_hat


def prepare_step1_data(reg_df_w_FEs_w_shock, estimation_strategy, workertypevar, mktvar, delta=0):
    for var in workertypevar + mktvar:
        reg_df_w_FEs_w_shock[var] = reg_df_w_FEs_w_shock[var].astype(str)
        
    reg_df_w_FEs_w_shock['worker_mkt_id'] = reg_df_w_FEs_w_shock.groupby(workertypevar + mktvar).ngroup()
    reg_df_w_FEs_w_shock['worker_type_id'] = reg_df_w_FEs_w_shock.groupby(workertypevar).ngroup()
        
    variables = [
        'ell_iota_j_pre_shock', 'ell_iota_j_post_shock', 'wage_pre_shock', 'wage_post_shock',
        'worker_mkt_id', 'worker_type_id'
    ]
    if estimation_strategy == 'panel_iv':
        variables += ['jid_masked_shock', 'iota_gamma_shock', 'Z_j']
    reg_df2 = reg_df_w_FEs_w_shock[variables].copy()
    
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
        import pyfixest as pf
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
        import pyfixest as pf
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
        df[['s_gamma_iota','ell_iota_j']] = compute_ell(df, eta, theta)
        df['pi_iota_j'] = compute_pi(df)
        df['s_j_gamma'] = compute_s_j_mkt(df)
        df['epsilon_j'] = compute_epsilon_j(df, eta, theta)
        
        # Update wage_guess
        df['wage_guess_new'] = df['epsilon_j']/(1+df['epsilon_j']) * df['phi_iota_j_new']
        diff = np.abs(df['wage_guess_new'] - df['wage_guess']).sum()
        #diff_l = np.abs(reg_df_w_FEs_w_shock['wage_guess_new'] - reg_df_w_FEs_w_shock['wage_guess_new']).sum()
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


def compute_theoretical_eta(df, wagevar, worker_type_true, job_type_true, job_type_misclassified, eta_true, theta_true, return_betas=False):
    df['ln_wage_post_shock'] = np.log(df[wagevar])
    df['ln_wage_demeaned_within_iota_gamma'] = df['ln_wage_post_shock'] - df.groupby([worker_type_true, job_type_true])['ln_wage_post_shock'].transform('mean')
    df['ln_wage_demeaned_within_iota_m']     = df['ln_wage_post_shock'] - df.groupby([worker_type_true, job_type_misclassified])['ln_wage_post_shock'].transform('mean')
    
    df['temp2'] = np.exp( df['ln_wage_post_shock'] * (1+eta_true) )
    df['mu_iota_gamma_j'] = np.log( df.groupby([worker_type_true,job_type_true])['temp2'].transform('sum') )  / (1 + eta_true)
    df['wbar_minus_mu_ig'] = df.groupby([worker_type_true,job_type_true])['ln_wage_post_shock'].transform('mean') - df['mu_iota_gamma_j']
    
    tilde_beta_1 = sm.OLS(df['ln_wage_demeaned_within_iota_gamma'], sm.add_constant(df['ln_wage_demeaned_within_iota_m'])).fit().params.iloc[1]
    # Beta_1 measures the correlation between the deviation of my wage from my gamma average and the deviation of my wage from my miscalssified market wage. 
    tilde_beta_2 = sm.OLS(df['wbar_minus_mu_ig'], sm.add_constant(df['ln_wage_demeaned_within_iota_m'])).fit().params.iloc[1]
    bias = (theta_true-eta_true) * (1 - tilde_beta_1 - tilde_beta_2)
    eta_hat_theoretical = eta_true + bias
    if return_betas:
        return eta_hat_theoretical, tilde_beta_1, tilde_beta_2
    else:
        return eta_hat_theoretical
   

def compute_theoretical_theta(df, wagevar, worker_type_true, job_type_true, job_type_misclassified, eta_true, eta_theo, theta_true, return_betas=False):

    df['ln_wage_post_shock'] = np.log(df[wagevar])
    
    # Compute mu_iota_m. This will be collapsed to the iota-m level
    df['temp_inner_m'] =  np.exp((1+eta_theo)*df['ln_wage_post_shock'])
    df['mu_iota_m'] = np.log( (df.groupby([worker_type_true,job_type_misclassified])['temp_inner_m'].transform('sum') )**(1/(1+eta_theo)) )
    #iota_m_df =  np.log( (df.groupby([worker_type_true,job_type_misclassified])['temp_inner_m'].sum() )**(1/(1+eta_theo)) ).reset_index().rename(columns={'temp_inner_m': 'mu_iota_m'}) 
    
    # Compute mu_iota_gamma. This will remain at the iota-j level because we collapse it to the iota-m level weighting by s_iota_j_m
    df['temp_inner_g'] =  np.exp((1+eta_true)*df['ln_wage_post_shock'])
    df['mu_iota_g'] =  np.log( (df.groupby([worker_type_true,'gamma'      ])['temp_inner_g'].transform('sum') )**(1/(1+eta_true)) )
    
    df['s_iota_j_m'] = np.exp(  (1+eta_true)*(df['ln_wage_post_shock'] - df['mu_iota_m'])  )
    
    df_iota_m = df[[worker_type_true, job_type_misclassified, 'mu_iota_m']].drop_duplicates()
    uniqueness_check = df_iota_m.groupby([worker_type_true, job_type_misclassified]).size().max() == 1
    if not uniqueness_check:
        raise ValueError("The result is not uniquely identified by worker_type_true and job_type_misclassified.")
    df_iota_m['mu_iota_bar'] = df_iota_m.groupby(worker_type_true)['mu_iota_m'].transform('mean')
    df_iota_m['mu_iota_m_demeaned'] = df_iota_m['mu_iota_m'] - df_iota_m['mu_iota_bar']

    df['temp3'] = df['s_iota_j_m'] * df['mu_iota_g']
    sum_s_mu = df.groupby([worker_type_true,job_type_misclassified])['temp3'].sum().reset_index().rename(columns={'temp3':'sum_s_mu'})
    df_iota_m = df_iota_m.merge(sum_s_mu, on=[worker_type_true,job_type_misclassified], validate='1:1')
    
    # Compute the delta ("Jensen bias" term)
    df['temp4'] = np.exp((theta_true - eta_true)*df['mu_iota_g'] ) * df['s_iota_j_m']
    delta_term1 = ((1/(theta_true - eta_true)) * np.log(df.groupby([worker_type_true,job_type_misclassified])['temp4'].sum())).reset_index().rename(columns={'temp4':'delta_term1'})
    df_iota_m = df_iota_m.merge(delta_term1, on=[worker_type_true,job_type_misclassified], validate='1:1')
    df_iota_m['delta_iota_m'] = df_iota_m['delta_term1'] - df_iota_m['sum_s_mu']
    
    coef1 = sm.OLS(df_iota_m['sum_s_mu'], sm.add_constant(df_iota_m['mu_iota_m_demeaned'])).fit().params.iloc[1]
    coef2 = sm.OLS(df_iota_m['delta_iota_m'], sm.add_constant(df_iota_m['mu_iota_m_demeaned'])).fit().params.iloc[1]
    bias = (eta_true - theta_true) * ( 1 - coef1 - coef2)
    theta_hat_theoretical  = theta_true + bias
    if return_betas:
        return theta_hat_theoretical, coef1, coef2
    else:
        return theta_hat_theoretical
    