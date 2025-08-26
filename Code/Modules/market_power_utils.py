#!/usr/bin/env python3
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
from sklearn.metrics import adjusted_rand_score
from linearmodels.iv import IV2SLS



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
    df['denominator'] = df.groupby(['jid'])['ell_iota_j'].transform('sum')
    return df['ell_iota_j'] / df['denominator']

# Equation 48 - Job j's payrolls share of market gamma (summing across all iotas)
def compute_s_j_mkt(df, wagevar='wage_guess', emp_counts='ell_iota_j',jobvar='jid', marketvar='gamma'):
    df['wl'] = df[wagevar] * df[emp_counts]
    df['numerator']   = df.groupby(jobvar)['wl'].transform('sum')
    df['denominator'] = df.groupby(marketvar)['wl'].transform('sum')
    return df['numerator'] / df['denominator']
    
# Equation 49: compute markdown
def compute_epsilon_j(df, eta, theta):
    df['pi_times_s'] = df['pi_iota_j'] * df['s_gamma_iota']
    df['weighted_share'] = df.groupby('jid')['pi_times_s'].transform('sum')
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

def generate_shocks(df, alpha=2, beta=5, delta=0, k=0.5, l=0.5):
    '''
    Parameters
    ----------
    df : data frame
        Worker-level data frame that is wide on year (pre vs. post)
    alpha : numeric, optional
        First parameter of beta distribution used to generate Bernoulli parameter p_gamma (probability instrument is 1). The default is 2.
    beta : numeric, optional
        Second parameter of beta distribution used to generate Bernoulli parameter p_gamma (probability instrument is 1). The default is 5.
    delta : numeric, optional
        First stage coefficient on instrument. The default is 0.
    k : numeric, optional
        Dependence between instrument and iota-gamma fixed effects. All correlation comes at the gamma level. The default is 0.5.
    l : numeric, optional
        Controls the amount of noise in the fixed effect that is independent of the instrument. The default is 0.5.

    Returns
    -------
    df : TYPE
        DESCRIPTION.

    '''
    gamma_jid_cw = df[['gamma','jid']].drop_duplicates()
    # Draw a Bernoulli parameter for each gamma
    p_gamma = {gamma: np.random.beta(alpha, beta, 1)[0] for gamma in df['gamma'].unique()} # This is phi_j
    gamma_jid_cw['p_gamma'] = gamma_jid_cw['gamma'].map(p_gamma)
    df['p_gamma'] = df['gamma'].map(p_gamma)
    
    # Draw Bernoulli samples for each row using the probability in p_gamma column
    gamma_jid_cw['Z_j'] = np.random.binomial(1, gamma_jid_cw['p_gamma'])
    df = df.merge(gamma_jid_cw[['jid','Z_j']], on='jid', how='inner', validate='m:1', indicator=False)
  
    # Generate a random shock 
    # This is called "xi" on Overleaf
    jid_shock = {jid: np.random.random() for jid in df['jid'].unique()} # This is phi_j
    df['jid_shock'] = df['jid'].map(jid_shock)
    # This is called "zeta" on Overleaf. XX maybe change to normal at some point? We say normal in the paper.
    iota_gamma_shock = {iota_gamma: np.random.random()*l for iota_gamma in df['iota_gamma'].unique()}
    df['iota_gamma_shock'] = df['iota_gamma'].map(iota_gamma_shock)
    # By adding k * p_gamma we allow for correlation between the zeta_ig shock and the instrument
    df['iota_gamma_shock'] = df['iota_gamma_shock'] + k*df['p_gamma']

    df['phi_iota_j_new'] = np.exp( \
        df['iota_gamma_fes']   + df['jid_fes'] + \
        df['iota_gamma_shock'] + df['jid_shock'] + \
        delta * df['Z_j']) 
    
    return df

def run_two_step_estimation(reg_df_w_FEs_w_shock, estimation_strategy, delta=0, workertypevar = ['iota'], mktvar = ['gamma'], stata_or_python='Python'):
    # Step 1: Data Preparation
    reg_df2 = prepare_step1_data(reg_df_w_FEs_w_shock, estimation_strategy, workertypevar = workertypevar, mktvar = mktvar, delta = delta)
    
    # Step 1: Estimate eta_hat
    eta_hat, eta_first_stage_f = estimate_eta_hat(reg_df2, estimation_strategy, stata_or_python)
    
    # Step 2: Data Preparation
    reg_df3 = prepare_step2_data(reg_df2, eta_hat, estimation_strategy)
    
    # Step 2: Estimate theta_hat
    theta_hat, theta_first_stage_f = estimate_theta_hat(reg_df3, estimation_strategy, stata_or_python)
    
    return eta_hat, theta_hat, eta_first_stage_f, theta_first_stage_f


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
        variables += ['jid_shock', 'iota_gamma_shock', 'Z_j']
    reg_df2 = reg_df_w_FEs_w_shock[variables].copy()
    
    for var in ['ell_iota_j_pre_shock', 'ell_iota_j_post_shock', 'wage_pre_shock', 'wage_post_shock']:
        reg_df2['ln_' + var]   = np.log(reg_df2[var])
    
    for var in ['ell_iota_j', 'wage']:
        reg_df2['diff_ln_' + var] = reg_df2['ln_' + var + '_post_shock'] - reg_df2['ln_' + var + '_pre_shock']
    
    # Instrument for IV estimation
    if estimation_strategy == 'panel_iv':
        if delta == 0:
            reg_df2['shock_iv'] = reg_df2['jid_shock'] + reg_df2['iota_gamma_shock']
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
        first_stage_f = np.nan
    if stata_or_python=='Python':
        import pyfixest as pf
        if regression_type=='ols':
            fit = pf.feols(f"{dependent_var} ~ {independent_var} | {absorb_var}", data=reg_df2)
            first_stage_f = np.nan
            eta_hat = fit.coef().loc[independent_var] - 1
        elif regression_type=='iv':
            g = reg_df2.groupby(absorb_var)
            def demean(s): return s - g[s.name].transform('mean')
            y_t = demean(reg_df2[dependent_var]).rename(dependent_var)
            x_t = demean(reg_df2[independent_var]).rename(independent_var)
            z_t = demean(reg_df2[instrument_var]).rename(instrument_var)
            res = IV2SLS(y_t, exog=None, endog=x_t, instruments=z_t).fit(cov_type='robust')
            eta_hat = res.params[independent_var] - 1
            first_stage_f = res.first_stage.diagnostics.loc[independent_var, "f.stat"]
    return eta_hat, first_stage_f

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
        first_stage_f = np.nan
    if stata_or_python=='Python':
        import pyfixest as pf
        if regression_type=='ols':
            fit = pf.feols(f"{dependent_var} ~ {independent_var} | {absorb_var}", data=reg_df3)
            theta_hat = fit.coef().loc[independent_var] - 1
            first_stage_f = np.nan
        elif regression_type=='iv':
            g = reg_df3.groupby(absorb_var)
            def demean(s): return s - g[s.name].transform('mean')
            y_t = demean(reg_df3[dependent_var]).rename(dependent_var)
            x_t = demean(reg_df3[independent_var]).rename(independent_var)
            z_t = demean(reg_df3[instrument_var]).rename(instrument_var)
            res = IV2SLS(y_t, exog=None, endog=x_t, instruments=z_t).fit(cov_type='robust')
            first_stage_f = res.first_stage.diagnostics.loc[independent_var, "f.stat"]
            theta_hat = res.params[independent_var] - 1
    return theta_hat, first_stage_f

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

def find_equilibrium(df, eta, theta, tol=1e-5, min_iter=10, max_iter=100):
    diff_mean = tol + 1
    iter_count = 0
    while iter_count < min_iter or (diff_mean > tol and iter_count < max_iter) :
        df[['s_gamma_iota','ell_iota_j']] = compute_ell(df, eta, theta)
        df['pi_iota_j'] = compute_pi(df)
        df['s_j_gamma'] = compute_s_j_mkt(df)
        df['epsilon_j'] = compute_epsilon_j(df, eta, theta)
        
        # Update wage_guess
        df['wage_guess_new'] = df['epsilon_j']/(1+df['epsilon_j']) * df['phi_iota_j_new']
        diff = np.abs(df['wage_guess_new'] - df['wage_guess']).sum()
        diff_mean = np.abs(df['wage_guess_new'] - df['wage_guess']).mean()
        #diff_l = np.abs(reg_df_w_FEs_w_shock['wage_guess_new'] - reg_df_w_FEs_w_shock['wage_guess_new']).sum()
        df['wage_guess'] = df['wage_guess_new']
        if True: #iter%10==0:
            print(iter_count)
            print(diff)
            print(diff_mean)
            #print(df['wage_guess'].describe())
        iter_count += 1
    return df['wage_guess']

def misclassify_jobs_random(df, misclassification_rate):
    # Create a copy of the dataframe to avoid modifying the original
    df_misclassified = df.copy()
    
    # Calculate the probability distribution of gammas
    gamma_probs = df_misclassified['gamma'].value_counts(normalize=True)
    
    # Get unique jid values
    unique_jids = df_misclassified['jid'].unique()
    
    # Generate new gammas for all unique jids
    new_gammas = np.random.choice(gamma_probs.index, size=len(unique_jids), p=gamma_probs.values)
    
    # Create a mask for jids to be misclassified
    misclassify_mask = np.random.random(len(unique_jids)) < misclassification_rate
    
    # Create a dictionary mapping jids to their new gammas (only for misclassified jids)
    new_gamma_dict = dict(zip(unique_jids[misclassify_mask], new_gammas[misclassify_mask]))
    
    # Apply new gammas to misclassified jids
    df_misclassified['gamma_error'] = df_misclassified['jid'].map(new_gamma_dict).fillna(df_misclassified['gamma'])
    
    return df_misclassified['gamma_error']


def misclassify_jobs_by_within_gamma_quantile(
    df: pd.DataFrame,
    rank_variable: str,
    frac_reassign: float = 0.10,
    job_ids_to_reassign: np.ndarray | list | None = None,
    random_state: int | None = None,
    ascending_gamma_rank: bool = True,
    move_distance: int = 1,
    edge_behavior: str = "clip",
) -> pd.DataFrame:
    """
    Reassign a fraction (or a specified set) of jobs to adjacent markets (gammas)
    based on each job's within-gamma Z_j percentile.

    Parameters
    ----------
    df : DataFrame
        Must contain columns: 'jid', 'gamma', rank_variable.
    frac_reassign : float, default 0.10
        Fraction of jobs to reassign (ignored if job_ids_to_reassign is provided).
    rank_variable : dataframe column name
        Name of variable on which to rank gammas 
    job_ids_to_reassign : array-like or None
        Explicit set of jid values to reassign. If provided, overrides frac_reassign.
    random_state : int or None
        RNG seed for reproducibility.
    ascending_gamma_rank : bool, default True
        How to rank gammas by their mean rank_variable:
        - True  => rank 1 = lowest mean rank_variable, max rank = highest mean rank_variable
        - False => rank 1 = highest mean rank_variable, max rank = lowest mean rank_variable
        “Higher-ranked” means moving toward larger rank numbers in this convention.
    edge_behavior : {"clip","stay"}, default "clip"
        What to do if a job is at the top/bottom gamma and tries to move beyond:
        - "clip": clamp to the edge gamma (rank stays within [1, G])
        - "stay": cancel the move (job keeps its current gamma)

    Returns
    -------
    DataFrame
        Original columns plus:
         - 'gamma_rank': integer rank of the job's current gamma
         - 'within_gamma_pct': job's within-gamma percentile in [0,1]
         - 'move_dir': drawn in {-1,+1} for reassigned jobs, else 0
         - 'gamma_new': the post-reassignment gamma label
    """
    rng = np.random.default_rng(random_state)
    out = df.copy()

    # 1) Gamma means and *dense* ranks 1..G via rank (no sort+enumerate)
    p_gamma_hat = out.groupby("gamma", observed=True)[rank_variable].mean()
    gamma_rank_map = p_gamma_hat.rank(method="dense", ascending=True).astype(int)
    out["gamma_rank"] = out["gamma"].map(gamma_rank_map).astype(int)
    out["p_gamma_hat"] = out["gamma"].map(p_gamma_hat)
    G = int(gamma_rank_map.max())

    # Build dictionary that maps each gamma rank to the gamma value
    rank_to_gamma = {int(r): g for g, r in gamma_rank_map.items()}

    # 2) Within-gamma percentile (0..1, ties averaged), fully vectorized
    out["within_gamma_pct"] = out.groupby("gamma", observed=True)[rank_variable].rank(method="average",pct=True) 

    # 3) Choose which jobs to reassign
    if job_ids_to_reassign is not None:
        reassigned = out["jid"].isin(job_ids_to_reassign).to_numpy()
    else:
        k = int(round(frac_reassign * len(out)))
        idx = rng.choice(out.index.to_numpy(), size=k, replace=False) if k > 0 else np.array([], dtype=int)
        reassigned = out.index.isin(idx) # Vector indicating whether each jid is to be reassigned

    # 4) Direction: P(+1)=percentile, P(-1)=1-p (use binomial for vectorization)
    move = np.zeros(len(out), dtype=int)
    p = out.loc[reassigned,'within_gamma_pct']
    move[reassigned] = move_distance * ( 2 * rng.binomial(1, p) - 1)   # {0,1} -> {-1,+1}
    out["move_dir"] = move

    # 5) Move one rank up/down with edge handling
    target_rank = out["gamma_rank"] + out["move_dir"]
    if edge_behavior == "clip":
        target_rank = target_rank.clip(1, G)
    elif edge_behavior == "stay":
        overshoot = (target_rank < 1) | (target_rank > G)
        target_rank = target_rank.where(~overshoot, out["gamma_rank"])
    else:
        raise ValueError("edge_behavior must be 'clip' or 'stay'.")

    out["gamma_new"] = target_rank.map(rank_to_gamma)
    return out["gamma_new"]




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
    
    y = df['ln_wage_demeaned_within_iota_gamma'].astype('float64')
    x = df['ln_wage_demeaned_within_iota_m'].astype('float64')
    tilde_beta_1 = sm.OLS(y, sm.add_constant(x)).fit().params.iloc[1]
    del y, x
    # Beta_1 measures the correlation between the deviation of my wage from my gamma average and the deviation of my wage from my miscalssified market wage. 
    y = df['wbar_minus_mu_ig'].astype('float64')
    x = df['ln_wage_demeaned_within_iota_m'].astype('float64')
    tilde_beta_2 = sm.OLS(y, sm.add_constant(x)).fit().params.iloc[1]
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
    
    y = df_iota_m['sum_s_mu'].astype('float64')
    x = df_iota_m['mu_iota_m_demeaned'].astype('float64')
    coef1 = sm.OLS(y, sm.add_constant(x)).fit().params.iloc[1]
    del x, y
    y = df_iota_m['delta_iota_m'].astype('float64')
    x = df_iota_m['mu_iota_m_demeaned'].astype('float64')
    coef2 = sm.OLS(y, sm.add_constant(x)).fit().params.iloc[1]
    bias = (eta_true - theta_true) * ( 1 - coef1 - coef2)
    theta_hat_theoretical  = theta_true + bias
    if return_betas:
        return theta_hat_theoretical, coef1, coef2
    else:
        return theta_hat_theoretical
    
    
    
def compute_rand_index(df, def1_vars, def2_vars):
    """
    Compute the Adjusted Rand Index between two market definitions.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe containing the data.
    def1_vars : str or list of str
        Variable(s) defining the first market definition.
    def2_vars : str or list of str
        Variable(s) defining the second market definition.
    
    Returns
    -------
    float
        Adjusted Rand Index between the two definitions.
    """
    
    # Ensure inputs are lists
    if isinstance(def1_vars, str):
        def1_vars = [def1_vars]
    if isinstance(def2_vars, str):
        def2_vars = [def2_vars]
    
    # Create group labels by concatenating variables
    labels1 = df[def1_vars].astype(str).agg("_".join, axis=1)
    labels2 = df[def2_vars].astype(str).agg("_".join, axis=1)
    
    # Compute adjusted Rand score
    return adjusted_rand_score(labels1, labels2)

    
  

def load_and_prepare_data(root, eta_bhm, theta_bhm):
    # Pull region codes
    region_codes = pd.read_csv(root + '/Data/raw/munic_microregion_rm.csv', encoding='latin1')

    ################################
    # Load earnings panel data 
     ################################
     
    _3states = '_3states'
    dfs = []
    for year in [1991,1997]:
        #export_filename = os.path.join(MAYARA_OUTPUT_DIR, f"rais{year}{_3states}.parquet")
        export_filename = os.path.join(root, '../market_power/Data/dump/', f"rais_for_earnings_premia{year}_gamma{_3states}.parquet")
        year_df = pd.read_parquet(export_filename)
        # Merge on cbo and municipality from raw data
        addl_occ_ind_geo_filename = os.path.join(root, '../market_power/Data/dump/', f"rais{year}{_3states}.parquet")
        addl_occ_ind_geo = pd.read_parquet(addl_occ_ind_geo_filename, columns = ['fakeid_worker', 'fakeid_firm', 'municipality','cbo','occ4'])
        addl_occ_ind_geo = addl_occ_ind_geo.loc[(addl_occ_ind_geo.fakeid_worker.notna()) & (addl_occ_ind_geo.fakeid_firm.notna())]
        year_df = year_df.merge(addl_occ_ind_geo, on=['fakeid_worker','fakeid_firm'], how='left', validate='1:1', indicator=False)
        #cnae95_master_file = os.path.join(root, '../market_power/Data/dump/' , f"rais_firm_cnae95_master_plus{_3states}.parquet")
        #df_cnae95 = pd.read_parquet(cnae95_master_file).drop_duplicates()
        #year_df = year_df.merge(df_cnae95[["fakeid_firm", "cnae95"]], on="fakeid_firm", how="inner")
        del addl_occ_ind_geo
        dfs.append(year_df)
    
    data_full = pd.concat(dfs, ignore_index=True)
    # XX Why are there a non-trivial number of missing fakeid_worker values here?
    # year_df['mi'] = year_df.fakeid_worker.isna()
    #pd.crosstab(year_df['mi'] ,year_df._merge_worker_gamma_pre)
    # _merge_worker_gamma_pre  left_only  right_only     both
    # mi                                                     
    # False                      6701856           0  4991899
    # True                       1973966     2830512   255850
    del year_df
    
    iotas = pd.read_csv(root + '../market_power/Data/dump/sbm_output/model_sbm_1986_1991_wblocks.csv')[['wid','worker_blocks_level_0']].rename(columns={'worker_blocks_level_0':'iota'})
    
    data_full = data_full.loc[data_full.fakeid_worker.notna()]
    data_full['wid'] = data_full['fakeid_worker'].astype('Int64')
    data_full = data_full.merge(iotas, on='wid', how='left',validate='m:1', indicator='_merge_iotas')
    # Temporarily just set all iotas equal to 1 since we aren't using iotas in estimation or in the theory of the paper
    #data_full['iota'] = 1
    
    #usecols = ['wid', 'jid', 'year', 'iota', 'gamma', 'cnpj_raiz', 'id_estab',
    #           'real_hrly_wage_dec', 'ln_real_hrly_wage_dec', 'codemun', 'cbo942d', 'occ2_first',
    #           'code_meso', 'occ2Xmeso', 'occ2Xmeso_first']
    data_full['real_hrly_wage_dec'] = data_full['earningsdecmw']
    data_full['ln_real_hrly_wage_dec'] = np.log(data_full['real_hrly_wage_dec'])

    # Create variable for Mayara's market definitions
    data_full['mkt_mayara'] = data_full.groupby(['mmc', 'cbo942d']).ngroup()

    # Compute Markdowns and merge on
    markdown_w_iota = compute_markdowns_w_iota(data_full, 'real_hrly_wage_dec', 'jid', 'gamma', 'iota', eta_bhm, theta_bhm)
    markdown_w_iota = pd.DataFrame(markdown_w_iota).reset_index().rename(columns={0:'markdown_w_iota'})
    data_full = data_full.merge(markdown_w_iota, on='jid', how='outer', validate='m:1')

    reg_df = data_full[['wid', 'jid', 'iota', 'gamma', 'cbo942d', 'mmc', 'mkt_mayara',
                        'ln_real_hrly_wage_dec', 'markdown_w_iota','cnae95', 'municipality','cbo','occ4']].copy()
    reg_df['y_tilde'] = reg_df.ln_real_hrly_wage_dec + np.log(reg_df.markdown_w_iota)
    reg_df['iota_gamma'] = reg_df.iota.astype(str) + '_' + reg_df.gamma.astype(str)

    # Trim approximately top 1% of wages
    
    reg_df = reg_df.loc[reg_df.ln_real_hrly_wage_dec < reg_df.ln_real_hrly_wage_dec.quantile(0.99)]

    # Save dataframe as parquet file
    reg_df.to_parquet(root + '../market_power/Data/dump/MarketPower_reghdfe_data.parquet')

    return reg_df

def mode_or_first(series):
    mode_values = series.mode()
    return mode_values.iloc[0] if not mode_values.empty else np.nan

# Estimate job and iota-gamma FEs
def estimate_fixed_effects(reg_df, stata_or_python):
    
    if stata_or_python == "Stata":
        # Your Stata code as a Python string
        stata_code = """
        clear
        set more off
        log using "log_file_path", replace
        
        use "dta_path", clear
        reghdfe y_tilde, absorb(jid_fes=jid iota_gamma_fes=iota_gamma, savefe) residuals(resid)
        
        save "results_path", replace
        """

        results_reg1 = run_stata_code(reg_df, stata_code)
        reg_df_w_FEs = results_reg1['results_df']
    elif stata_or_python == "Python":
        import pyfixest as pf
        # Perform the regression with fixed effects for 'jid' and 'iota_gamma'
        fit_ols = pf.feols("y_tilde ~ 1 | jid + iota_gamma", data=reg_df)
        # Extract the fixed effects as a dictionary
        fixed_effects = fit_ols.fixef()
        # Loop through each fixed effect group and add it to the original DataFrame
        for fe_var, fe_values in fixed_effects.items():
            fe_varname = fe_var[fe_var.find("(") + 1: fe_var.find(")")]
            fe_df = pd.DataFrame(list(fixed_effects[fe_var].items()), columns=[fe_varname, f'{fe_varname}_fes'])
            fe_df[fe_varname] = fe_df[fe_varname].astype(reg_df[fe_varname].dtype)
            # Merge the fixed effect estimates back to the original DataFrame
            reg_df = reg_df.merge(fe_df, on=fe_varname, how='left', validate='m:1', indicator=f'_merge_{fe_varname}')
        reg_df_w_FEs = reg_df.copy()
    else:
        raise ValueError("Invalid value for 'stata_or_python'. Should be 'Stata' or 'Python'.")
        
    reg_df_w_FEs = reg_df_w_FEs.groupby(['iota', 'gamma', 'iota_gamma', 'jid']).agg({
        'jid_fes': 'first',          # These don't vary within the group, so we can take the first value
        'iota_gamma_fes': 'first',          # These don't vary within the group, so we can take the first value
        'markdown_w_iota': 'first',         # These don't vary within the group, so we can take the first value
        'cbo942d': 'first',                    # These don't vary within the group, so we can take the first value
        'cbo': 'first',                    # These don't vary within the group, so we can take the first value
        'occ4': 'first',                    # These don't vary within the group, so we can take the first value
        'cnae95': 'first',                    # These don't vary within the group, so we can take the first value
        'mmc': mode_or_first,        # Use custom function to handle ties
        'ln_real_hrly_wage_dec': 'mean',    # Average of log earnings within the group
        'wid': 'count'               # Count of rows in this group
    }).reset_index()

    
    # Rename the count column to something more descriptive
    reg_df_w_FEs = reg_df_w_FEs.rename(columns={'wid': 'iota_gamma_jid_count'})

    return reg_df_w_FEs


def generate_shocks_and_find_equilibrium(reg_df_w_FEs, alpha, beta, delta, eta, theta):
    reg_df_w_FEs_w_shock = generate_shocks(reg_df_w_FEs, alpha=alpha, beta=beta, delta=delta)
    
    reg_df_w_FEs_w_shock['wage_guess_initial'] = reg_df_w_FEs_w_shock['markdown_w_iota'] * reg_df_w_FEs_w_shock['phi_iota_j_new']
    reg_df_w_FEs_w_shock['wage_guess'] = reg_df_w_FEs_w_shock['wage_guess_initial']
    reg_df_w_FEs_w_shock['iota_count'] = reg_df_w_FEs_w_shock.groupby('iota')['iota_gamma_jid_count'].transform('sum')

    # Next steps:
    #   1. Compute ell_iota_j following eq (45)
    #   2. Then iterate through 45-50
    #   3. Iterate until ell and w stabilize

    reg_df_w_FEs_w_shock['real_hrly_wage_dec'] = np.exp(reg_df_w_FEs_w_shock['ln_real_hrly_wage_dec'])

    # Restricting to jid-iotas with non-missing wages.
    reg_df_w_FEs_w_shock = reg_df_w_FEs_w_shock.loc[reg_df_w_FEs_w_shock['wage_guess'].notna()]

    reg_df_w_FEs_w_shock['wage_post_shock'] = find_equilibrium(reg_df_w_FEs_w_shock, eta, theta)

    reg_df_w_FEs_w_shock.rename(columns={
        'iota_gamma_jid_count': 'ell_iota_j_pre_shock',
        'ell_iota_j': 'ell_iota_j_post_shock',
        'real_hrly_wage_dec': 'wage_pre_shock'
    }, inplace=True)

    return reg_df_w_FEs_w_shock

    




def run_estimations_for_combinations(data, combinations, stata_or_python='Python', ari_var=None):
    """
    Runs estimations for each combination of worker type, market definition, estimation type, and delta.
    
    Parameters:
    - data: pandas DataFrame containing the necessary data for estimations.
    - combinations: list of tuples, where each tuple is (workertypevar, mktvar, est_type, delta).
      - workertypevar: list of strings representing worker type variables.
      - mktvar: list of strings representing market variables.
      - est_type: string representing the estimation type (e.g., 'panel_iv', 'panel_ols', 'ols').
      - delta: float or None, delta parameter to pass to run_two_step_estimation.
    - stata_or_python: string, either 'Stata' or 'Python'. Default is 'Python'.
    
    Returns:
    - estimates_df: pandas DataFrame containing the estimation results.
    """
    estimates_list = []
    
    for combo in combinations:
        wkr, mkt, est_type, delta = combo
        # Convert the lists to strings for better readability
        wkr_key = ','.join(wkr)
        mkt_key = ','.join(mkt)
        
        # Run estimation
        eta_hat, theta_hat, eta_first_stage_f, theta_first_stage_f = run_two_step_estimation(
            data,
            est_type,
            delta=delta,
            workertypevar=wkr,
            mktvar=mkt,
            stata_or_python=stata_or_python
        )
        if ari_var is not None:
            ari = compute_rand_index(data, ari_var, mkt)
        else:
            ari = None
        # Append the result as a record to the list
        estimates_list.append({
            'WORKERTYPE': wkr_key,
            'MKT': mkt_key,
            'EST_TYPE': est_type,
            'DELTA': delta,
            'ETA_HAT': eta_hat,
            'THETA_HAT': theta_hat,
            'AdjRandIdx': ari
        })
        print(f'WORKERTYPE = {wkr}, MKT = {mkt}, EST_TYPE = {est_type}, DELTA = {delta}, eta = {eta_hat}, theta = {theta_hat}')
    
    # Convert the list of records to a DataFrame
    estimates_df = pd.DataFrame(estimates_list)
  
def produce_ari_table(df, writefile : str, mkts: list):
    # --- which markets (use the ones shown in your table, in that order) ---
    
    # --- helpers ---
    def parse_mkt(s):  # split "cbo,mmc" -> ['cbo','mmc']
        return [t.strip() for t in s.split(',')]
    
    # labels: long for rows, short for columns (good for slides)
    full_map = {
        'gamma': r'$\gamma$', 'cbo942d': 'Occupation (2-digit)', 'mmc': 'Micro Region',
        'cbo': 'Occupation (5-digit)', 'occ4': 'Occupation (4-digit)', 'cnae95': 'Industry (5-digit)'
    }
    abbr_map = {
        'gamma': r'$\gamma$', 'cbo942d': 'Occ2', 'mmc': 'Micro. R',
        'cbo': 'Occ5', 'occ4': 'Occ4', 'cnae95': 'Ind5'
    }
    
    def fmt_label(s, mp, joiner=r' $\times$ '):
        return joiner.join(mp.get(t, t) for t in parse_mkt(s))
    
    def fmt_label_wrapped(s):  # wrap long row labels over 3 lines if needed
        return r' \\ $\times$ \\ '.join(full_map.get(t, t) for t in parse_mkt(s))
    
    # --- build pairwise ARI matrix ---
    n = len(mkts)
    A = pd.DataFrame(np.eye(n), index=mkts, columns=mkts, dtype=float)
    for i in range(n):
        for j in range(i):  # fill lower triangle (and mirror)
            li, lj = parse_mkt(mkts[i]), parse_mkt(mkts[j])
            val = compute_rand_index(df, li, lj)
            A.iat[i, j] = A.iat[j, i] = val
    
    # --- make a readable lower-triangle table for slides ---
    #   • columns: short labels  • rows: long, wrapped labels  • hide upper triangle
    A_disp = A.copy()
    A_disp.columns = [fmt_label(c, abbr_map) for c in A_disp.columns]
    A_disp.index   = [fmt_label_wrapped(r)   for r in A_disp.index]
    
    A_lt = A_disp.mask(np.triu(np.ones_like(A_disp, dtype=bool), k=1))  # NaN upper triangle
    A_str = A_lt.applymap(lambda x: "" if pd.isna(x) else f"{x:.3f}").rename_axis(None, axis=1)
    
    latex = A_str.to_latex(
        index=True, escape=False, na_rep="",
        column_format="l" + "r"*A_str.shape[1]  # tight numeric columns
    )
    
    # optional legend to explain abbreviations on the slide
    legend = "Legend: MMC=Micro Region, Ind5=Industry (5-digit), Occ5=Occupation (5-digit), Occ4=Occupation (4-digit), Occ2=Occupation (2-digit)."
    latex += f"\n% {legend}\n"
    
    # write to file
    with open(writefile, "w") as f:
        f.write(latex)
    
    return A
