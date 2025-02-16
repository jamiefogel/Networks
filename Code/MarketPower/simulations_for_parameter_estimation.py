#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on August 13 2024
- Adapted from NetworksGit/Code/MarketPower/do_all_marketpower.py
- Goal is to delete unnecessary code to focus on simply computing distributions of HHIs and markdowns across different market definitions

@author: jfogel
"""


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from config import homedir, os_name, root, rais, figuredir

#if os_name=='Windows':
#    stata_or_python = "Stata"
#elif os_name=='Linux':
#    stata_or_python = "Python"
stata_or_python = "Stata"

from market_power_utils import (
    run_stata_code,
    compute_markdowns_w_iota,
    generate_shocks,
    run_two_step_estimation,
    find_equilibrium,
    introduce_misclassification_by_jid,
    compute_market_hhi,
    compute_theoretical_eta, 
    compute_theoretical_theta,
    compute_s_j_mkt
)


inv_eta_mayara   = 0.985
inv_theta_mayara = 1.257

eta_mayara = 1/inv_eta_mayara
theta_mayara = 1/inv_theta_mayara

eta_bhm = 7.14
theta_bhm = .45

np.random.seed(734)


def load_and_prepare_data(root, eta_bhm, theta_bhm):
    # Pull region codes
    region_codes = pd.read_csv(root + '/Data/raw/munic_microregion_rm.csv', encoding='latin1')
    muni_micro_cw = pd.DataFrame({'code_micro': region_codes.code_micro, 'codemun': region_codes.code_munic // 10})

    # Load earnings panel data
    mle_data_filename = root + "Data/derived/earnings_panel/panel_3states_2009to2011_level_0.csv"

    usecols = ['wid_masked', 'jid_masked', 'year', 'iota', 'gamma', 'cnpj_raiz', 'id_estab',
               'real_hrly_wage_dec', 'ln_real_hrly_wage_dec', 'codemun', 'occ2', 'occ2_first',
               'code_meso', 'occ2Xmeso', 'occ2Xmeso_first']

    data_full = pd.read_csv(mle_data_filename, usecols=usecols)
    data_full = data_full.loc[(data_full.iota != -1) & (data_full.gamma != -1) & (data_full.jid_masked != -1)]
    data_full = data_full.merge(muni_micro_cw, on='codemun', how='left', validate='m:1', indicator='_merge')

    # Create variable for Mayara's market definitions
    data_full['mkt_mayara'] = data_full.groupby(['occ2', 'code_micro']).ngroup()

    # Compute Markdowns and merge on
    markdown_w_iota = compute_markdowns_w_iota(data_full, 'real_hrly_wage_dec', 'jid_masked', 'gamma', 'iota', eta_bhm, theta_bhm)
    markdown_w_iota = pd.DataFrame(markdown_w_iota).reset_index().rename(columns={0:'markdown_w_iota'})
    data_full = data_full.merge(markdown_w_iota, on='jid_masked', how='outer', validate='m:1')

    reg_df = data_full[['wid_masked', 'jid_masked', 'iota', 'gamma', 'occ2', 'code_micro', 'mkt_mayara',
                        'ln_real_hrly_wage_dec', 'markdown_w_iota']].copy()
    reg_df['y_tilde'] = reg_df.ln_real_hrly_wage_dec + np.log(reg_df.markdown_w_iota)
    reg_df['iota_gamma'] = reg_df.iota.astype(str) + '_' + reg_df.gamma.astype(str)

    # Trim approximately top 1% of wages
    reg_df = reg_df.loc[reg_df.ln_real_hrly_wage_dec < 5]

    # Save dataframe as parquet file
    reg_df.to_parquet(root + 'Data/derived/MarketPower_reghdfe_data.parquet')

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
        reghdfe y_tilde, absorb(jid_masked_fes=jid_masked iota_gamma_fes=iota_gamma, savefe) residuals(resid)
        
        save "results_path", replace
        """

        results_reg1 = run_stata_code(reg_df, stata_code)
        reg_df_w_FEs = results_reg1['results_df']
    elif stata_or_python == "Python":
        import pyfixest as pf
        # Perform the regression with fixed effects for 'jid_masked' and 'iota_gamma'
        fit_ols = pf.feols("y_tilde ~ 1 | jid_masked + iota_gamma", data=reg_df)
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
        
    reg_df_w_FEs = reg_df_w_FEs.groupby(['iota', 'gamma', 'iota_gamma', 'jid_masked']).agg({
        'jid_masked_fes': 'first',          # These don't vary within the group, so we can take the first value
        'iota_gamma_fes': 'first',          # These don't vary within the group, so we can take the first value
        'markdown_w_iota': 'first',         # These don't vary within the group, so we can take the first value
        'occ2': 'first',                    # These don't vary within the group, so we can take the first value
        'code_micro': mode_or_first,        # Use custom function to handle ties
        'ln_real_hrly_wage_dec': 'mean',    # Average of log earnings within the group
        'wid_masked': 'count'               # Count of rows in this group
    }).reset_index()

    
    # Rename the count column to something more descriptive
    reg_df_w_FEs = reg_df_w_FEs.rename(columns={'wid_masked': 'iota_gamma_jid_count'})

    return reg_df_w_FEs


def generate_shocks_and_find_equilibrium(reg_df_w_FEs, delta, eta, theta):
    reg_df_w_FEs_w_shock = generate_shocks(reg_df_w_FEs, delta=delta)
    
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


def misclassification_experiment(reg_df_w_FEs_w_shock, stata_or_python):
    # First, get the true estimates from the correctly classified data
    eta_hat_true, theta_hat_true = run_two_step_estimation(reg_df_w_FEs_w_shock, 'ols', delta=0, workertypevar=['iota'], mktvar=['gamma'], stata_or_python=stata_or_python)

    print(f"True eta_hat: {eta_hat_true}")
    print(f"True theta_hat: {theta_hat_true}")

    # Create lists to store our results
    misclassification_rates = np.arange(0, 1.1, 0.1)
    eta_estimates = []
    theta_estimates = []
    eta_theoretical = []
    theta_theoretical = []
    beta_1_eta_list = []
    beta_2_eta_list = []
    beta_1_theta_list = []
    beta_2_theta_list = []
    hhi_values = []
    hhi_w_error_values = []

    # Loop over misclassification rates
    for r in misclassification_rates:
        reg_df_w_FEs_w_shock_w_error = introduce_misclassification_by_jid(reg_df_w_FEs_w_shock, r)
        eta_hat_e, theta_hat_e = run_two_step_estimation(reg_df_w_FEs_w_shock_w_error, 'ols', delta=0,
                                                         workertypevar=['iota'], mktvar=['gamma_error'], stata_or_python=stata_or_python)

        eta_estimates.append(eta_hat_e)
        theta_estimates.append(theta_hat_e)

        # Calculate HHI
        hhi = compute_market_hhi(reg_df_w_FEs_w_shock_w_error, market_col='gamma')
        hhi_w_error = compute_market_hhi(reg_df_w_FEs_w_shock_w_error, market_col='gamma_error')

        hhi_values.append(hhi)
        hhi_w_error_values.append(hhi_w_error)

        # Calculate theoretical elasticities
        eta_theo, beta_1_eta, beta_2_eta = compute_theoretical_eta(
            reg_df_w_FEs_w_shock_w_error, 'wage_post_shock', 'iota', 'gamma', 'gamma_error', eta_hat_true, theta_hat_true, return_betas=True)
        eta_theoretical.append(eta_theo)
        beta_1_eta_list.append(beta_1_eta)
        beta_2_eta_list.append(beta_2_eta)

        theta_theo, beta_1_theta, beta_2_theta = compute_theoretical_theta(
            reg_df_w_FEs_w_shock_w_error, 'wage_post_shock', 'iota', 'gamma', 'gamma_error', eta_hat_true, eta_theo, theta_hat_true, return_betas=True)
        theta_theoretical.append(theta_theo)
        beta_1_theta_list.append(beta_1_theta)
        beta_2_theta_list.append(beta_2_theta)

        # Print the results (optional, for checking)
        print('--------------------------------------------')
        print(f"\nMisclassification rate: {r}")
        print(f"Estimated eta_hat: {eta_hat_e:.4f}")
        print(f"Theoretical eta: {eta_theo:.4f}")
        print(f"Estimated theta_hat: {theta_hat_e:.4f}")
        print(f"Theoretical theta: {theta_theo:.4f}")
        #print(f"Eta coef 1: {beta_1_eta:.4f}")
        #print(f"Eta coef 2: {beta_2_eta:.4f}")
        #print(f"Theta coef 1: {beta_1_theta:.4f}")
        #print(f"Theta coef 2: {beta_2_theta:.4f}")
        print('--------------------------------------------')

    # Create a DataFrame with the results
    misclassification_ests = pd.DataFrame({
        'Misclassification_Rate': misclassification_rates,
        'Estimated_Eta': eta_estimates,
        'Estimated_Theta': theta_estimates,
        'Theoretical_Eta': eta_theoretical,
        'Theoretical_Theta': theta_theoretical,
        'Beta 1 Eta': beta_1_eta_list,
        'Beta 2 Eta': beta_2_eta_list,
        'Beta 1 Theta': beta_1_theta_list,
        'Beta 2 Theta': beta_2_theta_list,
        'HHI': hhi_values,
        'HHI w/ error': hhi_w_error_values
    })

    # Create the plot for elasticity estimates
    plt.figure(figsize=(12, 6))
    plt.axhline(y=eta_hat_true, color='b', linestyle=':', label='True eta')
    plt.plot(misclassification_ests['Misclassification_Rate'], misclassification_ests['Estimated_Eta'], 'b-o', label='Estimated eta')
    plt.plot(misclassification_ests['Misclassification_Rate'], misclassification_ests['Theoretical_Eta'], color='darkorange', linestyle=':', linewidth=2.5, label='Theoretical eta')
    plt.axhline(y=theta_hat_true, color='r', linestyle=':', label='True theta')
    plt.plot(misclassification_ests['Misclassification_Rate'], misclassification_ests['Estimated_Theta'], 'r-o', label='Estimated theta')
    plt.plot(misclassification_ests['Misclassification_Rate'], misclassification_ests['Theoretical_Theta'], color='black', linestyle=':', linewidth=2.5, label='Theoretical theta')
    plt.xlabel('Misclassification Rate')
    plt.ylabel('Elasticity Estimate')
    plt.title('Elasticity Estimates vs Misclassification Rate')
    plt.legend()
    plt.grid(True)
    plt.show()

    return misclassification_ests


def run_estimations_for_combinations(data, combinations, stata_or_python='Python'):
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
        eta_hat, theta_hat = run_two_step_estimation(
            data,
            est_type,
            delta=delta,
            workertypevar=wkr,
            mktvar=mkt,
            stata_or_python=stata_or_python
        )
        
        # Append the result as a record to the list
        estimates_list.append({
            'WORKERTYPE': wkr_key,
            'MKT': mkt_key,
            'EST_TYPE': est_type,
            'DELTA': delta,
            'ETA_HAT': eta_hat,
            'THETA_HAT': theta_hat
        })
        print(f'WORKERTYPE = {wkr}, MKT = {mkt}, EST_TYPE = {est_type}, DELTA = {delta}, eta = {eta_hat}, theta = {theta_hat}')
    
    # Convert the list of records to a DataFrame
    estimates_df = pd.DataFrame(estimates_list)
    
    return estimates_df


def main():

    reg_df = load_and_prepare_data(root, eta_bhm, theta_bhm)
    reg_df.to_pickle(root + 'Data/derived/reg_df.p')
    reg_df = pd.read_pickle(root + 'Data/derived/reg_df.p')

    
    # Estimate fixed effects and collapse to jid-iota-gamma-level (really that's just jid-iota level b/c gamma nested w/in jid)
    reg_df_w_FEs = estimate_fixed_effects(reg_df, stata_or_python)
    reg_df_w_FEs.to_pickle(root + 'Data/derived/reg_df_w_FEs.p')
    reg_df_w_FEs = pd.read_pickle(root + 'Data/derived/reg_df_w_FEs.p')

    # Generate shocks and find equilibrium
    delta = 0.5
    reg_df_w_FEs_w_shock = generate_shocks_and_find_equilibrium(reg_df_w_FEs, delta, eta_bhm, theta_bhm)
    reg_df_w_FEs_w_shock.to_pickle(root + 'Data/derived/reg_df_w_FEs_w_shock.p')
    reg_df_w_FEs_w_shock = pd.read_pickle(root + 'Data/derived/reg_df_w_FEs_w_shock.p')
    
    ##############################################################################################
    
    # Define the list of combinations (wkr, mkt, est_type, delta)
    combinations = [
        # The initial four estimations with default worker type and market variables
        (['iota'], ['gamma'], 'panel_iv', 0),               # "Oracle" spec: use the full instrument   reg_df2['shock_iv'] = reg_df2['jid_masked_shock'] + reg_df2['iota_gamma_shock']
        (['iota'], ['gamma'], 'panel_iv', None), # "Realistic" spec: use the partial instrument   reg_df2['shock_iv'] = reg_df2['Z_j']
        (['iota'], ['gamma'], 'panel_ols', 0),             # delta = 0
        (['iota'], ['gamma'], 'ols', 0),                   # delta = 0
        # Other market definitions
        (['iota'], ['gamma'], 'panel_iv', None),                    # delta = None
        (['iota'], ['occ2', 'code_micro'], 'panel_iv', None),       # delta = None
        (['occ2'], ['code_micro'], 'panel_iv', None),               # delta = None
        (['occ2'], ['gamma'], 'panel_iv', None),               # delta = None
    ]
    
    
    # Run the estimations
    estimates_df = run_estimations_for_combinations(
        data=reg_df_w_FEs_w_shock,
        combinations=combinations,
        stata_or_python=stata_or_python
    )
    print(estimates_df)

    ##################
    # Experiment with misclassification
    misclassification_ests = misclassification_experiment(reg_df_w_FEs_w_shock, stata_or_python)


    #####################################
    # Histograms of implied markdowns
    
    
    reg_df_w_FEs_w_shock['occ2_micro_id'] = reg_df_w_FEs_w_shock.groupby(['occ2', 'code_micro']).ngroup()

    # XX This is wrong because its giving values > 1
    # I think the reason is that jid_masked is not nested within occ2_micro_id. This could happen, for example, if an establishment changes location. I don't think this can be due to occupation codes because 2-digit codes have to be nested within the 4-digit we used to define jids
   
    # Step 1: Aggregate to find the number of unique occ2_micro_id values for each jid_masked
    unique_counts = reg_df_w_FEs_w_shock.groupby('jid_masked')['occ2_micro_id'].nunique().reset_index().rename(columns={'occ2_micro_id': 'unique_count'})    
    # Step 2: Filter to retain only jid_masked with a single unique occ2_micro_id
    single_value_jid = unique_counts[unique_counts['unique_count'] == 1]['jid_masked']
    
    # Step 3: Filter the original DataFrame
    reg_df_filtered = reg_df_w_FEs_w_shock[reg_df_w_FEs_w_shock['jid_masked'].isin(single_value_jid)]
    
    
    
    
    # for now, I'll just restrict to obs where we have a unique occ2_micro_id for each jid
    reg_df_filtered['s_j_mkt'] = compute_s_j_mkt(reg_df_filtered, wagevar='wage_guess', emp_counts='ell_iota_j_pre_shock',jobvar='jid_masked', marketvar='occ2_micro_id')

    reg_df_filtered['s_j_gamma'] = compute_s_j_mkt(reg_df_filtered, wagevar='wage_guess', emp_counts='ell_iota_j_pre_shock',jobvar='jid_masked', marketvar='gamma')

    # Correct params, gamma
    eta = estimates_df.loc[(estimates_df.WORKERTYPE=='iota') & (estimates_df.MKT=='gamma') & (estimates_df.EST_TYPE=='panel_iv')& (estimates_df.DELTA.isna()), 'ETA_HAT'].values[0]
    theta = estimates_df.loc[(estimates_df.WORKERTYPE=='iota') & (estimates_df.MKT=='gamma') & (estimates_df.EST_TYPE=='panel_iv')& (estimates_df.DELTA.isna()), 'THETA_HAT'].values[0]
    reg_df_filtered['mu_right_params_gamma'] = 1 + 1/eta + (1/theta - 1/eta) * reg_df_filtered['s_j_gamma']
    
    # Correct params, occ2-micro
    eta = estimates_df.loc[(estimates_df.WORKERTYPE=='iota') & (estimates_df.MKT=='gamma') & (estimates_df.EST_TYPE=='panel_iv')& (estimates_df.DELTA.isna()), 'ETA_HAT'].values[0]
    theta = estimates_df.loc[(estimates_df.WORKERTYPE=='iota') & (estimates_df.MKT=='gamma') & (estimates_df.EST_TYPE=='panel_iv')& (estimates_df.DELTA.isna()), 'THETA_HAT'].values[0]
    reg_df_filtered['mu_right_params_mkt'] = 1 + 1/eta + (1/theta - 1/eta) * reg_df_filtered['s_j_mkt']
    

    # Wrong params, gamma
    eta = estimates_df.loc[(estimates_df.WORKERTYPE=='iota') & (estimates_df.MKT=='occ2,code_micro') & (estimates_df.EST_TYPE=='panel_iv')& (estimates_df.DELTA.isna()), 'ETA_HAT'].values[0]
    theta = estimates_df.loc[(estimates_df.WORKERTYPE=='iota') & (estimates_df.MKT=='occ2,code_micro') & (estimates_df.EST_TYPE=='panel_iv')& (estimates_df.DELTA.isna()), 'THETA_HAT'].values[0]
    reg_df_filtered['mu_wrong_params_gamma'] = 1 + 1/eta + (1/theta - 1/eta) * reg_df_filtered['s_j_gamma']

    # Wrong params, occ2-micro
    eta = estimates_df.loc[(estimates_df.WORKERTYPE=='iota') & (estimates_df.MKT=='occ2,code_micro') & (estimates_df.EST_TYPE=='panel_iv')& (estimates_df.DELTA.isna()), 'ETA_HAT'].values[0]
    theta = estimates_df.loc[(estimates_df.WORKERTYPE=='iota') & (estimates_df.MKT=='occ2,code_micro') & (estimates_df.EST_TYPE=='panel_iv')& (estimates_df.DELTA.isna()), 'THETA_HAT'].values[0]
    reg_df_filtered['mu_wrong_params_mkt'] = 1 + 1/eta + (1/theta - 1/eta) * reg_df_filtered['s_j_mkt']
    
    
    
    values_right_params_gamma   = reg_df_filtered['mu_right_params_gamma']
    values_wrong_params_gamma   = reg_df_filtered['mu_wrong_params_gamma']
    values_right_params_mkt     = reg_df_filtered['mu_right_params_mkt']
    values_wrong_params_mkt     = reg_df_filtered['mu_wrong_params_mkt']
    weights = reg_df_filtered['ell_iota_j_pre_shock']


    # Define the bin edges to ensure consistency
    bin_edges = np.histogram_bin_edges(np.concatenate([values_right_params_gamma, values_wrong_params_gamma, values_right_params_mkt, values_wrong_params_mkt]), bins=30)
    
    # Adjust bin edges slightly for better separation
    offset = (bin_edges[1] - bin_edges[0]) * 0.1
    
    plt.figure(figsize=(10, 6))
    plt.hist(values_right_params_gamma, bins=bin_edges - offset, weights=weights, alpha=0.5, label='Right Params, Gamma', color='blue', edgecolor='black')
    plt.hist(values_wrong_params_gamma, bins=bin_edges + offset, weights=weights, alpha=0.5, label='Wrong Params, Gamma', color='red', edgecolor='black')
    plt.hist(values_right_params_mkt, bins=bin_edges, weights=weights, alpha=0.5, label='Right Params, Market', color='green', edgecolor='black')
    plt.hist(values_wrong_params_mkt, bins=bin_edges - offset*2, weights=weights, alpha=0.5, label='Wrong Params, Market', color='orange', edgecolor='black')
    
    # Add labels, title, and legend
    plt.title('Weighted Histograms with Transparency', fontsize=14)
    plt.xlabel('Values', fontsize=12)
    plt.ylabel('Weighted Frequency', fontsize=12)
    plt.legend(fontsize=12)
    plt.grid(axis='y', alpha=0.75)
    plt.show()
    
    
    
    
    #################
    # Log scale on y-axis
    
    # Adjust bin edges slightly for better separation
    offset = (bin_edges[1] - bin_edges[0]) * 0.1
    
    plt.figure(figsize=(10, 6))
    plt.hist(values_right_params_gamma, bins=bin_edges, weights=weights, alpha=0.5, label='Right Params, Gamma', color='blue', edgecolor='black')
    plt.hist(values_wrong_params_gamma, bins=bin_edges + offset, weights=weights, alpha=0.5, label='Wrong Params, Gamma', color='red', edgecolor='black')
    plt.hist(values_right_params_mkt, bins=bin_edges - offset, weights=weights, alpha=0.5, label='Right Params, Market', color='green', edgecolor='black')
    plt.hist(values_wrong_params_mkt, bins=bin_edges, weights=weights, alpha=0.5, label='Wrong Params, Market', color='orange', edgecolor='black')
    
    # Add log scale to y-axis
    plt.yscale('log')
    
    # Add labels, title, and legend
    plt.title('Weighted Histograms with Transparency (Log Scale)', fontsize=14)
    plt.xlabel('Values', fontsize=12)
    plt.ylabel('Weighted Frequency (Log Scale)', fontsize=12)
    plt.legend(fontsize=12)
    plt.grid(axis='y', alpha=0.75)
    plt.show()



    # Looks like the results are much more sensitive to the parameter estimates than the market definitions, although obviously the parameter estimates are very sensitive to market definitions
    print(reg_df_filtered[['mu_right_params_gamma','mu_right_params_mkt','mu_wrong_params_gamma','mu_wrong_params_mkt']].describe(percentiles=[.9,.95, .98, .99, .999]))

    # We are getting some enormous markdowns using occ2-micro presumably because some occ2-micros are tiny and therefore extremely concentrated. 
    #XXX Mayara uses firms (or establishments?) not jobs. 


    # Try estimating eta and theta using OLS and no shock. Does not work
    tempdf = reg_df_filtered[['iota','gamma','ell_iota_j_pre_shock', 'wage_pre_shock','worker_mkt_id', 'worker_type_id']]
    tempdf['wage_post_shock']       = tempdf['wage_pre_shock']
    tempdf['ell_iota_j_post_shock'] = tempdf['ell_iota_j_pre_shock']
    eta_hat_ols, theta_hat_ols = run_two_step_estimation(tempdf, 'ols', delta=0, workertypevar=['iota'], mktvar=['gamma'], stata_or_python=stata_or_python)
    print(eta_hat_ols, theta_hat_ols)

if __name__ == "__main__":
    main()


