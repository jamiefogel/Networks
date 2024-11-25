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
import getpass
import platform
import statsmodels.api as sm




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


from market_power_utils import (
    run_stata_code,
    compute_markdowns_w_iota,
    generate_shocks,
    run_two_step_estimation,
    find_equilibrium,
    introduce_misclassification_by_jid,
    compute_market_hhi
)


inv_eta_mayara   = 0.985
inv_theta_mayara = 1.257

eta_mayara = 1/inv_eta_mayara
theta_mayara = 1/inv_theta_mayara

eta_bhm = 7.14
theta_bhm = .45

np.random.seed(734)

def main():
    
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
    theta_theoretical = []
    tilde_beta_1_list = []
    tilde_beta_2_list = []
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
        
        ################################################################################################
        # Calculate theoretical elasticities based on the derivations we came up with on 10/28/2024  
        ################################################################################################
        
        #######################
        # eta
        collapsed_df_w_shock_w_error['ln_wage_post_shock'] = np.log(collapsed_df_w_shock_w_error['wage_post_shock'])
        collapsed_df_w_shock_w_error['ln_wage_demeaned_within_iota_gamma'] = collapsed_df_w_shock_w_error['ln_wage_post_shock'] - collapsed_df_w_shock_w_error.groupby(['iota','gamma'])['ln_wage_post_shock'].transform('mean')
        collapsed_df_w_shock_w_error['ln_wage_demeaned_within_iota_m']     = collapsed_df_w_shock_w_error['ln_wage_post_shock'] - collapsed_df_w_shock_w_error.groupby(['iota','gamma_error'])['ln_wage_post_shock'].transform('mean')
        
        collapsed_df_w_shock_w_error['temp2'] = np.exp( collapsed_df_w_shock_w_error['ln_wage_post_shock'] * (1+eta_hat_true) )
        collapsed_df_w_shock_w_error['mu_iota_gamma_j'] = np.log( collapsed_df_w_shock_w_error.groupby(['iota','gamma'])['temp2'].transform('sum') )  / (1 + eta_hat_true)
        collapsed_df_w_shock_w_error['wbar_minus_mu_ig'] = collapsed_df_w_shock_w_error.groupby(['iota','gamma'])['ln_wage_post_shock'].transform('mean') - collapsed_df_w_shock_w_error['mu_iota_gamma_j']
        
        tilde_beta_1 = sm.OLS(collapsed_df_w_shock_w_error['ln_wage_demeaned_within_iota_gamma'], sm.add_constant(collapsed_df_w_shock_w_error['ln_wage_demeaned_within_iota_m'])).fit().params[1]
        # Beta_1 measures the correlation between the deviation of my wage from my gamma average and the deviation of my wage from my miscalssified market wage. 
        tilde_beta_2 = sm.OLS(collapsed_df_w_shock_w_error['wbar_minus_mu_ig'], sm.add_constant(collapsed_df_w_shock_w_error['ln_wage_demeaned_within_iota_m'])).fit().params[1]
        bias = (theta_hat_true-eta_hat_true) * (1 - tilde_beta_1 - tilde_beta_2)
        eta_theo = eta_hat_true + bias
        eta_theoretical.append(eta_theo)
        tilde_beta_1_list.append(tilde_beta_1)
        tilde_beta_2_list.append(tilde_beta_2)
        
        #######################
        # theta
        
        # Compute mu_iota_m. This will be collapsed to the iota-m level
        collapsed_df_w_shock_w_error['temp_inner_m'] =  np.exp((1+eta_theo)*collapsed_df_w_shock_w_error['ln_wage_post_shock'])
        collapsed_df_w_shock_w_error['mu_iota_m'] = np.log( (collapsed_df_w_shock_w_error.groupby(['iota','gamma_error'])['temp_inner_m'].transform('sum') )**(1/(1+eta_theo)) )
        #iota_m_df =  np.log( (collapsed_df_w_shock_w_error.groupby(['iota','gamma_error'])['temp_inner_m'].sum() )**(1/(1+eta_theo)) ).reset_index().rename(columns={'temp_inner_m': 'mu_iota_m'}) 
        
        # Compute mu_iota_gamma. This will remain at the iota-j level because we collapse it to the iota-m level weighting by s_iota_j_m
        collapsed_df_w_shock_w_error['temp_inner_g'] =  np.exp((1+eta_hat_true)*collapsed_df_w_shock_w_error['ln_wage_post_shock'])
        collapsed_df_w_shock_w_error['mu_iota_g'] =  np.log( (collapsed_df_w_shock_w_error.groupby(['iota','gamma'      ])['temp_inner_g'].transform('sum') )**(1/(1+eta_hat_true)) )
        
        collapsed_df_w_shock_w_error['s_iota_j_m'] = np.exp(  (1+eta_hat_true)*(collapsed_df_w_shock_w_error['ln_wage_post_shock'] - collapsed_df_w_shock_w_error['mu_iota_m'])  )
        
        # XX Do I need to fill in rows that are zeros here?
        df_iota_m = collapsed_df_w_shock_w_error[['iota', 'gamma_error', 'mu_iota_m']].drop_duplicates()
        uniqueness_check = df_iota_m.groupby(['iota', 'gamma_error']).size().max() == 1
        if not uniqueness_check:
            raise ValueError("The result is not uniquely identified by 'iota' and 'gamma_error'.")
        df_iota_m['mu_iota_bar'] = df_iota_m.groupby('iota')['mu_iota_m'].transform('mean')
        df_iota_m['mu_iota_m_demeaned'] = df_iota_m['mu_iota_m'] - df_iota_m['mu_iota_bar']
    
        collapsed_df_w_shock_w_error['temp3'] = collapsed_df_w_shock_w_error['s_iota_j_m'] * collapsed_df_w_shock_w_error['mu_iota_g']
        sum_s_mu = collapsed_df_w_shock_w_error.groupby(['iota','gamma_error'])['temp3'].sum().reset_index().rename(columns={'temp3':'sum_s_mu'})
        df_iota_m = df_iota_m.merge(sum_s_mu, on=['iota','gamma_error'], validate='1:1')
        
        # Compute the delta ("Jensen bias" term)
        collapsed_df_w_shock_w_error['temp4'] = np.exp((theta_hat_true - eta_hat_true)*collapsed_df_w_shock_w_error['mu_iota_g'] ) * collapsed_df_w_shock_w_error['s_iota_j_m']
        delta_term1 = ((1/(theta_hat_true - eta_hat_true)) * np.log(collapsed_df_w_shock_w_error.groupby(['iota','gamma_error'])['temp4'].sum())).reset_index().rename(columns={'temp4':'delta_term1'})
        df_iota_m = df_iota_m.merge(delta_term1, on=['iota','gamma_error'], validate='1:1')
        df_iota_m['delta_iota_m'] = df_iota_m['delta_term1'] - df_iota_m['sum_s_mu']
        
    
        coef1 = sm.OLS(df_iota_m['sum_s_mu'], sm.add_constant(df_iota_m['mu_iota_m_demeaned'])).fit().params[1]
        coef2 = sm.OLS(df_iota_m['delta_iota_m'], sm.add_constant(df_iota_m['mu_iota_m_demeaned'])).fit().params[1]
        bias = (eta_hat_true - theta_hat_true) * ( 1 - coef1 - coef2)
        theta_theo = theta_hat_true + bias
        theta_theoretical.append(theta_theo)
        print(theta_theo)
        print(theta_hat_e)
        
        # Print the results (optional, for checking)
        print('--------------------------------------------')
        print(f"\nMisclassification rate: {r}")
        print(f"Estimated eta_hat: {eta_hat_e:.4f}")
        print(f"Theoretical eta: {eta_theo:.4f}")
        print(f"Estimated theta_hat: {theta_hat_e:.4f}")
        print(f"Theoretical theta: {theta_theo:.4f}")
        print(f"tilde beta 1: {tilde_beta_1:.4f}")
        print(f"tilde beta 2: {tilde_beta_2:.4f}")
        print(f"HHI: {hhi:.4f}")
        print(f"HHI w/ error: {hhi_w_error:.4f}")
        print('--------------------------------------------')
    
    # Create a DataFrame with the results
    misclassification_ests = pd.DataFrame({
        'Misclassification_Rate': misclassification_rates,
        'Estimated_Eta': eta_estimates,
        'Estimated_Theta': theta_estimates,
        'Theoretical_Eta': eta_theoretical,
        'Theoretical_Theta': theta_theoretical,
        'Tilde Beta 1': tilde_beta_1_list,
        'Tilde Beta 2': tilde_beta_2_list,
        #'Theoretical_Theta': theta_theoretical,
        'HHI': hhi_values,
        'HHI w/ error': hhi_w_error_values
    })

    # Create the first plot for elasticity estimates
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

if __name__ == "__main__":
    main()


