import os
import pandas as pd
import numpy as np
from datetime import datetime

os.chdir("/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies/csv/20210802")

# '3states_gamma_mcmc', '3states_gamma_7500', '3states_gamma_7500_mcmc', '3states_gamma1_mcmc'
iterations = ['3states_mmc_cbo942d','mmc_cbo942d', 'gamma', '3states_gamma', '3states_gamma1', '3states_gamma1_7500']

# outsheet using "${monopsonies}/csv/`outdate'/theta_change_regressions_simpler_3states_clean_`path'.csv", comma replace
# outsheet using "${monopsonies}/csv/`outdate'/theta_change_regressions_simpler_3states_clean_`path'_`date'_`time'.csv", comma replace

results = pd.DataFrame(columns=['model', 'N_firms', 'N_eta', 'N_mkts_eta', 'eta', 'eta_se', 'eta_F', 'N_theta', 'theta', 'theta_inv', 'theta_inv_se', 'theta_F', 'theta_last_modified','eta_last_modified'])
#	Eta 	Theta
# Mayara (paper)	1.015228426	0.795544948
i=0
results.loc[i, 'model'] = 'Mayara (Table 3)'
results.loc[i, 'N_mkts_eta'] = 15717
results.loc[i, 'theta'] = 1 / 1.257
results.loc[i, 'theta_inv'] = 1.257    
results.loc[i, 'theta_inv_se'] = .096
results.loc[i, 'theta_F'] = 150.752
results.loc[i, 'N_firms'] = 344066
results.loc[i, 'N_eta'] = 15717
results.loc[i, 'eta'] = 1/(1.257-.272)
results.loc[i, 'eta_se'] = 'see table 3'
results.loc[i, 'eta_F'] = 'see table 3'
results.loc[i, 'theta_last_modified'] = 'N/A'
results.loc[i, 'eta_last_modified'] = 'N/A'

for i in range(len(iterations)):
    results.loc[i+1, 'model'] = iterations[i]
    
    theta_file = 'theta_change_regressions_simpler_clean_'+ iterations[i] + '.csv'
    eta_file   = 'eta_change_regressions_'+ iterations[i] + '.csv'
    theta_temp = pd.read_csv(theta_file)
    results.loc[i+1, 'N_theta'] = theta_temp['obs'][0]
    results.loc[i+1, 'theta'] = 1 / theta_temp['theta_inverse_b'][0]
    results.loc[i+1, 'theta_inv'] = theta_temp['theta_inverse_b'][0]    
    results.loc[i+1, 'theta_inv_se'] = theta_temp['theta_inverse_se'][0]
    results.loc[i+1, 'theta_F'] = theta_temp['fs_F'][0]
    
    eta_temp = pd.read_csv(eta_file)
    results.loc[i+1, 'N_firms'] = eta_temp['firms'][0]
    results.loc[i+1, 'N_eta'] = eta_temp['obs'][0]
    results.loc[i+1, 'N_mkts_eta'] = eta_temp['markets'][0]
    results.loc[i+1, 'eta'] = 1/eta_temp['iv_b'][0]
    results.loc[i+1, 'eta_se'] = eta_temp['iv_se'][0]
    results.loc[i+1, 'eta_F'] = eta_temp['fs_F'][0]

    results.loc[i+1, 'theta_last_modified'] = datetime.fromtimestamp(os.path.getmtime(theta_file)).strftime('%Y-%m-%d %H:%M:%S')
    results.loc[i+1, 'eta_last_modified']   = datetime.fromtimestamp(os.path.getmtime(eta_file)  ).strftime('%Y-%m-%d %H:%M:%S')

print(results)