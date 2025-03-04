import os
import pandas as pd
import numpy as np

os.chdir("/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies/csv/20210802")

iterations = ['mmc_cbo942d', 'gamma', 'gamma_mcmc', 'gamma_7500', 'gamma1']

# outsheet using "${monopsonies}/csv/`outdate'/theta_change_regressions_simpler_3states_clean_`path'.csv", comma replace
# outsheet using "${monopsonies}/csv/`outdate'/theta_change_regressions_simpler_3states_clean_`path'_`date'_`time'.csv", comma replace

results = pd.DataFrame(columns=['model', 'N_firms', 'N_eta', 'eta', 'eta_se', 'eta_F', 'N_theta', 'theta', 'theta_inverse', 'theta_inverse_se', 'theta_F'])
#	Eta 	Theta
# Mayara (paper)	1.015228426	0.795544948
i=0
results.loc[i, 'model'] = 'Mayara (Table 3)'
results.loc[i, 'N_theta'] = 15717
results.loc[i, 'theta'] = 1 / 1.257
results.loc[i, 'theta_inverse'] = 1.257    
results.loc[i, 'theta_inverse_se'] = .096
results.loc[i, 'theta_F'] = 150.752
results.loc[i, 'N_firms'] = 344066
results.loc[i, 'N_eta'] = 15717
results.loc[i, 'eta'] = 1/(1.257-.272)
results.loc[i, 'eta_se'] = 'see table 3'
results.loc[i, 'eta_F'] = 'see table 3'

for i in range(len(iterations)):
    results.loc[i+1, 'model'] = iterations[i]
    theta_temp = pd.read_csv('theta_change_regressions_simpler_3states_clean_'+ iterations[i] + '.csv')
    results.loc[i+1, 'N_theta'] = theta_temp['obs'][0]
    results.loc[i+1, 'theta'] = 1 / theta_temp['theta_inverse_b'][0]
    results.loc[i+1, 'theta_inverse'] = theta_temp['theta_inverse_b'][0]    
    results.loc[i+1, 'theta_inverse_se'] = theta_temp['theta_inverse_se'][0]
    results.loc[i+1, 'theta_F'] = theta_temp['fs_F'][0]
    
    eta_temp = pd.read_csv('eta_change_regressions_3states_'+ iterations[i] + '.csv')
    results.loc[i+1, 'N_firms'] = eta_temp['firms'][0]
    results.loc[i+1, 'N_eta'] = eta_temp['obs'][0]
    results.loc[i+1, 'eta'] = eta_temp['iv_b'][0]
    results.loc[i+1, 'eta_se'] = eta_temp['iv_se'][0]
    results.loc[i+1, 'eta_F'] = eta_temp['fs_F'][0]


