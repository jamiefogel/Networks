# conda activate gt


import os
import pandas as pd
import torch
import numpy as np
import pickle
from datetime import datetime
import sys
import getpass


now = datetime.now()
dt_string = now.strftime("%Y_%m_%d__%H_%M_%S")
print("date and time =", dt_string)	

homedir = os.path.expanduser('~')
if getpass.getuser()=='p13861161':
    print("Running on Linux")
    root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
elif getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    homedir = os.path.expanduser('~')

sys.path.append(root + 'Code/Modules')
figuredir = root + 'Results/'

os.chdir(root)


import matplotlib.pyplot as plt
from torch_mle import torch_mle
from mle_load_fulldata import mle_load_fulldata
from normalization_k import normalization_k
from alphas_func import compute_alphas
from alphas_func import load_alphas
import solve_model_functions as smf
from occ_counts_by_type import occ_counts_by_type
#from correlogram import correlogram



# Change all the filepaths to the new data

################################################################
## STRUCTURAL OBJECTS
################################################################


# Default is 100. 1000 looks better. 10000 crashed the computer.
plt.rcParams['figure.dpi'] = 100
#plt.rcParams['figure.dpi'] = 1000

level = 0
#level = int(sys.argv[1])

pre = 2013
post = 2018
eta = 2
year = pre
S = 15

xi_outopt_scalar = 0
phi_outopt_scalar = 0

torch.set_printoptions(precision=4, linewidth=200, sci_mode=False, edgeitems=150)
pd.options.display.max_columns=20
pd.options.display.width=200
np.set_printoptions(linewidth=200)
np.set_printoptions(suppress=True)


run_all = True
run_mle = True
run_query_sums = True
run_normalization = True
run_occ_counts = True
run_correlogram = True
solve_GE_silently = True
a_s_variation = True


worker_type_var = 'iota'
#worker_type_var = 'kmeans'
#worker_type_var = 'occ4_first_recode'
#worker_type_var = 'occ2_first_recode'
job_type_var    = 'gamma'
#job_type_var    = 'sector_IBGE'
#job_type_var = 'iota'



classification_list = [('iota','gamma'), ('occ2_first_recode','sector_IBGE'), ('occ4_first_recode','sector_IBGE'), ('occ4_first_recode','gamma'), ('iota','occ2Xmeso'), ('occ2Xmeso','occ2Xmeso')] # , ('kmeans','sector_IBGE'), ('kmeans','gamma')


################################################################
## RUNNING
################################################################

#--------------------------
#  Create intro figs
#--------------------------

exec(open(root + 'Code/intro_figs.py').read())


#--------------------------
#  LOAD DATA AND RUN MLE
#--------------------------
filename_stub = "panel_3states_2013to2016_new"
# Define filenames
if 1==1:
    #mle_data_filename      = homedir + "/Networks/RAIS_exports/earnings_panel/" + filename_stub + "_level_" + str(level) + ".csv"
    mle_data_filename      = root + "Data/derived/earnings_panel/" + filename_stub + "_level_0.csv"
    mle_data_sums_filename = root + "Data/derived/mle_data_sums/" + filename_stub + "_mle_data_sums_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"
    mle_estimates_filename = root + "Data/derived/MLE_estimates/" + filename_stub + "_mle_estimates_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"
    psi_and_k_file         = root + "Data/derived/MLE_estimates/" + filename_stub + "_psi_normalized_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + "_eta_" + str(eta) + ".p"
    alphas_file            = root + "Data/derived/MLE_estimates/" + filename_stub + "_alphas_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"
    
    
    
# This loads earnings_panel/panel_rio_2009_2012.csv, merges on O*NET stuff, does k-means clustering of occupatinos based on O*NET, and saves earnings_panel/panel_rio_2009_2012.csv
# Commenting out for now because we want to just use the old version of  earnings_panel/panel_rio_2009_2012.csv. See https://github.com/jamiefogel/Networks/issues/2.
#exec(open(root + 'Code/process_brazil_onet.py').read())




#--------------------------
# LOAD BETAS AND A_s
#--------------------------
exec(open(root + 'Code/load_model_parameters.py').read())

if job_type_var == 'sector_IBGE':
    b_gs = torch.diag(x_s * torch.ones(S))
else:
    b_gs = alphags * x_s




if run_all==True:
    for idx in classification_list: #
        # using wtype_var and jtype_Var instead of worker_type_var and job_type_var so we don't reset the master versions set in do_all.py
        wtype_var = idx[0]
        jtype_var = idx[1]
        suffix = wtype_var + "_" + jtype_var + "_" "level_" + str(level)
        est_mle_data_filename      = root + "Data/derived/earnings_panel/" + filename_stub + "_level_0.csv"
        est_mle_data_sums_filename = root + "Data/derived/mle_data_sums/" + filename_stub + "_mle_data_sums_" + suffix + ".p"
        est_mle_estimates_filename = root + "Data/derived/MLE_estimates/" + filename_stub + "_mle_estimates_"  + suffix + ".p"
        est_psi_and_k_file         = root + "Data/derived/MLE_estimates/" + filename_stub + "_psi_normalized_" + suffix + "_eta_" + str(eta) + ".p"
        est_alphas_file            = root + "Data/derived/MLE_estimates/" + filename_stub + "_alphas_" + suffix + "_eta_" + str(eta) + ".p"
        if run_query_sums == 1:
            mle_load_fulldata(est_mle_data_filename, est_mle_data_sums_filename, wtype_var, jtype_var, mle_firstyear=2013, mle_lastyear=2016)
        if run_mle == True:
            if wtype_var != jtype_var:
                torch_mle(est_mle_data_sums_filename, est_mle_estimates_filename, wtype_var, jtype_var, level)
            else: # Can probably be deleted. torch_mle_diagonal is in june2021 but not aug2021
                from torch_mle_diagonal import torch_mle_diag
                torch_mle_diag(est_mle_data_sums_filename, est_mle_estimates_filename, wtype_var, jtype_var, level)
        compute_alphas(est_mle_data_filename, jtype_var, 'real_hrly_wage_dec', alphas_file=est_alphas_file)
        alpha_gs=load_alphas(est_alphas_file)
        est_mle_data_sums = pickle.load(open(est_mle_data_sums_filename, "rb"), encoding='bytes')
        est_mle_estimates = pickle.load(open(est_mle_estimates_filename, "rb"), encoding='bytes')
        if jtype_var == 'sector_IBGE':
            b_gs = torch.diag(x_s * torch.ones(S))
        else:
            b_gs = alphags * x_s
        if run_normalization == True:
            normalization_k(est_psi_and_k_file,  wtype_var, jtype_var, est_mle_estimates, est_mle_data_sums, S, a_s, b_gs, eta, phi_outopt_scalar, xi_outopt_scalar, level, pre, raw_data_file=est_mle_data_filename) 









if run_query_sums == 1:
    mle_load_fulldata(mle_data_filename, mle_data_sums_filename, worker_type_var, job_type_var, mle_firstyear=2013, mle_lastyear=2016)

if run_mle == True:
    if worker_type_var != job_type_var:
        torch_mle(mle_data_sums_filename, mle_estimates_filename, worker_type_var, job_type_var, level)
    else: # Can probably be deleted. torch_mle_diagonal is in june2021 but not aug2021
        from torch_mle_diagonal import torch_mle_diag
        torch_mle_diag(mle_data_sums_filename, mle_estimates_filename, worker_type_var, job_type_var, level)

mle_data_sums = pickle.load(open(mle_data_sums_filename, "rb"), encoding='bytes')
mle_estimates = pickle.load(open(mle_estimates_filename, "rb"), encoding='bytes')

# Load estimates and data
if run_normalization == True:
    normalization_k(psi_and_k_file,  worker_type_var, job_type_var, mle_estimates, mle_data_sums, S, a_s, b_gs, eta, phi_outopt_scalar, xi_outopt_scalar, level, pre, raw_data_file=mle_data_filename) 

if run_occ_counts == True:
    [w_dict, j_dict] = occ_counts_by_type(mle_data_filename, root + 'Data/translated_occ_codes/translated_occ_codes_english_only.csv', level=0, w_output=root + '/Data/Derived/occ_counts/" + filename_stub + "_occ_counts_by_i_level_' + str(level) + '.csv', j_output=root + '/Data/Derived/occ_counts/" + filename_stub + "_occ_counts_by_g_level_' + str(level) + '.csv')
    pickle.dump((w_dict, j_dict), open(root + '/Data/Derived/occ_counts/occ_counts.p', 'wb'))





#--------------------------------------

#--------------------------
# LOAD BETAS AND A_s
#--------------------------


    
    
# Load estimates and data
psi_and_k = pickle.load(open(psi_and_k_file, "rb"), encoding='bytes')
psi_hat = psi_and_k['psi_hat']
k = psi_and_k['k']    
    

#--------------------------
#  SOLVE MODEL
#--------------------------
#exec(open(root + 'Code/solve_model.py').read())




#--------------------------
#  Descriptive analysis
#--------------------------


if run_correlogram==True:
    exec(open(root + 'Code/correlogram.py').read())
    exec(open(root + 'Code/concentration_figures.py').read())
    

#--------------------------
#  ANALYSIS
#--------------------------



exec(open(root + 'Code/model_fit.py').read())
exec(open(root + 'Code/reduced_form.py').read())

exec(open(root + 'Code/shock_case_study.py').read())




#--------------------------
#  Some summary stats about occ4s and kmeans
#--------------------------

data_full = pd.read_csv(root + 'Data/RAIS_exports/earnings_panel/" + filename_stub + "_w_kmeans.csv')
n_kmeans = data_full.kmeans.value_counts().shape[0]
print(n_kmeans, " kmeans groups remaining after dropping those with fewer than 5000 observations")




#--------------------------
#  Miscellaneous
#--------------------------

exec(open(root + 'Code/trans_mat_symmetry_analysis.py').read())  # Previously called misc_analysis
exec(open(root + 'Code/classification_error_analysis.py').read())
#exec(open(root + 'Code/akm_exercise.py').read())

#--------------------------
#  OLD
#-----------
#correlogram(psi_hat, figuredir+'correlograms_' + worker_type_var + '_' + job_type_var + '.png' , figuredir+'correlograms_hist_' + worker_type_var + '_' + job_type_var + '.png' ,sorted=False)

'''
exec(open(root + 'Code/locality_figures_v2.py').read())

exec(open(root + 'Code/simulate_shock.py').read())

exec(open(root + 'Code/skill_maps.py').read())

exec(open(root + 'Code/shock_decomposition.py').read())



exec(open(root + 'Code/model_fit_cross_section.py').read())

exec(open(root + 'Code/actual_adh.py').read())
'''



#exec(open(root + 'Code/labor_demand_shocks.py').read())


#--------------------------
#  DGP
#--------------------------

#exec(open(root + 'Code/dgp_exercise_generate_data.py').read())
#exec(open(root + 'Code/dgp_exercise_analysis.py').read())

