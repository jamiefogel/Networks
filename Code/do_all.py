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
    root = homedir + '/NetworksGit/'

sys.path.append(root + 'Code/Modules')
figuredir = root + 'Results/'

os.chdir(root)

import bisbm
from create_earnings_panel import create_earnings_panel
from pull_one_year import pull_one_year


import matplotlib.pyplot as plt
from torch_mle import torch_mle
from mle_load_fulldata import mle_load_fulldata
from normalization_k import normalization_k
from alphas_func import load_alphas
import solve_model_functions as smf
from occ_counts_by_type import occ_counts_by_type
from correlogram import correlogram
from concentration_figures import concentration_figures


# Change all the filepaths to the new data

################################################################
## STRUCTURAL OBJECTS
################################################################


#####################################
# Options from IPEA/do_all_ipea.py

run_sbm = True
run_pull=True
run_append = True
maxrows=None
#modelname = '3states_2009to2012'
modelname = 'synthetic_data_3states_2009to2012'
#rais_filename_stub =  '~/rais/RAIS/csv/brasil' 
rais_filename_stub = root + './Data/raw/synthetic_data_'


firstyear_sbm = 2009
lastyear_sbm  = 2012
firstyear_panel = 2009
lastyear_panel  = 2014
state_codes = [31, 33, 35]


#####################################
# Options from do_all.py

# Default is 100. 1000 looks better. 10000 crashed the computer.
plt.rcParams['figure.dpi'] = 100
#plt.rcParams['figure.dpi'] = 1000

level = 0
#level = int(sys.argv[1])

pre = 2009
post = 2014
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
run_predictions = True

worker_type_var = 'iota'
#worker_type_var = 'kmeans'
#worker_type_var = 'occ4_first_recode'
#worker_type_var = 'occ2_first_recode'
job_type_var    = 'gamma'
#job_type_var    = 'sector_IBGE'
#job_type_var = 'iota'



classification_list = [('iota','gamma'), ('occ2_first_recode','sector_IBGE'), ('occ4_first_recode','sector_IBGE'), ('occ4_first_recode','gamma'), ('iota','occ2Xmeso_recode')] # ('occ2Xmeso_recode','occ2Xmeso_recode')] # , ('kmeans','sector_IBGE'), ('kmeans','gamma')




################################################################
## PULL RAW DATA FROM RAIS, RUN SBM, CREATE EARNINGS PANEL (from original IPEA/do_all_ipea.py)
################################################################


# Pull region codes
region_codes = pd.read_csv('./Data/raw/munic_microregion_rm.csv', encoding='latin1')
muni_micro_cw = pd.DataFrame({'code_micro':region_codes.code_micro,'codemun':region_codes.code_munic//10})
code_list = region_codes.loc[region_codes.rm_dummy==1][['micro','code_micro']].drop_duplicates().sort_values(by='code_micro')
micro_code_list = code_list.code_micro.values
dict = {}
for m in micro_code_list:
    value_list = [n//10 for n in region_codes.loc[region_codes.code_micro==m].code_munic.values.tolist()]
    dict[m] = {'micro':code_list.loc[code_list.code_micro==m]['micro'].iloc[0],'muni_codes':value_list}



# Changes to make:
# - Choose best measure of earnings


################################################################################################    
# Pull raw data for SBM

if run_pull==True:
    print('Starting pull_one_year() at ', datetime.now())
    for year in range(firstyear_panel,lastyear_panel+1):
        print(year, ' ', datetime.now())
        pull_one_year(year, 'cbo2002', savefile='./Data/derived/raw_data_sbm_' + modelname + '_' + str(year) + '.p',state_codes=state_codes, age_lower=25, age_upper=55, othervars=['data_adm','data_deslig','tipo_salario','rem_dez_r','horas_contr','clas_cnae20'], parse_dates=['data_adm','data_deslig'], nrows=maxrows, filename=rais_filename_stub + str(year) + '.csv')
        
################################################################################################
# Append raw data for SBM

if run_append==True:
    print('Starting append at ', datetime.now())
    for year in range(firstyear_panel,lastyear_panel+1):
        print(year, ' ', datetime.now())
        df = pickle.load( open('./Data/derived/raw_data_sbm_' + modelname + '_' + str(year) + '.p', "rb" ) )
        if year==firstyear_panel:
            appended = df
        else:
            appended = df.append(appended, sort=True)
        del df
    appended.to_pickle('./Data/derived/appended_sbm_' + modelname + '.p')

################################################################################################
# Run SBM

appended = pd.read_pickle('./Data/derived/appended_sbm_' + modelname + '.p')
occvar = 'cbo2002'
if run_sbm==True:
    print('Starting SBM section at ', datetime.now())
    # It's kinda inefficient to pickle the edgelist then load it from pickle but kept this for flexibility
    bipartite_edgelist = appended.loc[(appended['year']>=firstyear_sbm) & (appended['year']<=lastyear_sbm)][['wid','jid']].drop_duplicates(subset=['wid','jid'])
    jid_occ_cw =         appended.loc[(appended['year']>=firstyear_sbm) & (appended['year']<=lastyear_sbm)][['jid','cbo2002']].drop_duplicates(subset=['jid','cbo2002'])
    pickle.dump( bipartite_edgelist,  open('./Data/derived/bipartite_edgelist_'+modelname+'.p', "wb" ) )
    model = bisbm.bisbm()                                                                       
    model.create_graph(filename='./Data/derived/bipartite_edgelist_'+modelname+'.p',min_workers_per_job=5)
    model.fit(n_init=1)
    # In theory it makes more sense to save these as pickles than as csvs but I keep getting an error loading the pickle and the csv works fine
    model.export_blocks(output='./Data/derived/sbm_output/model_'+modelname+'_blocks.csv', joutput='./Data/derived/sbm_output/model_'+modelname+'_jblocks.csv', woutput='./Data/derived/sbm_output/model_'+modelname+'_wblocks.csv')
    pickle.dump( model, open('./Data/derived/sbm_output/model_'+modelname+'.p', "wb" ), protocol=4 )
    print('SBM section complete at ', datetime.now())


################################################################################################
# Create earnings panel that we can use for the MLE

print('Starting create_earnings_panel() section at ', datetime.now())
create_earnings_panel(modelname, appended, 2009, 2014)
print('create_earnings_panel() section finished  at ', datetime.now())





################################################################
## MLE AND ANALYSIS (from original do_all.py)
################################################################

#--------------------------
#  Create intro figs
#--------------------------

exec(open(root + 'Code/intro_figs.py').read())


#--------------------------
#  LOAD DATA AND RUN MLE
#--------------------------
filename_stub = "panel_"+modelname
#filename_stub = "panel_3states_2013to2016_new"
# Define filenames
if 1==1:
    mle_data_filename      = root + "Data/derived/earnings_panel/" + filename_stub + "_level_0.csv"
    mle_data_sums_filename = root + "Data/derived/mle_data_sums/" + filename_stub + "_mle_data_sums_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"
    mle_estimates_filename = root + "Data/derived/MLE_estimates/" + filename_stub + "_mle_estimates_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"
    psi_and_k_file         = root + "Data/derived/MLE_estimates/" + filename_stub + "_psi_normalized_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + "_eta_" + str(eta) + ".p"
    alphas_file            = root + "Data/derived/MLE_estimates/" + filename_stub + "_alphas_" + worker_type_var + "_" + job_type_var + "_" "level_" + str(level) + ".p"
    
    
    
# This loads earnings_panel/panel_rio_2009_2012.csv, merges on O*NET stuff, does k-means clustering of occupatinos based on O*NET, and saves earnings_panel/panel_rio_2009_2012.csv
# Commenting out for now because we want to just use the old version of  earnings_panel/panel_rio_2009_2012.csv. See https://github.com/jamiefogel/Networks/issues/2.
#exec(open(root + 'Code/process_brazil_onet.py').read())




#--------------------------
# LOAD sector-level production and other related info
#--------------------------
exec(open(root + 'Code/load_model_parameters.py').read())


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
            mle_load_fulldata(est_mle_data_filename, est_mle_data_sums_filename, wtype_var, jtype_var, 'real_hrly_wage_dec', est_alphas_file, mle_firstyear=2013, mle_lastyear=2016)
        if run_mle == True:
            if wtype_var != jtype_var:
                torch_mle(est_mle_data_sums_filename, est_mle_estimates_filename, wtype_var, jtype_var, level)
            else: # Can probably be deleted. torch_mle_diagonal is in june2021 but not aug2021
                from torch_mle_diagonal import torch_mle_diag
                torch_mle_diag(est_mle_data_sums_filename, est_mle_estimates_filename, wtype_var, jtype_var, level)
        alphags=load_alphas(est_alphas_file)
        est_mle_data_sums = pickle.load(open(est_mle_data_sums_filename, "rb"), encoding='bytes')
        est_mle_estimates = pickle.load(open(est_mle_estimates_filename, "rb"), encoding='bytes')
        if jtype_var == 'sector_IBGE':
            b_gs = torch.diag(x_s * torch.ones(S))
        else:
            b_gs = alphags * x_s
        if run_normalization == True:
            normalization_k(est_psi_and_k_file,  wtype_var, jtype_var, est_mle_estimates, est_mle_data_sums, S, a_s, b_gs, eta, phi_outopt_scalar, xi_outopt_scalar, level, pre, raw_data_file=est_mle_data_filename) 



mle_data_sums = pickle.load(open(mle_data_sums_filename, "rb"), encoding='bytes')
mle_estimates = pickle.load(open(mle_estimates_filename, "rb"), encoding='bytes')


if run_occ_counts == True:
    [w_dict, j_dict] = occ_counts_by_type(mle_data_filename, root + 'Data/raw/translated_occ_codes_english_only.csv', level=0, w_output=root + '/Data/derived/occ_counts/' + filename_stub + '_occ_counts_by_i_level_' + str(level) + '.csv', j_output=root + '/Data/derived/occ_counts/' + filename_stub + '_occ_counts_by_g_level_' + str(level) + '.csv')
    pickle.dump((w_dict, j_dict), open(root + '/Data/derived/occ_counts/'+ filename_stub + 'occ_counts_level_' + str(level) + '.p', 'wb'))





#--------------------------------------

#--------------------------
# LOAD BETAS AND A_s
#--------------------------


    
    
# Load estimates and data
psi_and_k = pickle.load(open(psi_and_k_file, "rb"), encoding='bytes')
psi_hat = psi_and_k['psi_hat']
k = psi_and_k['k']    
    


#--------------------------
#  Descriptive analysis
#--------------------------


# Correlograms
if run_correlogram==True:
    exec(open(root + 'Code/run_correlograms.py').read())



# Concentration figures
data_full = pd.read_csv(mle_data_filename)
data_full_concfigs = data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1)]

concentration_figures(data_full_concfigs, 'iota', 'Workers (sorted by employment HHI)', ['sector_IBGE','gamma'],    {'sector_IBGE':'Sector','gamma':'Market'},                     figuredir+'concentration_figures__iota__sector_IBGE__gamma.png',weighted=True)
concentration_figures(data_full_concfigs, 'iota', 'Workers (sorted by employment HHI)', ['clas_cnae20','gamma'],    {'clas_cnae20':'5-Digit Industry','gamma':'Market'},           figuredir+'concentration_figures__iota__clas_cnae20__IBGE_gamma.png',weighted=True)
concentration_figures(data_full_concfigs, 'iota', 'Workers (sorted by employment HHI)', ['occ2Xmeso','gamma'],      {'occ2Xmeso':'Occ2 X Meso Region','gamma':'Market'},           figuredir+'concentration_figures__iota__occ2Xmeso__IBGE_gamma.png',weighted=True)
concentration_figures(data_full_concfigs, 'gamma', 'Markets (sorted by hiring HHI)',    ['occ2Xmeso_first','iota'], {'occ2Xmeso_first':'Occ2 X Meso Region','iota':'Worker Type'}, figuredir+'concentration_figures__gamma__occ2Xmeso_first__iota.png',weighted=True)
concentration_figures(data_full_concfigs, 'gamma', 'Markets (sorted by hiring HHI)',    ['occ4_first','iota'],      {'occ4_first':'4-Digit Occupation','iota':'Worker Type'},      figuredir+'concentration_figures__gamma__occ4_first__iota.png',weighted=True)

# Gamma summary stats including binscatters and meso_plots
exec(open(root + 'Code/gamma_summary_stats.py').read())

    
#--------------------------
#  Add prediction exercise code
#--------------------------

if run_predictions==True:
    exec(open(root + 'Code/predicting_flows_data_pull.py').read())
    # WE have been running the actual predictions (coded in the script below) in parallel using the following script: NetworksGit\Code\bash_parallel_jobtransitions_market_based_mutiple_mkts.sh
    # XX Code ran successfully through here on 8/11/2023. Failed somewhere in in the next script
    exec(open(root + 'Code/parallel_jobtransitions_market_based_multiple_mkts.py').read())
    exec(open(root + 'Code/parallel_jobtransitions_stack_results.py').read())


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

