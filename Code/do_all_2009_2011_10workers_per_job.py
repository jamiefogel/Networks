
# conda activate gt

import os
import sys
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import getpass
import subprocess
import platform
import matplotlib.pyplot as plt


now = datetime.now()
dt_string = now.strftime("%Y_%m_%d__%H_%M_%S")
print("date and time =", dt_string)	


homedir = os.path.expanduser('~')
os_name = platform.system()
if getpass.getuser()=='p13861161':
    if os_name == 'Windows':
        print("Running on Windows") 
        root = "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/"
        rais = "//storage6/bases/DADOS/RESTRITO/RAIS/"
        sys.path.append(root + 'Code/Modules')
    elif os_name == 'Linux':
        print("Running on Linux") 
        root = "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/"
        rais = "/home/DLIPEA/p13861161/rais/RAIS/"
        sys.path.append(root + 'Code/Modules')
        import torch
        import bisbm
        from create_earnings_panel import create_earnings_panel
        from torch_mle import torch_mle
        from mle_load_fulldata import mle_load_fulldata
        from normalization_k import normalization_k
        from alphas_func import load_alphas
        import solve_model_functions as smf
        from correlogram import correlogram
        torch.set_printoptions(precision=4, linewidth=200, sci_mode=False, edgeitems=150)



if getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'
    sys.path.append(root + 'Code/Modules')

figuredir = root + 'Results/'
os.chdir(root)

from pull_one_year import pull_one_year
from concentration_figures import concentration_figures
from occ_counts_by_type import occ_counts_by_type



# Change all the filepaths to the new data

################################################################
## STRUCTURAL OBJECTS
################################################################


#####################################
# Options from IPEA/do_all_ipea.py

run_sbm = False
run_sbm_mcmc = False
run_pull=False
run_append = False
run_create_earnings_panel = False
maxrows=None
modelname = '3states_2009to2011'
#modelname = 'synthetic_data_3states_2009to2012'
filename_stub = "panel_"+modelname
rais_filename_stub =  '~/rais/RAIS/csv/brasil' 
#rais_filename_stub = root + './Data/raw/synthetic_data_'


firstyear_sbm = 2009
lastyear_sbm  = 2011
firstyear_panel = 2009
lastyear_panel  = 2014
state_codes = [31, 33, 35]
gamma_summary_stats_year = 2010   # Define a year to compute summary stats

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
S = 45
num_states = 3 # This is relevant for knowing how many repetitions of the alphags matrix to make. One for each state. 


xi_outopt_scalar = 0
phi_outopt_scalar = 0

pd.options.display.max_columns=20
pd.options.display.width=200
np.set_printoptions(linewidth=200)
np.set_printoptions(suppress=True)


run_all = False
run_mle = False
run_query_sums = False
run_normalization = False
solve_GE_silently = True
a_s_variation = True

run_intro_figs = False
run_occ_counts = False
run_shock_case_study=False
run_reduced_form=True
run_model_fit=False
run_concentration_figures=True
run_correlograms=False
run_predictions=False
run_gamma_summary_stats=False


worker_type_var = 'iota'
#worker_type_var = 'kmeans'
#worker_type_var = 'occ4_first_recode'
#worker_type_var = 'occ2_first_recode'
job_type_var    = 'gamma'
#job_type_var    = 'sector_IBGE'
#job_type_var = 'iota'


#
classification_list = [('iota','gamma'), ('occ2_first_recode','sector_IBGE'), ('occ4_first_recode','sector_IBGE'), ('occ4_first_recode','gamma'), ('iota','occ2Xmeso_recode'), ('occ2Xmeso_first_recode','gamma'), ('occ2Xmeso_first_recode','sector_IBGE') ] # ('occ2Xmeso_recode','occ2Xmeso_recode')] # , ('kmeans','sector_IBGE'), ('kmeans','gamma')




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
        pull_one_year(year, 'cbo2002', savefile='./Data/derived/raw_data_sbm_' + modelname + '_' + str(year) + '.p',state_codes=state_codes, age_lower=25, age_upper=55, othervars=['data_adm','data_deslig','tipo_salario','rem_dez_r','horas_contr','clas_cnae20','cnpj_raiz','grau_instr'], parse_dates=['data_adm','data_deslig'], nrows=maxrows, filename=rais_filename_stub + str(year) + '.csv')
        
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
    model.create_graph(filename='./Data/derived/bipartite_edgelist_'+modelname+'.p',min_workers_per_job=10)
    model.fit(n_init=1)
    # In theory it makes more sense to save these as pickles than as csvs but I keep getting an error loading the pickle and the csv works fine
    model.export_blocks(output='./Data/derived/sbm_output/model_'+modelname+'_blocks.csv', joutput='./Data/derived/sbm_output/model_'+modelname+'_jblocks.csv', woutput='./Data/derived/sbm_output/model_'+modelname+'_wblocks.csv')
    pickle.dump( model, open('./Data/derived/sbm_output/model_'+modelname+'.p', "wb" ), protocol=4 )
    print('SBM section complete at ', datetime.now())

    
if run_sbm_mcmc==True:
    model = pickle.load(open('./Data/derived/sbm_output/model_'+modelname+'.p', "rb" ))
    model.mcmc_sweeps('./Data/derived/sbm_output/model_'+modelname+'_mcmc.p', tempsavedir='./Data/derived/sbm_output/', numiter=1000, seed=734)
    pickle.dump( model, open('./Data/derived/sbm_output/model_'+modelname+'_MCMC.p', "wb" ), protocol=4 )


################################################################################################
# Create earnings panel that we can use for the MLE

if run_create_earnings_panel==True:
    print('Starting create_earnings_panel() section at ', datetime.now())
    create_earnings_panel(modelname, appended, 2009, 2014)
    print('create_earnings_panel() section finished  at ', datetime.now())





################################################################
## MLE AND ANALYSIS (from original do_all.py)
################################################################

#--------------------------
#  Create intro figs
#--------------------------

if run_intro_figs==True:
    exec(open(root + 'Code/intro_figs.py').read())


#--------------------------
#  LOAD DATA AND RUN MLE
#--------------------------

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
sector_data_filepath = root + "Data/raw/IBGE/Conta_da_producao_2002_2017_xls"
exec(open(root + 'Code/process_ibge_data.py').read())
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
        est_alphas_file            = root + "Data/derived/MLE_estimates/" + filename_stub + "_alphas_" + suffix + ".p"
        if run_query_sums == True:
            mle_load_fulldata(est_mle_data_filename, est_mle_data_sums_filename, wtype_var, jtype_var, 'real_hrly_wage_dec', est_alphas_file, mle_firstyear=firstyear_sbm, mle_lastyear=lastyear_sbm, worker_type_min_count=1000)
        if run_mle == True:
            if wtype_var != jtype_var:
                if idx==('occ2Xmeso_first_recode', 'gamma'):
                    c_bump=.5
                    count_bump=.5
                else:
                    c_bump=.2
                    count_bump=.2
                torch_mle(est_mle_data_sums_filename, est_mle_estimates_filename, wtype_var, jtype_var, level, c_bump=c_bump, count_bump=count_bump)
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
    print('Running occ counts')
    [w_dict, j_dict] = occ_counts_by_type(mle_data_filename, root + 'Data/raw/translated_occ_codes_english_only.csv', level=0, w_output=root + '/Data/derived/occ_counts/' + filename_stub + '_occ_counts_by_i_level_' + str(level) + '.csv', j_output=root + '/Data/derived/occ_counts/' + filename_stub + '_occ_counts_by_g_level_' + str(level) + '.csv')
    pickle.dump((w_dict, j_dict), open(root + '/Data/derived/occ_counts/'+ filename_stub + 'occ_counts_level_' + str(level) + '.p', 'wb'))
    print('Finished occ counts')




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
if run_correlograms==True:
    print('Starting Correlograms')
    exec(open(root + 'Code/run_correlograms.py').read())
    print('Finished Correlograms')

# Concentration figures
if run_concentration_figures==True:
    print('Starting concentration figures')
    data_full = pd.read_csv(mle_data_filename)
    data_full_concfigs = data_full.loc[(data_full.iota!=-1) & (data_full.gamma!=-1)]
    concentration_figures(data_full_concfigs, 'iota', 'Workers (sorted by employment HHI)', ['sector_IBGE','gamma'],    {'sector_IBGE':'Sector','gamma':'Market'},                     figuredir+'concentration_figures/concentration_figures__iota__sector_IBGE__gamma.png',weighted=True)
    concentration_figures(data_full_concfigs, 'iota', 'Workers (sorted by employment HHI)', ['clas_cnae20','gamma'],    {'clas_cnae20':'5-Digit Industry','gamma':'Market'},           figuredir+'concentration_figures/concentration_figures__iota__clas_cnae20__gamma.png',weighted=True)
    concentration_figures(data_full_concfigs, 'iota', 'Workers (sorted by employment HHI)', ['occ2Xmeso','gamma'],      {'occ2Xmeso':'Occ2 X Meso Region','gamma':'Market'},           figuredir+'concentration_figures/concentration_figures__iota__occ2Xmeso__gamma.png',weighted=True)
    concentration_figures(data_full_concfigs, 'gamma', 'Markets (sorted by hiring HHI)',    ['occ2Xmeso_first','iota'], {'occ2Xmeso_first':'Occ2 X Meso Region','iota':'Worker Type'}, figuredir+'concentration_figures/concentration_figures__gamma__occ2Xmeso_first__iota.png',weighted=True)
    concentration_figures(data_full_concfigs, 'gamma', 'Markets (sorted by hiring HHI)',    ['occ4_first','iota'],      {'occ4_first':'4-Digit Occupation','iota':'Worker Type'},      figuredir+'concentration_figures/concentration_figures__gamma__occ4_first__iota.png',weighted=True)
    print('Finished concentration figures')
    
    concentration_figures(data_full_concfigs, 'iota', 'Workers (sorted by employment HHI)', ['occ4','gamma'],    {'occ4':'4-Digit Occupation','gamma':'Market'},                     figuredir+'concentration_figures/concentration_figures__iota__occ4__gamma.png',weighted=True)


# Gamma summary stats including binscatters and meso_plots
if run_gamma_summary_stats==True:
    print('Starting gamma summary stats')
    exec(open(root + 'Code/gamma_summary_stats.py').read())
    print('Finished gamma summary stats')
    
#--------------------------
#  Add prediction exercise code
#--------------------------

ins_years = [2009, 2010, 2011]
oos_years = [2012, 2013]

if run_predictions==True:
    print('Starting predictions')
    exec(open(root + 'Code/predicting_flows_data_pull.py').read())
    # WE have been running the actual predictions (coded in the script below) in parallel using the following script: NetworksGit\Code\bash_parallel_jobtransitions_market_based_mutiple_mkts.sh
    # XX Code ran successfully through here on 8/11/2023. Failed somewhere in in the next script
    #exec(open(root + 'Code/parallel_jobtransitions_market_based_multiple_mkts.py').read())
    #exec(open(root + 'Code/parallel_jobtransitions_stack_results.py').read())
    subprocess.Popen([root + 'Code/bash_parallel_jobtransitions_market_based_mutiple_mkts.sh'], shell=True)
    print('Finished predictions')

    
#--------------------------
#  ANALYSIS
#-------------------------


if run_model_fit==True:
    print('Starting model fit')
    exec(open(root + 'Code/model_fit.py').read())
    print('Finished model fit')


if run_reduced_form==True:
    print('Starting reduced form')
    exec(open(root + 'Code/reduced_form.py').read())
    print('Finished reduced form')

if run_shock_case_study==True:
    print('Starting shock case study')
    exec(open(root + 'Code/shock_case_study.py').read())
    print('Finished shock case study')
    # XX The shock case study is currently failing, but I think we need to figure out what we're actually trying to do with it before actually fixing the code. The error is below. 
    # FileNotFoundError: [Errno 2] No such file or directory: '/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Data/dgp/dgp_equi_AccomFood.p'




#--------------------------
#  Some summary stats about occ4s and kmeans
#--------------------------
# XX 1/17/2024: not currently relevant but not ready to delete quite yet
#data_full = pd.read_csv(root + 'Data/RAIS_exports/earnings_panel/" + filename_stub + "_w_kmeans.csv')
#n_kmeans = data_full.kmeans.value_counts().shape[0]
#print(n_kmeans, " kmeans groups remaining after dropping those with fewer than 5000 observations")




#--------------------------
#  Miscellaneous
#--------------------------

# XX These two should probably be combined. I think I should put everything that actually makes it into the paper into summary_stats.py and everything else in other_descriptive_stats.py
exec(open(root + 'Code/summary_stats.py').read())
# Stuff in this script doesn't make it into the final paper except for some ocupation transition rate stats that I think we should cut. Need to make some edits to the code before it will run so commenting it out for now. 
exec(open(root + 'Code/other_descriptive_stats.py').read())


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

