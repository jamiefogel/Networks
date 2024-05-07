

# conda activate gt

import os
import sys
import pandas as pd
import numpy as np
import pickle
from datetime import datetime
import getpass
import platform
import subprocess

now = datetime.now()
dt_string = now.strftime("%Y_%m_%d__%H_%M_%S")
print("date and time =", dt_string)	

homedir = os.path.expanduser('~')
os_name = platform.system()
if getpass.getuser()=='p13861161':
    print("Running on Linux") 
    if os_name == 'Windows':
        root = "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/"
        rais = "//storage6/bases/DADOS/RESTRITO/RAIS/"
        sys.path.append(root + 'Code/Modules')
    elif os_name == 'Linux':
        root = "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/"
        rais = "~/rais/RAIS/"
        sys.path.append(root + 'Code/Modules')
        # These all require torch
        import torch
        from torch_mle import torch_mle
        import bisbm
        from mle_load_fulldata import mle_load_fulldata
        from normalization_k import normalization_k
        from alphas_func import load_alphas
        import solve_model_functions as smf
        from correlogram import correlogram

if getpass.getuser()=='jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'
    sys.path.append(root + 'Code/Modules')


figuredir = root + 'Results/'

os.chdir(root)

sector_data_filepath = root + "Data/raw/IBGE/Conta_da_producao_2002_2017_xls"


#from create_earnings_panel import create_earnings_panel
from create_earnings_panel_trade_shock import create_earnings_panel_trade_shock
from pull_one_year import pull_one_year


import matplotlib.pyplot as plt
from occ_counts_by_type import occ_counts_by_type
from concentration_figures import concentration_figures


# Change all the filepaths to the new data

################################################################
## STRUCTURAL OBJECTS
################################################################


#####################################
# Options from IPEA/do_all_ipea.py

run_sbm = False
run_sbm_mcmc = False
run_pull = False
run_append = False
run_create_earnings_panel = True
maxrows=None
modelname = 'trade_shock'
#modelname = 'synthetic_data_3states_2009to2012'
rais_filename_stub =  rais + 'csv/brasil' 
#rais_filename_stub = root + './Data/raw/synthetic_data_'

eta = 2
firstyear_sbm = 1987
lastyear_sbm  = 1990
firstyear_panel = 1987
lastyear_panel  = 1990
state_codes = [31, 33, 35]


#####################################
# Options from do_all.py

# Default is 100. 1000 looks better. 10000 crashed the computer.
plt.rcParams['figure.dpi'] = 100
#plt.rcParams['figure.dpi'] = 1000

level = 0
#level = int(sys.argv[1])

xi_outopt_scalar = 0
phi_outopt_scalar = 0

pd.options.display.max_columns=20
pd.options.display.width=200
np.set_printoptions(linewidth=200)
np.set_printoptions(suppress=True)


run_all = True
run_mle = True
run_query_sums = True
run_normalization = True
solve_GE_silently = True
a_s_variation = True

run_occ_counts = False
run_shock_case_study=True
run_reduced_form=True
run_model_fit=False
run_concentration_figures=False
run_correlograms=False
run_predictions=False
run_gamma_summary_stats=False


worker_type_var = 'iota'
job_type_var    = 'gamma'



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
        #pull_one_year(year, 'cbo1994', savefile='./Data/derived/raw_data_sbm_' + modelname + '_' + str(year) + '.p',state_codes=None, age_lower=25, age_upper=55, nrows=maxrows, filename=rais_filename_stub + str(year) + '.csv')
        occvar = 'cbo1994'
        state_codes=None
        savefile='./Data/derived/raw_data_sbm_' + modelname + '_' + str(year) + '.p'
        filename=rais_filename_stub + str(year) + '.csv'
        print('new version')
        print(year)
        now = datetime.now()
        currenttime = now.strftime('%H:%M:%S')
        print('Starting ', year, ' at ', currenttime)
        if ((year < 1998) | (year==2016) | (year==2018) | (year==2019)):
            sep = ';'
        else:
            sep = ','
        vars = ['pis','id_estab',occvar,'codemun','fx_etaria','nat_vinculo','rem_dez_sm','subs_ibge','emp_31dez','cnpj_raiz'] 
        if filename is None:
            filename = rais + '/csv/brasil' + str(year) + '.csv'
        # 2009 csv is corrupted so use Stata instead
        if year!=2009:
            raw_data = pd.read_csv(filename, usecols=vars, sep=sep, dtype={'id_estab':str, 'pis':str, occvar:str}, nrows=maxrows)
        elif year==2009:
            filename_stata = filename.replace('.csv','.dta').replace('/csv/','/Stata/')
            raw_data = pd.read_stata(filename_stata, columns=vars)
        if state_codes is not None:
            raw_data = raw_data[raw_data['codemun'].fillna(99).astype(str).str[:2].astype('int').isin(state_codes)]
        # Drop public sector workers
        raw_data = raw_data.loc[raw_data.nat_vinculo!=2]
        # Restrict to ages 25-64
        raw_data = raw_data.loc[(raw_data.fx_etaria>=4) & (raw_data.fx_etaria<=7)]
        raw_data = raw_data.dropna(subset=['pis','id_estab',occvar])
        raw_data['year'] = year
        raw_data['occ4'] = raw_data[occvar].str[0:4]
        raw_data['jid'] = raw_data['id_estab'] + '_' + raw_data['occ4']
        #raw_data['jid6'] = raw_data['id_estab'] + '_' + raw_data['cbo2002']
        raw_data.rename(columns={'pis':'wid'}, inplace=True)
        if savefile is not None:
            pickle.dump( raw_data, open(savefile, "wb" ) )
        del raw_data
    

        
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
            appended = pd.concat([df, appended], ignore_index=True, sort=True)
        del df
    appended.to_pickle('./Data/derived/appended_sbm_' + modelname + '.p', protocol=4)

################################################################################################
# Run SBM

appended = pd.read_pickle('./Data/derived/appended_sbm_' + modelname + '.p')
occvar = 'cbo1994'
if run_sbm==True:
    print('Starting SBM section at ', datetime.now())
    # It's kinda inefficient to pickle the edgelist then load it from pickle but kept this for flexibility
    bipartite_edgelist = appended.loc[(appended['year']>=firstyear_sbm) & (appended['year']<=lastyear_sbm)][['wid','jid']].drop_duplicates(subset=['wid','jid'])
    jid_occ_cw =         appended.loc[(appended['year']>=firstyear_sbm) & (appended['year']<=lastyear_sbm)][['jid','cbo1994']].drop_duplicates(subset=['jid','cbo1994'])
    pickle.dump( bipartite_edgelist,  open('./Data/derived/bipartite_edgelist_'+modelname+'.p', "wb" ) )
    model = bisbm.bisbm()                                                                       
    model.create_graph(filename='./Data/derived/bipartite_edgelist_'+modelname+'.p',min_workers_per_job=10)
    model.fit(n_init=1)
    # In theory it makes more sense to save these as pickles than as csvs but I keep getting an error loading the pickle and the csv works fine
    model.export_blocks(output='./Data/derived/sbm_output/model_'+modelname+'_blocks.csv', joutput='./Data/derived/sbm_output/model_'+modelname+'_jblocks.csv', woutput='./Data/derived/sbm_output/model_'+modelname+'_wblocks.csv')
    pickle.dump( model, open('./Data/derived/sbm_output/model_'+modelname+'.p', "wb" ), protocol=4 )
    print('SBM section complete at ', datetime.now())

model = pickle.load( open('./Data/derived/sbm_output/model_'+modelname+'.p', "rb" ))
    
if run_sbm_mcmc==True:
    model.mcmc_sweeps('./Data/derived/sbm_output/model_'+modelname+'_mcmc.p', tempsavedir='./Data/derived/sbm_output/', numiter=1000, seed=734)
    pickle.dump( model, open('./Data/derived/sbm_output/model_'+modelname+'.p', "wb" ), protocol=4 )

gammas = pd.read_csv('./Data/derived/sbm_output/model_'+modelname+'_jblocks.csv', usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
iotas = pd.read_csv('./Data/derived/sbm_output/model_'+modelname+'_wblocks.csv', usecols=['wid','worker_blocks_level_0'], dtype={'wid': object}).rename(columns={'worker_blocks_level_0':'iota'})



################################################################################################
# Create earnings panel that we can use for the MLE

if run_create_earnings_panel==True:
    print('Starting create_earnings_panel() section at ', datetime.now())
    create_earnings_panel_trade_shock(modelname, appended, 1987, 1990, sbm_modelname='trade_shock_mcmc', sector_var='subs_ibge')
    print('create_earnings_panel() section finished  at ', datetime.now())



#####################
# Testing some stuff for compatibility with Dix-Carneiro and Kovak (2017)
'''
gammas = pd.read_csv('./Data/derived/sbm_output/model_'+modelname+'_jblocks.csv', usecols=['jid','job_blocks_level_0']).rename(columns={'job_blocks_level_0':'gamma'})
iotas = pd.read_csv('./Data/derived/sbm_output/model_'+modelname+'_wblocks.csv', usecols=['wid','worker_blocks_level_0'], dtype={'wid': object}).rename(columns={'worker_blocks_level_0':'iota'})


rais_codemun_to_mmc_1970_2010 = pd.read_stata('./Code/DixCarneiro_Kovak_2017/Data_Other/rais_codemun_to_mmc_1970_2010.dta')
rais_codemun_to_mmc_1970_2010['codemun'] = pd.to_numeric(rais_codemun_to_mmc_1970_2010['codemun'], errors='coerce')

appended = appended.merge(rais_codemun_to_mmc_1970_2010, on='codemun', how='left', indicator=True)
appended = appended.merge(iotas, on='wid', how='left', indicator='_merge_iota')
appended._merge_iota.value_counts()
appended = appended.merge(gammas, on='jid', how='left', indicator='_merge_gamma')
appended._merge_gamma.value_counts()

'''


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
# Temp definition of parameters just to make the code run
S = 15
pre=2009
exec(open(root + 'Code/load_model_parameters.py').read())

classification_list = [('iota','gamma')]
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
        if run_query_sums == 1:
            mle_load_fulldata(est_mle_data_filename, est_mle_data_sums_filename, wtype_var, jtype_var, 'real_hrly_wage_dec', est_alphas_file, mle_firstyear=firstyear_sbm, mle_lastyear=lastyear_sbm, sector_var='subs_ibge')
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

''' 

model_fit.py failed on occ4_first_recode sector_IBGE

OUTPUT BELOW:

---------------------------------------------
occ4_first_recode sector_IBGE
---------------------------------------------
 
CONVERGED!
 
CONVERGED!
/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/Modules/model_fit_func.py:54: UserWarning: To copy construct from a tensor, it is recommended to use sourceTensor.clone().detach() or sourceTensor.clone().detach().requires_grad_(True), rather than torch.tensor(sourceTensor).
  MSE = (torch.sum( wgt * (torch.tensor(x-y)**2))/torch.sum(wgt)).item()
MSE 0.0169806987330254
/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/Modules/model_fit_func.py:54: UserWarning: To copy construct from a tensor, it is recommended to use sourceTensor.clone().detach() or sourceTensor.clone().detach().requires_grad_(True), rather than torch.tensor(sourceTensor).
  MSE = (torch.sum( wgt * (torch.tensor(x-y)**2))/torch.sum(wgt)).item()
MSE 0.0169806987330254
/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/Modules/model_fit_func.py:54: UserWarning: To copy construct from a tensor, it is recommended to use sourceTensor.clone().detach() or sourceTensor.clone().detach().requires_grad_(True), rather than torch.tensor(sourceTensor).
  MSE = (torch.sum( wgt * (torch.tensor(x-y)**2))/torch.sum(wgt)).item()
MSE 0.0169806987330254
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "<string>", line 58, in <module>
  File "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/Modules/model_fit_func.py", line 290, in model_fit
    make_scatter(E_phi_no_N_pre, ln_wage_no_N_iota_pre, 'E_phi_no_N_pre', 'ln_wage_no_N_iota_pre', xlabel='Model', ylabel='Actual', xlim=(1.5,4.5), ylim=(1.5,4.5), title=r'$\Phi$ versus actual log wages; 2009, level ' + str(level), filename=figuredir + '/' + 'model_fit_cross_section_pre_level_' + worker_type_var + "_" + job_type_var + "_" + "level_" + str(level) + '_eta_' + str(eta) + '.png', printvars=False)
  File "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/Modules/model_fit_func.py", line 54, in make_scatter
    MSE = (torch.sum( wgt * (torch.tensor(x-y)**2))/torch.sum(wgt)).item()
RuntimeError: The size of tensor a (573) must match the size of tensor b (570) at non-singleton dimension 0
'''

if run_reduced_form==True:
    print('Starting reduced form')
    exec(open(root + 'Code/reduced_form.py').read())
    print('Finished reduced form')

if run_shock_case_study==True:
    print('Starting shock case study')
    exec(open(root + 'Code/shock_case_study.py').read())
    print('Finished shock case study')




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

