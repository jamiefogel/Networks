# This code creates the edgelist and runs the SBM



from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import sys

homedir = os.path.expanduser('~')
root = homedir + '/labormkt/labormkt_rafaelpereira/NetworksGit/'
sys.path.append(root + 'Code/Modules')
os.chdir(root)

import bisbm
import functions
from functions import create_earnings_panel
from pull_one_year import pull_one_year



run_sbm = False
run_pull=True
run_append = True
maxrows=None

# This should create data pulls identical to 3states_2013to2016 except with the addition of clas_cnae20, ind2 and sector_IBGE. But I want to confirm that before saving over the old.
#modelname = '3states_2013to2016'
modelname = '3states_2013to2016_new'


state_codes = [31, 33, 35]
# Pull region codes
region_codes = pd.read_csv('./Data/raw/munic_microregion_rm.csv', encoding='latin1')
muni_micro_cw = pd.DataFrame({'code_micro':region_codes.code_micro,'codemun':region_codes.code_munic//10})
muni_meso_cw  = pd.DataFrame({'code_meso': region_codes.code_meso, 'codemun':region_codes.code_munic//10})

code_list = region_codes.loc[region_codes.rm_dummy==1][['micro','code_micro']].drop_duplicates().sort_values(by='code_micro')
micro_code_list = code_list.code_micro.values
dict = {}
for m in micro_code_list:
    value_list = [n//10 for n in region_codes.loc[region_codes.code_micro==m].code_munic.values.tolist()]
    dict[m] = {'micro':code_list.loc[code_list.code_micro==m]['micro'].iloc[0],'muni_codes':value_list}



# Changes to make:
# - Restrict age
# - Choose best measure of earnings




################################################################################################    
# Pull raw data for SBM

if run_pull==True:
    firstyear = 2013
    lastyear = 2016   
    for year in range(firstyear,lastyear+1):
        pull_one_year(year, 'cbo2002', savefile='./Data/derived/raw_data_sbm_' + modelname + '_' + str(year) + '.p',state_codes=state_codes, age_lower=25, age_upper=55, othervars=['data_adm','data_deslig','tipo_salario','rem_dez_r','horas_contr','clas_cnae20'], parse_dates=['data_adm','data_deslig'], nrows=maxrows)

################################################################################################
# Append raw data for SBM

if run_append==True:
    for year in range(firstyear,lastyear+1):
        df = pickle.load( open('./Data/derived/raw_data_sbm_' + modelname + '_' + str(year) + '.p', "rb" ) )
        if year==firstyear:
            appended = df
        else:
            appended = df.append(appended, sort=True)
        del df
    appended.to_pickle('./Data/derived/appended_sbm_' + modelname + '.p')

# Create occ2Xmeso variable
appended = pd.read_pickle('./Data/derived/appended_sbm_' + modelname + '.p')
appended = appended.merge(muni_meso_cw, how='left', on='codemun', copy=False) # validate='m:1', indicator=True)
appended['occ2Xmeso'] = appended.cbo2002.str[0:2] + '_' + appended['code_meso'].astype('str')


################################################################################################
# Run SBM

occvar = 'cbo2002'
if run_sbm==True:
    # It's kinda inefficient to pickle the edgelist then load it from pickle but kept this for flexibility
    bipartite_edgelist = appended[['wid','jid']].drop_duplicates(subset=['wid','jid'])
    jid_occ_cw = appended[['jid',occvar]].drop_duplicates(subset=['jid',occvar])
    pickle.dump( bipartite_edgelist,  open('./Data/derived/bipartite_edgelist_'+modelname+'.p', "wb" ) )
    model = bisbm.bisbm()                                                                       
    model.create_graph(filename='./Data/derived/bipartite_edgelist_'+modelname+'.p',min_workers_per_job=5)
    model.fit(n_init=1)
    # In theory it makes more sense to save these as pickles than as csvs but I keep getting an error loading the pickle and the csv works fine
    model.export_blocks(output='./Data/derived/sbm_output/model_'+modelname+'_blocks.csv', joutput='./Data/derived/sbm_output/model_'+modelname+'_jblocks.csv', woutput='./Data/derived/sbm_output/model_'+modelname+'_wblocks.csv')
    pickle.dump( model, open('./Data/derived/sbm_output/model_'+modelname+'.p', "wb" ), protocol=4 )
else:        
    model = pickle.load( open('./Data/derived/sbm_output/model_3states_2013to2016_mcmc.p', "rb" ))


################################################################################################
# Create earnings panel that we can use for the MLE

create_earnings_panel(modelname, appended, 2013, 2016, sbm_modelname='3states_2013to2016_mcmc')
<<<<<<< HEAD
=======

>>>>>>> main
