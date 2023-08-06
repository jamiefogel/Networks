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
from create_earnings_panel import create_earnings_panel
from pull_one_year import pull_one_year



run_sbm = True
run_pull=True
run_append = True
maxrows=None

# This should create data pulls identical to 3states_2013to2016 except with the addition of clas_cnae20, ind2 and sector_IBGE. But I want to confirm that before saving over the old.
#modelname = '3states_2013to2016'
modelname = '3states_2009to2012'


firstyear_sbm = 2009
lastyear_sbm  = 2012
firstyear_panel = 2009
lastyear_panel  = 2014
state_codes = [31, 33, 35]
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
    print('Starting pull_one_year() at ', datetime.datetime.now())
    for year in range(firstyear_panel,lastyear_panel+1):
        print(year, ' ', datetime.datetime.now())
        pull_one_year(year, 'cbo2002', savefile='./Data/derived/raw_data_sbm_' + modelname + '_' + str(year) + '.p',state_codes=state_codes, age_lower=25, age_upper=55, othervars=['data_adm','data_deslig','data_nasc','tipo_salario','rem_dez_r','horas_contr','clas_cnae20'], parse_dates=['data_adm','data_deslig','data_nasc'], nrows=maxrows)

################################################################################################
# Append raw data for SBM

if run_append==True:
    print('Starting append at ', datetime.datetime.now())
    for year in range(firstyear_panel,lastyear_panel+1):
        print(year, ' ', datetime.datetime.now())
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
    print('Starting SBM section at ', datetime.datetime.now())
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
    print('SBM section complete at ', datetime.datetime.now())


################################################################################################
# Create earnings panel that we can use for the MLE

print('Starting create_earnings_panel() section at ', datetime.datetime.now())
create_earnings_panel(modelname, appended, 2009, 2016, sbm_modelname='3states_2013to2016_mcmc')
print('create_earnings_panel() section finished  at ', datetime.datetime.now())
