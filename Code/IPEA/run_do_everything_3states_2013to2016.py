# This code creates the edgelist and runs the SBM



from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os

import bisbm
import functions
from functions import create_earnings_panel
from pull_one_year import pull_one_year

homedir = os.path.expanduser('~')
os.chdir(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/code/')

run_sbm = False
run_pull=True
run_append = True
maxrows=None

# This should create data pulls identical to 3states_2013to2016 except with the addition of clas_cnae20, ind2 and sector_IBGE. But I want to confirm that before saving over the old.
#modelname = '3states_2013to2016'
modelname = '3states_2013to2016_new'


state_codes = [31, 33, 35]
# Pull region codes
region_codes = pd.read_csv(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/external/munic_microregion_rm.csv', encoding='latin1')
muni_micro_cw = pd.DataFrame({'code_micro':region_codes.code_micro,'codemun':region_codes.code_munic//10})
code_list = region_codes.loc[region_codes.rm_dummy==1][['micro','code_micro']].drop_duplicates().sort_values(by='code_micro')
micro_code_list = code_list.code_micro.values
dict = {}
for m in micro_code_list:
    #print(m)
    value_list = [n//10 for n in region_codes.loc[region_codes.code_micro==m].code_munic.values.tolist()]
    dict[m] = {'micro':code_list.loc[code_list.code_micro==m]['micro'].iloc[0],'muni_codes':value_list}
    #print(dict[m])


# Changes to make:
# - Restrict age
# - Choose best measure of earnings

    
################################################################################################    
# Pull raw data for SBM

if run_pull==True:
    firstyear = 2013
    lastyear = 2016   
    for year in range(firstyear,lastyear+1):
        pull_one_year(year, 'cbo2002', savefile='../dump/raw_data_sbm_' + modelname + '_' + str(year) + '.p',state_codes=state_codes, age_lower=25, age_upper=55, othervars=['data_adm','data_deslig','data_nasc','tipo_salario','rem_dez_r','horas_contr','clas_cnae20'], parse_dates=['data_adm','data_deslig','data_nasc'], nrows=maxrows)

################################################################################################
# Append raw data for SBM

if run_append==True:
    for year in range(firstyear,lastyear+1):
        df = pickle.load( open('../dump/raw_data_sbm_' + modelname + '_' + str(year) + '.p', "rb" ) )
        if year>firstyear:
            appended = df.append(appended, sort=True)
        else:
            appended = df
        del df
    appended.to_pickle('../dump/appended_sbm_' + modelname + '.p')

################################################################################################
# Run SBM

appended = pd.read_pickle('../dump/appended_sbm_' + modelname + '.p')
occvar = 'cbo2002'
if run_sbm==True:
    # It's kinda inefficient to pickle the edgelist then load it from pickle but kept this for flexibility
    bipartite_edgelist = appended[['wid','jid']].drop_duplicates(subset=['wid','jid'])
    jid_occ_cw = appended[['jid',occvar]].drop_duplicates(subset=['jid',occvar])
    pickle.dump( bipartite_edgelist,  open('../data/bipartite_edgelist_'+modelname+'.p', "wb" ) )
    model = bisbm.bisbm()                                                                       
    model.create_graph(filename='../data/bipartite_edgelist_'+modelname+'.p',min_workers_per_job=5)
    model.fit(n_init=1)
    # In theory it makes more sense to save these as pickles than as csvs but I keep getting an error loading the pickle and the csv works fine
    model.export_blocks(output='../data/model_'+modelname+'_blocks.csv', joutput='../data/model_'+modelname+'_jblocks.csv', woutput='../data/model_'+modelname+'_wblocks.csv')
    pickle.dump( model, open('../data/model_'+modelname+'.p', "wb" ), protocol=4 )
        


################################################################################################
# Create earnings panel that we can use for the MLE

create_earnings_panel(modelname, appended, 2013, 2016, sbm_modelname='3states_2013to2016_mcmc')

# XX code to run to check that this worked: df1.equals(df2)
appended_new = pd.read_pickle('../dump/appended_sbm_3states_2013to2016_new.p')
appended = pd.read_pickle('../dump/appended_sbm_3states_2013to2016.p')
appended.equals(appended_new.drop(columns=['ind2','clas_cnae20','sector_IBGE'])



'''
########
# Temp: re-create model and state post-SBM since the pickle above failed:
- Re-create the model that we pass to minimize_nested_blockstate. This involves running the following lines:

    bipartite_edgelist = appended[['wid','jid']].drop_duplicates(subset=['wid','jid'])
    jid_occ_cw = appended[['jid',occvar]].drop_duplicates(subset=['jid',occvar])
    pickle.dump( bipartite_edgelist,  open('../data/bipartite_edgelist_'+modelname+'.p', "wb" ) )
    model = bisbm.bisbm()                                                                       
    model.create_graph(filename='../data/bipartite_edgelist_'+modelname+'.p',min_workers_per_job=5)

After this we probably still need to do some stuff that's in the bisbm.fit() function in order to designate nodes as workers or jobs. 

Once we have the model we need to re-create the state that is produced by minimize_nested_blockmodel(). We should be able to do this with the CSVs of worker and job block assignments. 


- Re-run the mass_layoffs code for the right states and years
- Bring in causa_deslig and define layoffs using it



# Sample code for creating a new nested block state

jblocks = pd.read_csv('../data/model_'+modelname+'_jblocks.csv')
jblocks.rename(columns=lambda x:x.replace('job_',''), inplace=True)
jblocks.rename(columns={'jid_py':'id_py','jid':'id'}, inplace=True)
wblocks = pd.read_csv('../data/model_'+modelname+'_wblocks.csv')
wblocks.rename(columns=lambda x:x.replace('worker_',''), inplace=True)
wblocks.rename(columns={'wid_py':'id_py','wid':'id'}, inplace=True)

blocks = pd.concat([jblocks, wblocks], axis=0)


bs =[]
for i in range(0,24):
    b = model.g.new_vertex_property("double")
    b = blocks['blocks_level_' + str(i)].values
    bs.append(b)
    del b

import graph_tool.all as gt
state = gt.NestedBlockState(model.g, bs=bs)    
model.state = state
model.L = 24
model.export_blocks(output='../data/test_model_'+modelname+'_blocks.csv', joutput='../data/test_model_'+modelname+'_jblocks.csv', woutput='../data/test_model_'+modelname+'_wblocks.csv')
pickle.dump( model, open('../data/model_'+modelname+'.p', "wb" ), protocol=4 )
        
j1 = pd.read_csv('../data/model_'+modelname+'_jblocks.csv', usecols=['jid','jid_py','job_blocks_level_0'])
j2 = pd.read_csv('../data/test_model_'+modelname+'_jblocks.csv', usecols=['jid','jid_py','job_blocks_level_0'])
j1.equals(j2)


I think Im close but the number of worker and job blocks past block 0 seem off. Need to figure out whats going on there. The issue is in the gt.NestedBlockState() function. But maybe this just isnt worth worrying about if level 0 checks out. 

'''
