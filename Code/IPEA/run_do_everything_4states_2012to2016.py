
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os

import bisbm
import functions

homedir = os.path.expanduser('~')
os.chdir(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/code/')

run_sbm = True
maxrows=None

modelname = '4states_2012to2016'


state_codes = [31, 32, 33, 35]
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


def pull_one_year(year, occvar, savefile=None, municipality_codes=None, state_codes=None, nrows=None, othervars=None, age_upper=None, age_lower=None):
    print(year)
    now = datetime.now()
    currenttime = now.strftime('%H:%M:%S')
    print('Starting ', year, ' at ', currenttime)
    if ((year < 1998) | (year==2018) | (year==2019)):
        sep = ';'
    else:
        sep=','
    vars = ['pis','id_estab',occvar,'codemun','tipo_vinculo','idade'] 
    if othervars is not None:
        vars = vars + othervars
    filename = '~/rais/RAIS/csv/brasil' + str(year) + '.csv'
    raw_data = pd.read_csv(filename, usecols=vars, sep=sep, dtype={'id_estab':str, 'pis':str, occvar:str}, nrows=nrows)
    if municipality_codes is not None:
        raw_data = raw_data[raw_data['codemun'].isin(municipality_codes)]   
    if state_codes is not None:
        raw_data = raw_data[raw_data['codemun'].fillna(99).astype(str).str[:2].astype('int').isin(state_codes)]
    if age_lower is not None:
        raw_data = raw_data.loc[raw_data.idade>age_lower]
    if age_upper is not None:
        raw_data = raw_data.loc[raw_data.idade<age_upper]
    raw_data = raw_data.dropna(subset=['pis','id_estab',occvar])
    raw_data = raw_data[~raw_data['tipo_vinculo'].isin([30,31,35])]
    raw_data['year'] = year
    raw_data['yob'] = raw_data['year'] - raw_data['idade']
    raw_data['occ4'] = raw_data[occvar].str[0:4]
    raw_data['jid'] = raw_data['id_estab'] + '_' + raw_data['occ4']
    #raw_data['jid6'] = raw_data['id_estab'] + '_' + raw_data['cbo2002']
    raw_data.rename(columns={'pis':'wid'}, inplace=True)
    if savefile is not None:
        pickle.dump( raw_data, open(savefile, "wb" ) )
    else:
        return raw_data

# Changes to make:
# - Restrict age
# - Choose best measure of earnings

    
################################################################################################    
# Pull raw data for SBM
'''
if run_sbm==True:
    firstyear = 2012
    lastyear = 2016   
    for year in range(firstyear,lastyear+1):
        pull_one_year(year, 'cbo2002', savefile='../dump/raw_data_sbm_' + modelname + '_' + str(year) + '.p',state_codes=state_codes, age_lower=25, age_upper=55)

################################################################################################
# Append raw data for SBM

if run_sbm==True:
    for year in range(firstyear,lastyear+1):
        df = pickle.load( open('../dump/raw_data_sbm_' + modelname + '_' + str(year) + '.p', "rb" ) )
        if year>firstyear:
            appended = df.append(appended, sort=True)
        else:
            appended = df
        del df
    appended.to_pickle('../dump/appended_sbm_' + modelname + '.p')
'''   
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
    pickle.dump( model, open('../data/model_'+modelname+'.p', "wb" ) )
        


