

from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os

import bisbm
import functions

homedir = os.path.expanduser('~')
os.chdir(homedir + '/labormkt/labormkt_rafaelpereira/aug2022/code/')

run_sbm = False
maxrows=None


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


def pull_one_year(year, occvar, savefile=None, municipality_codes=None, nrows=None, othervars=None):
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

if run_sbm==True:
    firstyear = 1996
    lastyear = 1999    
    for m in micro_code_list:
        for year in range(firstyear,lastyear+1):
            pull_one_year(year, 'cbo1994', savefile='../dump/raw_data_sbm_' + str(m) + '_' + str(year) + '.p',municipality_codes=dict[m]['muni_codes'])

################################################################################################
# Append raw data for SBM

if run_sbm==True:
    for m in micro_code_list:
        print('Now appending micro code: ', m)
        for year in range(firstyear,lastyear+1):
            df = pickle.load( open('../dump/raw_data_sbm_' + str(m) + '_' + str(year) + '.p', "rb" ) )
            if year>firstyear:
                appended = df.append(appended, sort=True)
            else:
                appended = df
            del df
        appended.to_pickle('../dump/appended_sbm_'+str(m)+'.p')
    
################################################################################################
# Run SBM
if run_sbm==True:
    max_idx = len(micro_code_list)
    idx = 1
    for m in micro_code_list:
        print('Now running micro region ', m,', ', idx, '/', max_idx)
        modelname = str(m)
        appended = pd.read_pickle('../dump/appended_sbm_'+str(m)+'.p')
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
        idx +=1
    
    
    
 
################################################################################################
# Stack results of all SBMs. (Will need to append micro code to iotas and gammas to distinguish between them


if run_sbm==True:
    idx = 1
    max_idx = len(micro_code_list)
    for m in micro_code_list:
        print('Now running micro region ', m,', ', idx, '/', max_idx)
        modelname = str(m)
        model = pickle.load( open('../data/model_'+modelname+'.p', "rb" ) )
        job_blocks    = pd.read_csv('../data/model_'+modelname+'_jblocks.csv', usecols=['jid','job_blocks_level_0'])
        worker_blocks = pd.read_csv('../data/model_'+modelname+'_wblocks.csv', usecols=['wid','worker_blocks_level_0'], dtype={'wid':str})
        # Copied
        gammas_temp = job_blocks[['jid']]
        iotas_temp = worker_blocks[['wid']]
        gammas_temp['code_micro'] = m
        iotas_temp['code_micro'] = m    
        # Leaving flexibility to allow for other levels other than 0 but may want to simplify
        for l in [0]:         #range(model.L)
            print(l)
            oldname = 'job_blocks_level_' + str(l)
            newname = 'gamma_level_' + str(l)
            temp = job_blocks[oldname] + 1
            gammas_temp[newname] = str(m) + '_' +  temp.astype(str)
            oldname = 'worker_blocks_level_' + str(l)
            newname = 'iota_level_' + str(l)
            w_min = w_min = worker_blocks[oldname].min()
            temp = worker_blocks[oldname] + 1 - w_min
            iotas_temp[newname] = str(m) + '_' + temp.astype(str)
        
        if idx==1:
            gammas = gammas_temp
            iotas = iotas_temp
        else:
            gammas = pd.concat([gammas,gammas_temp])
            iotas  = pd.concat([iotas,iotas_temp])
        idx+=1

    pickle.dump( gammas, open('../data/gammas.p', "wb" ) )
    pickle.dump( iotas,  open('../data/iotas.p',  "wb" ) )
    
gammas = pickle.load(open('../data/gammas.p', "rb" ) )
iotas  = pickle.load(open('../data/iotas.p',  "rb" ) )


################################################################################################
# Create earnings panel
print('Starting earnings panel')

exchange_rate_2010 = 1.7606

cpi = pd.read_csv(homedir + '/labormkt/labormkt_rafaelpereira/ExternalData/BRACPIALLMINMEI.csv', parse_dates=['date'], names=['date','cpi'], header=0)
cpi['cpi'] = cpi.cpi/100
cpi['year'] = cpi['date'].dt.year
cpi['month'] = cpi['date'].dt.month
cpi['cpi_2010'] = cpi.cpi/cpi.loc[cpi.year==2010].cpi.mean()

'''
firstyear = 1999
lastyear  = 2010
for year in range(firstyear, lastyear+1):
    if year <= 2002:
        occvar = 'cbo1994'
    else:
        occvar = 'cbo2002'
    data = pull_one_year(year, occvar, othervars=['rem_dez_r'])
    if year==firstyear:
        earnings_panel = data[['wid','year','rem_dez_r']]
    else:
        earnings_panel = pd.concat([earnings_panel,data[['wid','year','rem_dez_r']]])

earnings_panel = earnings_panel.merge(cpi.loc[cpi['month']==12][['cpi_2010','year']], on='year', how='left')
earnings_panel['earnings_dec_2010usd'] = (earnings_panel['rem_dez_r']/(earnings_panel['cpi_2010']))/exchange_rate_2010
earnings_panel.drop(columns=['rem_dez_r','cpi_2010'], inplace=True)

# NEXT STEP: COMPUTE CUMULATIVE EARNINGS BY WID
#XX CHECK THIS CODE
earnings_panel['earnings_1999_2010usd'] = earnings_panel['earnings_dec_2010usd']*(earnings_panel.year==1999)
earnings_by_wid_cumul = earnings_panel.groupby(['wid']).earnings_dec_2010usd.sum().reset_index()
earnings_by_wid_cumul.rename(columns={'earnings_dec_2010usd':'earnings_dec_2010usd_cumul'}, inplace=True)
earnings_by_wid_cumul['earnings_1999_2010usd'] = earnings_panel.groupby(['wid']).earnings_1999_2010usd.sum().values
earnings_by_wid_cumul = earnings_by_wid_cumul.loc[earnings_by_wid_cumul.earnings_1999_2010usd>0]
earnings_by_wid_cumul['earnings_by_wid_cumul_norm1999'] = earnings_by_wid_cumul.earnings_dec_2010usd_cumul/earnings_by_wid_cumul.earnings_1999_2010usd



pickle.dump( earnings_panel,  open('../data/earnings_panel.p', "wb" ) )
pickle.dump( earnings_by_wid_cumul,  open('../data/earnings_by_wid_cumul.p', "wb" ) )
del earnings_panel
'''
earnings_by_wid_cumul = pickle.load(  open('../data/earnings_by_wid_cumul.p', "rb" ) )
'''
data_1999 = pull_one_year(1999, 'cbo1994', othervars=['clas_cnae', 'cnpj_raiz', 'genero', 'grau_instr', 'horas_contr', 'id_estab', 'idade', 'rem_med_r', 'rem_dez_r', 'raca_cor'])


data_1999 = data_1999.merge(earnings_by_wid_cumul, on='wid', how='left', validate='m:1', indicator=False)
data_1999 = data_1999.merge(muni_micro_cw, how='left', on='codemun', validate='m:1')

data_1999 = data_1999.merge(iotas, how='left', on=['wid','code_micro'], validate='m:1', indicator='_merge_iota')
data_1999 = data_1999.merge(gammas, how='left', on=['jid','code_micro'], validate='m:1', indicator='_merge_gamma')
data_1999._merge_iota.value_counts()
data_1999._merge_gamma.value_counts()
'''

#pickle.dump( data_1999,  open('../data/data_1999.p', "wb" ) )
data_1999 = pd.read_pickle('../data/data_1999.p')




################################################################################################
# Values for Bernardo's Stata code

data_1999_nomiss_ig = data_1999.loc[(data_1999.gamma_level_0.isnull()==False) & (data_1999.iota_level_0.isnull()==False)] 


data_1999_nomiss_ig_sorted = data_1999_nomiss_ig.sort_values(by=['wid','rem_dez_r'], ascending=False, na_position='last')
data_1999_nomiss_ig_sorted = data_1999_nomiss_ig_sorted.reset_index().drop(columns='index')

# XX Re-run from here
analysis_data = data_1999_nomiss_ig_sorted.drop_duplicates(subset=['wid'], keep='first')

#Merge on trade data Bernardo created in Stata
cnae_trade_short = pd.read_stata('../external/cnae_trade_short.dta')
cnae_trade_short.fillna(0, inplace=True)


# This data set has the variables Delta_X_hat_isic3  Delta_I_hat_isic3  Delta_X_isic3  Delta_I_isic3 for each CNAE. What exactly is the interpretation and units of these variables?
#- Delta_X is 2010 Brazil->China exports - 2000 Brazil->China exports measured in 1000s of $2010 USD 
#- Delta_I is 2010 China->Brazil exports - 2000 China->Brazil imports measured in 1000s of $2010 USD 
#- Delta_X_hat is the 2000 to 2010 change in Brazil->China exports controlling for worldwide shocks to industry j (in $1000)
#- Delta_I_hat is the 2000 to 2010 change in China->Brazil imports controlling for worldwide shocks to industry j (in $1000)


# Merge trade data onto primary analysis data
analysis_data['cnae4'] = np.floor(analysis_data['clas_cnae']/10)
analysis_data = analysis_data.merge(cnae_trade_short, left_on='cnae4', right_on='cnae', how='left', validate='m:1', indicator='trade_merge')



d = {}
for var in ['isic3','gamma_level_0','iota_level_0','occ4']:
    print(var)
    # Earnings
    d['earnings_by_'+var]  = analysis_data.groupby([var]).earnings_1999_2010usd.sum().reset_index().rename(columns={'earnings_1999_2010usd':'total_earnings_'+var})
    d['earnings_by_'+var+'_micro'] = analysis_data.groupby([var,'code_micro']).earnings_1999_2010usd.sum().reset_index().rename(columns={'earnings_1999_2010usd':'total_earnings_'+var+'_micro'})
    if var is not 'isic3':
        d['earnings_by_'+var+'_isic3'] = analysis_data.groupby([var,'isic3']).earnings_1999_2010usd.sum().reset_index().rename(columns={'earnings_1999_2010usd':'total_earnings_'+var+'_isic3'})
    # Employment counts
    d['emp_by_'+var]  = analysis_data.groupby([var]).earnings_1999_2010usd.count().reset_index().rename(columns={'earnings_1999_2010usd':'total_emp_'+var})
    d['emp_by_'+var+'_micro'] = analysis_data.groupby([var,'code_micro']).earnings_1999_2010usd.count().reset_index().rename(columns={'earnings_1999_2010usd':'total_emp_'+var+'_micro'})
    if var is not 'isic3':
        d['emp_by_'+var+'_isic3'] = analysis_data.groupby([var,'isic3']).earnings_1999_2010usd.count().reset_index().rename(columns={'earnings_1999_2010usd':'total_emp_'+var+'_isic3'})

        
d['earnings_by_micro'] = analysis_data.groupby(['code_micro']).earnings_1999_2010usd.sum().reset_index().rename(columns={'earnings_1999_2010usd':'total_earnings_micro'})
d['emp_by_micro'] = analysis_data.groupby(['code_micro']).earnings_1999_2010usd.count().reset_index().rename(columns={'earnings_1999_2010usd':'total_emp_micro'})


analysis_data = analysis_data.merge(d['earnings_by_micro'], on='code_micro', how='left', validate='m:1')
analysis_data = analysis_data.merge(d['emp_by_micro'], on='code_micro', how='left', validate='m:1')
for var in ['isic3','gamma_level_0','iota_level_0','occ4']:
    analysis_data = analysis_data.merge(d['earnings_by_'+var], on=var, how='left', validate='m:1')
    analysis_data = analysis_data.merge(d['earnings_by_'+var+'_micro'], on=[var,'code_micro'], how='left', validate='m:1')
    analysis_data = analysis_data.merge(d['emp_by_'+var], on=var, how='left', validate='m:1')
    analysis_data = analysis_data.merge(d['emp_by_'+var+'_micro'], on=[var,'code_micro'], how='left', validate='m:1')
    if var is not 'isic3':
        analysis_data = analysis_data.merge(d['earnings_by_'+var+'_isic3'], on=[var,'isic3'], how='left', validate='m:1')
        analysis_data = analysis_data.merge(d['emp_by_'+var+'_isic3'], on=[var,'isic3'], how='left', validate='m:1')





        
# Distribute ISIC3-level shocks by iota, gamma and occ4
for var in ['Delta_X_hat','Delta_I_hat','Delta_X','Delta_I']:
    for group in ['gamma_level_0','iota_level_0','occ4']:
        newvar = var + '_' + group
        analysis_data[newvar] = analysis_data.groupby([group])[var+'_isic3'].transform('mean')

export_list_earn = []
export_list_emp = []    
ll = [['X_inst','Delta_X_hat'], ['I_inst','Delta_I_hat'], ['X_exp','Delta_X'], ['I_exp','Delta_I']]
for l in ll:
    for group in ['isic3','gamma_level_0','iota_level_0','occ4']:
        analysis_data[l[0]+'_'+group+'_earn'] = (analysis_data[l[1]+'_'+group] * analysis_data['total_earnings_'+group+'_micro']) / (analysis_data['total_earnings_'+group] * analysis_data['total_earnings_micro'])
        analysis_data[l[0]+'_'+group+'_emp']  = (analysis_data[l[1]+'_'+group] * analysis_data['total_emp_'     +group+'_micro']) / (analysis_data['total_emp_'     +group] * analysis_data['total_emp_micro'])
        export_list_earn.append(l[0]+'_'+group+'_earn')
        export_list_emp.append(l[0]+'_'+group+'_emp')

analysis_data.to_csv('../data/analysis_data.csv')
analysis_data.to_pickle('../dump/analysis_data.p')
analysis_data = pd.read_pickle('../dump/analysis_data.p')

analysis_data.loc[(analysis_data.earnings_1999_2010usd!=np.inf) & (analysis_data.earnings_by_wid_cumul_norm1999!=np.inf) & (analysis_data.total_earnings_micro!=np.inf)][['codemun', 'idade', 'rem_dez_r', 'genero', 'raca_cor', 'grau_instr', 'occ4', 'earnings_dec_2010usd_cumul', 'earnings_1999_2010usd', 'earnings_by_wid_cumul_norm1999', 'code_micro', 'iota_level_0', 'gamma_level_0', 'cnae4', 'isic3', 'cnae', 'Delta_X_hat_isic3', 'Delta_I_hat_isic3', 'Delta_X_isic3', 'Delta_I_isic3', 'Delta_X_hat_gamma_level_0', 'Delta_X_hat_iota_level_0', 'Delta_X_hat_occ4', 'Delta_I_hat_gamma_level_0', 'Delta_I_hat_iota_level_0', 'Delta_I_hat_occ4', 'Delta_X_gamma_level_0', 'Delta_X_iota_level_0', 'Delta_X_occ4', 'Delta_I_gamma_level_0', 'Delta_I_iota_level_0', 'Delta_I_occ4', 'X_inst_isic3', 'X_inst_gamma_level_0', 'X_inst_iota_level_0', 'X_inst_occ4', 'I_inst_isic3', 'I_inst_gamma_level_0', 'I_inst_iota_level_0', 'I_inst_occ4', 'X_exp_isic3', 'X_exp_gamma_level_0', 'X_exp_iota_level_0', 'X_exp_occ4', 'I_exp_isic3', 'I_exp_gamma_level_0', 'I_exp_iota_level_0', 'I_exp_occ4']].to_csv('../data/analysis_data_regressions.csv')

'''
# Checking for inf in raw data
yearly_varlist = {}
for year in range(1999,2011):
    print(year)
    if ((year < 1998) | (year==2018) | (year==2019)):
        sep = ';'
    else:
        sep=','
    filename = '~/rais/RAIS/csv/brasil' + str(year) + '.csv'
    raw_data = pd.read_csv(filename, sep=sep, nrows=100)
    print(year, raw_data.columns.values)
    yearly_varlist[year] = raw_data.columns.values
'''

####################################################################################################
####################################################################################################
####################################################################################################
# Stuff not directly relevant to all of the above
####################################################################################################
####################################################################################################
####################################################################################################

'''

# CNAE is not unique within establishments or firms
estab_cnae_counts_2001 = d2001.groupby(['id_estab','clas_cnae']).size().reset_index().rename(columns={0:'count_2001'})
estab_cnae_counts_2002 = d2002.groupby(['id_estab','clas_cnae10']).size().reset_index().rename(columns={0:'count_2002'})
cnae_cw = d2001.merge(d2002, how='outer', on=['id_estab','pis'], validate='1:1', indicator=True)

# Confirm that CNAE is unique within establishments and firms
d2001.groupby(['id_estab','clas_cnae']).size().reset_index().duplicated(subset=['id_estab']).sum()
d2002.groupby(['id_estab','clas_cnae']).size().reset_index().duplicated(subset=['id_estab']).sum()

d2001.groupby(['cnpj_raiz','clas_cnae']).size().reset_index().duplicated(subset=['cnpj_raiz']).sum()
d2002.groupby(['cnpj_raiz','clas_cnae']).size().reset_index().duplicated(subset=['cnpj_raiz']).sum()






#################################################################################
# CNAE crosswalk

d2001 = pd.read_csv('~/rais/RAIS/csv/brasil2001.csv', usecols=['id_estab','cnpj_raiz','pis','clas_cnae'],    dtype={'id_estab':str,'pis':str}, nrows=None)
d2002 = pd.read_csv('~/rais/RAIS/csv/brasil2002.csv', usecols=['id_estab','cnpj_raiz','pis','clas_cnae10'],  dtype={'id_estab':str,'pis':str}, nrows=None)
 
# Drop the relatively small number of duplicate worker-estab obs
d2001 = d2001.drop_duplicates(subset=['id_estab','pis'], keep='first') 
d2002 = d2002.drop_duplicates(subset=['id_estab','pis'], keep='first') 

cnae_cw = d2001.merge(d2002, how='outer', on=['id_estab','pis'], validate='1:1', indicator=True)

ct = pd.crosstab(cnae_cw.clas_cnae,cnae_cw.clas_cnae10,dropna=True)
# There are 537 columns, which correspond to CNAE 1.0.
# There are 563 rows, which correspond to CNAE
ct.columns

max_share = ct.max(axis=1)/ct.sum(axis=1)
max_share.describe(percentiles=[.05,.1,.2,.5,.75,.8,.9])


cw = pd.DataFrame(columns=['clas_cnae','clas_cnae10'])
idx = 0
for cnae in ct.index:
    print(cnae)
    cnae10 = ct.loc[ct.index==cnae].idxmax(axis=1).values[0]
    cw.loc[idx] = [cnae, cnae10]
    idx += 1

(cw.clas_cnae==cw.clas_cnae10).mean()

'''
