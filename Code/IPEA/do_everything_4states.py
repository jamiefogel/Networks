

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
    
################################################################################################
# Run SBM

appended = pd.read_pickle('../dump/appended_sbm_' + modelname + '.p')
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
        
    
 
################################################################################################
# Stack results of all SBMs. (Will need to append micro code to iotas and gammas to distinguish between them

# This is for stacking results when we ran SBMs separately for each micro region. Not relevant when we ran it for 4 states at once. Basically, we need to create new iotas/gammas that concatenate the original iota/gamma with the micro region since iotas/gammas are nested within micros
multiple_sbms = False
if (run_sbm==True & multiple_sbms==True):
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


if multiple_sbms==False:
    model = pickle.load( open('../data/model_'+modelname+'.p', "rb" ) )
    model.state = pickle.load(open('../data/state_mcmc_iters.p', "rb"))[0]
    model.export_blocks(output='../data/model_'+modelname+'_blocks.csv', joutput='../data/model_'+modelname+'_jblocks.csv', woutput='../data/model_'+modelname+'_wblocks.csv')
    job_blocks    = pd.read_csv('../data/model_'+modelname+'_jblocks.csv', usecols=['jid','job_blocks_level_0'])
    worker_blocks = pd.read_csv('../data/model_'+modelname+'_wblocks.csv', usecols=['wid','worker_blocks_level_0'], dtype={'wid':str})

iotas = worker_blocks.rename(columns={'worker_blocks_level_0':'iota_level_0'})
gammas = job_blocks.rename(columns={'job_blocks_level_0':'gamma_level_0'})


################################################################################################
# Create earnings panel
print('Starting earnings panel')

exchange_rate_2010 = 1.7606

cpi = pd.read_csv(homedir + '/labormkt/labormkt_rafaelpereira/ExternalData/BRACPIALLMINMEI.csv', parse_dates=['date'], names=['date','cpi'], header=0)
cpi['cpi'] = cpi.cpi/100
cpi['year'] = cpi['date'].dt.year
cpi['month'] = cpi['date'].dt.month
cpi['cpi_2010'] = cpi.cpi/cpi.loc[cpi.year==2010].cpi.mean()


firstyear = 1999
lastyear  = 2010
for year in range(firstyear, lastyear+1):
    if year <= 2002:
        occvar = 'cbo1994'
    else:
        occvar = 'cbo2002'
    data = pull_one_year(year, occvar,state_codes=state_codes, othervars=['rem_dez_r'])
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



pickle.dump( earnings_panel,  open('../data/earnings_panel_' + modelname + '.p', "wb" ) )
pickle.dump( earnings_by_wid_cumul,  open('../data/earnings_by_wid_cumul_' + modelname + '.p', "wb" ) )
#earnings_by_wid_cumul = pickle.load( open('../data/earnings_by_wid_cumul_' + modelname + '.p', "rb" ) )
del earnings_panel


data_1999 = pull_one_year(1999, 'cbo1994', state_codes=state_codes, othervars=['clas_cnae', 'cnpj_raiz', 'genero', 'grau_instr', 'horas_contr', 'id_estab', 'idade', 'rem_med_r', 'rem_dez_r', 'raca_cor'])


data_1999 = data_1999.merge(earnings_by_wid_cumul, on='wid', how='left', validate='m:1', indicator=False)
data_1999 = data_1999.merge(muni_micro_cw, how='left', on='codemun', validate='m:1')

data_1999 = data_1999.merge(iotas, how='left', on=['wid'], validate='m:1', indicator='_merge_iota')
data_1999 = data_1999.merge(gammas, how='left', on=['jid'], validate='m:1', indicator='_merge_gamma')
data_1999._merge_iota.value_counts()
data_1999._merge_gamma.value_counts()


pickle.dump( data_1999,  open('../data/data_1999_' + modelname + '.p', "wb" ) )
#data_1999 = pd.read_pickle('../data/data_1999_' + modelname + '.p')

'''
earnings_by_wid_cumul.earnings_by_wid_cumul_norm1999.describe()
count    1.107328e+07
mean     5.704351e+01
std      7.184055e+03
min      1.000000e+00
25%      3.863070e+00
50%      8.903092e+00
75%      1.401193e+01
max      6.280989e+06
Name: earnings_by_wid_cumul_norm1999, dtype: float64
'''

################################################################################################
# Values for Bernardo's Stata code

data_1999_nomiss_ig = data_1999.loc[(data_1999.gamma_level_0.isnull()==False) & (data_1999.iota_level_0.isnull()==False)] 


data_1999_nomiss_ig_sorted = data_1999_nomiss_ig.sort_values(by=['wid','rem_dez_r'], ascending=False, na_position='last')
data_1999_nomiss_ig_sorted = data_1999_nomiss_ig_sorted.reset_index().drop(columns='index')

analysis_data = data_1999_nomiss_ig_sorted.drop_duplicates(subset=['wid'], keep='first')

# Left off here before lunch on 10/28
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

# We have 54 obs wehre cnae4 is missing or 0. Drop them.
analysis_data = analysis_data.loc[(analysis_data.cnae4!=0) & (analysis_data.cnae4.isna()==False)]

analysis_data = analysis_data.merge(cnae_trade_short, left_on='cnae4', right_on='cnae', how='left', validate='m:1', indicator='trade_merge')

analysis_data['code_micro'] = analysis_data.code_micro.astype(int)
analysis_data['occ4Xmicro'] = analysis_data.occ4 + '_' + analysis_data.code_micro.astype(str)

analysis_data['occ2'] = analysis_data.cbo1994.str.strip().str[0:2]
analysis_data['occ2Xmicro'] = analysis_data.occ2 + '_' + analysis_data.code_micro.astype(str)

analysis_data['occ2Xmicro_count'] = analysis_data.groupby('occ2Xmicro')['occ2Xmicro'].transform('count')
analysis_data['gamma_level_0_count'] = analysis_data.groupby('gamma_level_0')['gamma_level_0'].transform('count')
# Restrict to markets with at least 500 people according to each definition
analysis_data = analysis_data.loc[(analysis_data.occ2Xmicro_count>500) & (analysis_data.gamma_level_0_count>500)]

# Occ2, Occ4, and Occ5 probably all make sense for CBO1994

# Some quick checks: If we define a market as an occ2Xmicro and restrict to marekts with >500 obs, we have 2312 markets and retain ~92% of obs. For now let's go with this.
#np.sum(analysis_data.occ2Xmicro.value_counts()>500)
#analysis_data.loc[analysis_data.occ2Xmicro_count>500].shape[0]


# Indicators for working in a directly-shocked firm (separatly for imports and exports)
analysis_data['directly_shocked_I'] = analysis_data.Delta_I_isic3>0
analysis_data['directly_shocked_X'] = analysis_data.Delta_X_isic3>0 


# Creating earnings and employment by ISIC3, market, and ISIC3-Market. These are the quantities denoted by L (and various subscripts) in the Costa et al paper. These will not differ for imports vs exports. 
market_defns = ['gamma_level_0','occ2Xmicro']
d = {}
for var in market_defns + ['isic3']:
    print(var)
    # Earnings
    d['earnings_by_'+var]  = analysis_data.groupby([var]).earnings_1999_2010usd.sum().reset_index().rename(columns={'earnings_1999_2010usd':'total_earnings_'+var})
    if var is not 'isic3':
        d['earnings_by_'+var+'_isic3'] = analysis_data.groupby([var,'isic3']).earnings_1999_2010usd.sum().reset_index().rename(columns={'earnings_1999_2010usd':'total_earnings_'+var+'_isic3'})
    # Employment counts
    d['emp_by_'+var]  = analysis_data.groupby([var]).earnings_1999_2010usd.count().reset_index().rename(columns={'earnings_1999_2010usd':'total_emp_'+var})
    if var is not 'isic3':
        d['emp_by_'+var+'_isic3'] = analysis_data.groupby([var,'isic3']).earnings_1999_2010usd.count().reset_index().rename(columns={'earnings_1999_2010usd':'total_emp_'+var+'_isic3'})

'''
'earnings_by_gamma_level_0'        - L_m
'earnings_by_gamma_level_0_isic3'  - L_mj
'emp_by_gamma_level_0'             - L_m
'emp_by_gamma_level_0_isic3'       - L_mj
'earnings_by_occ2Xmicro'           - L_m 
'earnings_by_occ2Xmicro_isic3'     - L_mj
'emp_by_occ2Xmicro'                - L_m
'emp_by_occ2Xmicro_isic3'          - L_mj
'earnings_by_isic3'                - L_j
'emp_by_isic3'                     - L_j
'''

# Create XD_m and IS_m
for mkt in market_defns:
    for e in ['earnings','emp']:
        temp = d[e+'_by_'+mkt+'_isic3'].merge(d[e+'_by_isic3'], on='isic3').merge(d[e+'_by_'+mkt], on=mkt).merge(cnae_trade_short.drop(columns='cnae').drop_duplicates(), on='isic3')
        for trade in ['X', 'I']:
            for hat in ['_hat','']:
                # This creates the object inside the sum from Costa et al. Defined at the m-j level
                temp[trade+hat+'_'+mkt+'_isic3'+'_'+e] = (temp['total_'+e+'_'+mkt+'_isic3']/temp['total_'+e+'_'+mkt]) * (temp['Delta_'+trade+hat+'_isic3'] / temp['total_'+e+'_isic3'])
                # This sums across j to get market-level exposure (XD_m and IS_m)
                temp[trade+hat+'_'+mkt+'_'+e] = temp.groupby(mkt)[trade+hat+'_'+mkt+'_isic3'+'_'+e].transform('sum')
        analysis_data = analysis_data.merge(temp[['isic3', mkt, 'X_hat_'+mkt+'_isic3_'+e, 'X_hat_'+mkt+'_'+e, 'X_'+mkt+'_isic3_'+e,'X_'+mkt+'_'+e, 'I_hat_'+mkt+'_isic3_'+e, 'I_hat_'+mkt+'_'+e, 'I_'+mkt+'_isic3_'+e, 'I_'+mkt+'_'+e]], on=['isic3',mkt],how='left',validate='m:1')


# This creates an industry-level exposure measure akin to ADHS. It's equivalent to the second fraction from the Costa et al exposure measure
for e in ['earnings','emp']:
    # Create an ISIC3-level data set with trade data and employment/earnings
    temp_isic3 = d[e+'_by_isic3'].merge(cnae_trade_short.drop(columns='cnae').drop_duplicates(), on='isic3', validate='1:1')
    for trade in ['X', 'I']:
        for hat in ['_hat','']:
            temp_isic3[trade+hat+'_isic3'+'_'+e] = (temp_isic3['Delta_'+trade+hat+'_isic3'] / temp_isic3['total_'+e+'_isic3'])
    analysis_data = analysis_data.merge(temp_isic3[['isic3', 'X_hat_isic3_' + e, 'X_isic3_' + e, 'I_hat_isic3_' + e, 'I_isic3_' + e]], on='isic3',how='left',validate='m:1')
        
        


# Subset for Stata
vars = ['codemun', 'idade', 'genero', 'raca_cor', 'grau_instr', 'occ2','occ2Xmicro', 'occ4','occ4Xmicro',  'earnings_by_wid_cumul_norm1999', 'earnings_dec_2010usd_cumul', 'iota_level_0', 'gamma_level_0', 'cnae4', 'isic3', 'cnae', 'directly_shocked_I', 'directly_shocked_X', 'occ2Xmicro_count', 'gamma_level_0_count'] + [i for i in analysis_data.columns if i.startswith('X_')]  + [i for i in analysis_data.columns if i.startswith('I_')]

analysis_data.loc[(analysis_data.idade>=25) & (analysis_data.idade<=55) & (analysis_data.earnings_by_wid_cumul_norm1999!=np.inf) & (analysis_data.earnings_by_wid_cumul_norm1999!=np.nan)][vars].to_stata('../data/analysis_data_subset.dta')

analysis_data.to_pickle('../dump/analysis_data.p')
analysis_data = pd.read_pickle('../dump/analysis_data.p')



###########################################################################################
# Summary stats

analysis_data[[i for i in analysis_data.columns if i.startswith('X_')]  + [i for i in analysis_data.columns if i.startswith('I_')]].describe()

pd.options.display.float_format = '{:.4f}'.format

################
# By earnings 
print(analysis_data[['X_gamma_level_0_earnings','X_occ2Xmicro_earnings','I_gamma_level_0_earnings','I_occ2Xmicro_earnings']].corr().to_string(header=False))

analysis_data[['X_gamma_level_0_earnings','X_occ2Xmicro_earnings','I_gamma_level_0_earnings','I_occ2Xmicro_earnings']].describe()
# More variation when using occ2Xmicro. But I think gammas have much more uniform size distributions so this could be playing into it. I wonder if the story would change if we weighted by market size.

################
# By employment
print(analysis_data[['X_gamma_level_0_emp','X_occ2Xmicro_emp','I_gamma_level_0_emp','I_occ2Xmicro_emp']].corr().to_string(header=False))

analysis_data[['X_gamma_level_0_emp','X_occ2Xmicro_emp','I_gamma_level_0_emp','I_occ2Xmicro_emp']].describe()
# More variation when using occ2Xmicro. But I think gammas have much more uniform size distributions so this could be playing into it. I wonder if the story would change if we weighted by market size.


occ2Xmicro_size = analysis_data.groupby('occ2Xmicro')['occ2Xmicro'].count()
gamma_level_0_size = analysis_data.groupby('gamma_level_0')['gamma_level_0'].count()

occ2Xmicro_size.describe()
gamma_level_0_size.describe()


np.sum(analysis_data.occ2Xmicro.value_counts()>500)
analysis_data.loc[analysis_data.occ2Xmicro_count>500].shape[0]

np.sum(analysis_data.gamma_level_0.value_counts()>500)
analysis_data.loc[analysis_data.gamma_level_0_count>500].shape[0]


# Scatterplot

'X_hat_gamma_level_0_isic3_earnings'
'X_hat_gamma_level_0_earnings',
'X_hat_gamma_level_0_isic3_emp'
'X_hat_gamma_level_0_emp',
'X_hat_occ2Xmicro_isic3_earnings'
'X_hat_occ2Xmicro_earnings',
'X_hat_occ2Xmicro_isic3_emp'
'X_hat_occ2Xmicro_emp',
'X_hat_isic3_emp',

'X_gamma_level_0_isic3_earnings'
'X_gamma_level_0_earnings',
'X_gamma_level_0_isic3_emp'
'X_gamma_level_0_emp',
'X_occ2Xmicro_isic3_earnings'
'X_occ2Xmicro_earnings',
'X_occ2Xmicro_isic3_emp'
'X_occ2Xmicro_emp'
'X_isic3_emp'


analysis_data[['X_gamma_level_0_earnings','X_occ2Xmicro_earnings','I_gamma_level_0_earnings','I_occ2Xmicro_earnings']].corr()
