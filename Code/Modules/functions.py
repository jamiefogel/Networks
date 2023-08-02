
from datetime import datetime
import pickle
import pandas as pd
import numpy as np
import os
import bisbm

def create_earnings_panel(modelname, appended, firstyear_panel, lastyear_panel, sbm_modelname=None):
    if sbm_modelname==None:
        sbm_modelname=modelname
    # Because dates aren't stored correctly in some years
    appended['data_deslig'] = pd.to_datetime(appended['data_deslig'])
    appended['data_adm'] = pd.to_datetime(appended['data_adm'])
    
    # Load CPI data
    cpi = pd.read_csv('./Data/raw/BRACPIALLMINMEI.csv', parse_dates=['date'], names=['date','cpi'], header=0)
    cpi['year'] = cpi['date'].dt.year
    cpi['month'] = cpi['date'].dt.month
    
    appended = appended.merge(cpi.loc[cpi['month']==12][['cpi','year']], on='year', how='left')
    
    # Drop jobs that do not persist for all of December
    appended['data_deslig_adj'] = appended['data_deslig']
    appended.loc[pd.isnull(appended['data_deslig']), 'data_deslig_adj'] = pd.to_datetime({'year':appended['year'],'month':12,'day':31}) + pd.to_timedelta(1, unit='D') #If missing, set to Jan 1 of the following year
    end_of_year = pd.to_datetime({'year':appended['year'],'month':12,'day':31})
    start_of_dec = pd.to_datetime({'year':appended['year'],'month':12,'day':1})
    drop = (appended['data_deslig_adj'] < end_of_year) | (appended['data_adm'] > start_of_dec)
    appended = appended.loc[drop==False]
    
    # Only keep jobs that pay on a monthly basis (most jobs)
    appended = appended.loc[appended['tipo_salario']==1]
    # Restrict to positive earnings
    appended = appended.loc[appended['rem_dez_r']>0]
    # Drop military occupations
    appended = appended.loc[appended['cbo2002'].str[0]!='0']
    
    appended['real_hrly_wage_dec'] = (appended['rem_dez_r']/(appended['horas_contr']*4.4) )/(appended['cpi']/100)
    appended['ln_real_hrly_wage_dec'] = np.log(appended['real_hrly_wage_dec'])

    # Keep jobs in terms of most hours first and then highest earnings second
    appended.sort_values(by=['wid', 'year', 'horas_contr', 'real_hrly_wage_dec'], inplace=True, ascending=True)
    appended = appended[appended.duplicated(subset=['wid', 'year'], keep='last')==False]

    # Reestrict to people who were at least 25 at the start of the panel and not older than 55 at the end
    appended = appended.loc[(appended['yob']+25<=firstyear_panel) & (appended['yob']+55>=lastyear_panel)]
    
    # Create a balanced panel
    # First create a 'spine' with one row for each worker-year pair.
    # Second merge the appended onto this spine. For now treat all variable as time-varying but may want to edit this and do two merges: one for time-varying and one for time-invariant
    unique_wids=appended['wid'].unique()
    spine = pd.DataFrame({'wid':np.tile(unique_wids, lastyear_panel+1-firstyear_panel), 'year':np.repeat(np.arange(firstyear_panel,lastyear_panel+1),unique_wids.shape[0])})        
    balanced = spine.merge(appended, how='left', on=['wid','year'], indicator=True, validate='1:1')
    del spine          
    # Why aren't these equal? Look into it later
    # unique_wids==model.num_workers
    
    
    ####################################33
    # Merge on worker and job blocks
    
    model = pickle.load(   open('./Data/derived/sbm_output/model_'+sbm_modelname+'.p', "rb" ) )        
    job_blocks    = pd.read_csv('./Data/derived/sbm_output/model_'+sbm_modelname+'_jblocks.csv')
    worker_blocks = pd.read_csv('./Data/derived/sbm_output/model_'+sbm_modelname+'_wblocks.csv', dtype={'wid':str})
    
    gammas = job_blocks[['jid']]
    iotas = worker_blocks[['wid']]
    for l in range(model.L):
        print(l)
        oldname = 'job_blocks_level_' + str(l)
        newname = 'gamma_level_' + str(l)
        gammas[newname] = job_blocks[oldname] + 1
        oldname = 'worker_blocks_level_' + str(l)
        newname = 'iota_level_' + str(l)
        w_min = w_min = worker_blocks[oldname].min()
        iotas[newname] = worker_blocks[oldname] + 1 - w_min
    
    balanced = balanced.merge(iotas,  how='left', on='wid', indicator=False, validate='m:1')
    balanced = balanced.merge(gammas, how='left', on='jid', indicator=False, validate='m:1')
    balanced.sort_values(by=['wid', 'year'], inplace=True, ascending=True)
    
    # Recode nonemployment to gamma=0, missings to iota/gamma=-1
    for l in range(model.L):
        gname = 'gamma_level_' + str(l)
        iname = 'iota_level_' + str(l)
        balanced.loc[(np.isnan(balanced[gname])) & (np.isnan(balanced['ln_real_hrly_wage_dec'])),gname] = 0 
        balanced.loc[(np.isnan(balanced[gname])),gname] = -1 
        balanced.loc[(np.isnan(balanced[iname])),iname] = -1 

    #Identify first occupation worker is ever observed in
    print('Identifying workers first cbo2002 and clas_cnae20')
    cbo2002_first = balanced.groupby('wid')['cbo2002'].first().reset_index().rename(columns={'cbo2002':'cbo2002_first'})
    balanced = balanced.merge(cbo2002_first, on='wid', how='left',validate='m:1')
    clas_cnae20_first = balanced.groupby('wid')['clas_cnae20'].first().reset_index().rename(columns={'clas_cnae20':'clas_cnae20_first'})
    balanced = balanced.merge(clas_cnae20_first, on='wid', how='left',validate='m:1')

    #######################
    # Create occ2 variables
    data_full['occ2_first']         = data_full.cbo2002_first.astype(str).str[:2].astype(int)
    data_full['occ2_first_recode']  = data_full.cbo2002_first.astype(str).str[:2].rank(method='dense').astype(int)

    #######################
    # Create occ4 variables
    data_full['occ4_first']         = data_full.cbo2002_first.astype(str).str[:4].astype(int)
    # Set occ4s that rarely occur to missing. The cutoff at 5000 is totally arbitrary
    data_full['occ4_first'].loc[data_full.groupby('occ4_first')['occ4_first'].transform('count') < 5000] = np.nan
    # Recode occ4s to go from 1 to I. 
    data_full['occ4_first_recode']  = data_full.occ4_first.rank(method='dense', na_option='keep')
    # Recode missings to -1
    data_full['occ4_first'].loc[np.isnan(data_full.occ4_first)] = -1
    data_full['occ4_first_recode'].loc[np.isnan(data_full.occ4_first_recode)] = -1
    # Convert from float to int (nans are automatically stored as floats so when we added nans it converted to floats)
    data_full['occ4_first']         = data_full['occ4_first'].astype(int)
    data_full['occ4_first_recode']  = data_full['occ4_first_recode'].astype(int)
        

    # Flag every job change (including the worker's first observation). We use jid_temp because the fact that NaN==NaN evaluates to False would cause us to flag every year of a multi-year spell of non-employment as a new job, rather than just the first.
    print('Flagging job changes')
    balanced['jid_temp'] = balanced['jid']
    balanced['jid_temp'].loc[balanced['jid'].isna()==True] = '0'
    balanced['c'] = ((balanced['wid'] != balanced['wid'].shift()) | (balanced['jid_temp'] != balanced['jid_temp'].shift())).astype(int)
    balanced.drop(columns='jid_temp')

    print('Creating masked wid and jid')
    balanced['wid_masked'] = balanced.groupby('wid').grouper.group_info[0]
    balanced['jid_masked'] = balanced.groupby('jid').grouper.group_info[0]
    print('Pickling the BALANCED dataframe')
    balanced.to_pickle('./Data/derived/panel_'+modelname+'.p')
    print('Exporting each level of the model to CSV')
    for l in range(model.L):
        print('Exporting level ', l)
        gname = 'gamma_level_' + str(l)
        iname = 'iota_level_' + str(l)
        # Export an uncompressed version of level 0; compress other levels
        if l==0:
            balanced.rename(columns={iname:'iota',gname:'gamma'}).to_csv('./Data/derived/earnings_panel/panel_'+modelname+'_level_'+str(l)+'.csv', index=False, columns = ['wid_masked', 'jid_masked', 'year', 'cbo2002', 'cbo2002_first', 'clas_cnae20', 'clas_cnae20_first', 'sector_IBGE', 'occ2Xmeso', 'codemun', 'code_meso', 'c', 'real_hrly_wage_dec', 'ln_real_hrly_wage_dec', 'yob', 'iota', 'gamma'])
        else:            
            balanced.rename(columns={iname:'iota',gname:'gamma'}).to_csv('./Data/derived/earnings_panel/panel_'+modelname+'_level_'+str(l)+'.csv.gz', index=False, compression='gzip', columns = ['wid_masked', 'jid_masked', 'year', 'cbo2002', 'cbo2002_first', 'clas_cnae20', 'clas_cnae20_first', 'sector_IBGE', 'occ2Xmeso', 'codemun', 'code_meso', 'c', 'real_hrly_wage_dec', 'ln_real_hrly_wage_dec', 'yob', 'iota', 'gamma'])



def do_everything(firstyear_panel, lastyear_panel, firstyear_sbm, lastyear_sbm, municipality_codes, modelname, vars, nrows=None, run_sbm=False, pull_raw_data=False, append_raw_data=False, run_create_earnings_panel=False):


    print('Run SBM: ', run_sbm)
    print('Pull raw data: ', pull_raw_data)
    print('Append raw data: ', append_raw_data)
    print('Run create_earnings_panel(): ', run_create_earnings_panel)
    #################################################################
    # Load and append raw data
    ################################
    #################################

    if pull_raw_data == True:
        for year in range(firstyear_panel,lastyear_panel+1):
            pull_one_year(year, vars, municipality_codes, savefile='../dump/raw_data_' + modelname + '_' + str(year) + '.p', nrows=nrows)
        
    if append_raw_data==True:
        for year in range(firstyear_panel,lastyear_panel+1):
            print(year)
            df = pickle.load( open('../dump/raw_data_' + modelname + '_' + str(year) + '.p', "rb" ) )
            if year>firstyear_panel:
                appended = df.append(appended, sort=True)
            else:
                appended = df
            del df
        appended.to_pickle('../dump/appended_'+modelname+'.p')
    else:
        appended = pd.read_pickle('../dump/appended_'+modelname+'.p')
       
    
    #################################################################
    # Run SBM
    #################################################################
    
    if run_sbm == True:
        # It's kinda inefficient to pickle the edgelist then load it from pickle but kept this for flexibility
        bipartite_edgelist = appended.loc[(appended['year']>=firstyear_sbm) & (appended['year']<=lastyear_sbm)][['wid','jid']].drop_duplicates(subset=['wid','jid'])
        jid_occ_cw = appended.loc[(appended['year']>=firstyear_sbm) & (appended['year']<=lastyear_sbm)][['jid','cbo2002']].drop_duplicates(subset=['jid','cbo2002'])
        pickle.dump( bipartite_edgelist,  open('./Data/bipartite_edgelist_'+modelname+'.p', "wb" ) )
        model = bisbm.bisbm()                                                                       
        model.create_graph(filename='./Data/bipartite_edgelist_'+modelname+'.p',min_workers_per_job=5)
        model.fit(n_init=1)
        # In theory it makes more sense to save these as pickles than as csvs but I keep getting an error loading the pickle and the csv works fine
        model.export_blocks(output='./Data/model_'+modelname+'_blocks.csv', joutput='./Data/model_'+modelname+'_jblocks.csv', woutput='./Data/model_'+modelname+'_wblocks.csv')
        pickle.dump( model, open('./Data/model_'+modelname+'.p', "wb" ) )
        
    
        
    #################################################################
    # Create earnings panel
    #################################################################
    
    if run_create_earnings_panel==True:
        create_earnings_panel(modelname, appended, firstyear_panel, lastyear_panel)



