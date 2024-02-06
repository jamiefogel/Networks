# -*- coding: utf-8 -*-
"""
Created on Sun Aug  6 15:26:05 2023

@author: p13861161
"""

import pickle
import pandas as pd
import numpy as np
import bisbm


def create_earnings_panel(modelname, appended, firstyear_panel, lastyear_panel, sbm_modelname=None):
    if sbm_modelname==None:
        sbm_modelname=modelname
    # Because dates aren't stored correctly in some years. Also we had a very small number of invalid dates (5 out of hundreds of millions) and this sets them to missing rather than failing.
    appended['data_deslig'] = pd.to_datetime(appended['data_deslig'], errors='coerce')
    appended['data_adm'] = pd.to_datetime(appended['data_adm'], errors='coerce')
    
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

    # Create occ2Xmeso by first merging on code_meso and then creating the variable
    region_codes = pd.read_csv('Data/raw/munic_microregion_rm.csv', encoding='latin1')
    muni_meso_cw = pd.DataFrame({'code_meso':region_codes.code_meso,'code_uf':region_codes.code_uf,'codemun':region_codes.code_munic//10})
    balanced = balanced.merge(muni_meso_cw, how='left', on='codemun', copy=False) # validate='m:1', indicator=True)
    balanced['occ2Xmeso'] = balanced.cbo2002.str[0:2] + '_' + balanced['code_meso'].astype('str')
    
    balanced['occ2'] = balanced.cbo2002.str[:2]
    balanced['occ4'] = balanced.cbo2002.str[:4]
    balanced['code_uf_cat'] = balanced['code_uf'].astype('category').cat.codes
    balanced['sector_IBGE_state'] = np.where(balanced['code_uf'].isna()  | balanced['sector_IBGE'].isna(), np.nan, balanced['sector_IBGE'] + balanced['code_uf_cat'] * 15)
    #Identify first occupation/industry/occ2Xmeso worker is ever observed in
    print('Identifying workers first cbo2002 and clas_cnae20')
    occ_recode_cw = {}
    for var in ['cbo2002','occ2','occ4','clas_cnae20','occ2Xmeso', 'gamma_level_0'] :
        first = balanced.groupby('wid')[var].first().reset_index().rename(columns={var:var+'_first'})
        balanced = balanced.merge(first, on='wid', how='left',validate='m:1')
        # Set groups that rarely occur to missing. The cutoff at 500 is totally arbitrary
        balanced[var+'_first'].loc[balanced.groupby([var+'_first'])[var+'_first'].transform('count') < 500] = np.nan
        # Create a crosswalk between the original values and the recodes that go from 1 to N+1 and will be useful later. Do this for both _first and the original
        # The recodes go from 1 to N+1 because we want to reserve 0 to denote non-employment. This is consistent with how we code iota and gamma.
        # original
        recode, original = pd.factorize(balanced[var])
        occ_recode_cw[var] = pd.DataFrame({'recode':range(1,len(original)+1), 'original':original})
        balanced[var+'_recode']  = recode+1
        del recode, original
        # _first
        recode, original = pd.factorize(balanced[var+'_first'])
        occ_recode_cw[var+'_first'] = pd.DataFrame({'recode':range(1,len(original)+1), 'original':original})
        balanced[var+'_first_recode']  = recode+1
        del recode, original
        # Recode missings to -1. I think these correspond to people not currently employed who have been added to create a balanced panel.
        balanced[var                ].loc[balanced[var                ].isna()] = -1
        balanced[var+'_recode'      ].loc[balanced[var+'_recode'      ].isna()] = -1
        balanced[var+'_first'       ].loc[balanced[var+'_first'       ].isna()] = -1
        balanced[var+'_first_recode'].loc[balanced[var+'_first_recode'].isna()] = -1

        
    pickle.dump(occ_recode_cw, open('./Data/derived/occ_recode_cw_'+modelname+'.p', 'wb') )

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
    export_columns = ['wid_masked', 'jid_masked', 'year', 'sector_IBGE', 'codemun', 'code_uf', 'code_meso', 'c', 'real_hrly_wage_dec', 'ln_real_hrly_wage_dec', 'yob', 'iota', 'gamma', 'cbo2002', 'cbo2002_first', 'cbo2002_recode', 'cbo2002_first_recode', 'clas_cnae20', 'clas_cnae20_first', 'clas_cnae20_recode', 'clas_cnae20_first_recode', 'occ2', 'occ2_first', 'occ2_recode', 'occ2_first_recode', 'occ4', 'occ4_first', 'occ4_recode', 'occ4_first_recode', 'occ2Xmeso', 'occ2Xmeso_first', 'occ2Xmeso_recode', 'occ2Xmeso_first_recode', 'gamma_level_0_first','cnpj_raiz','grau_instr','id_estab']
    for l in range(model.L):
        print('Exporting level ', l)
        gname = 'gamma_level_' + str(l)
        iname = 'iota_level_' + str(l)
        # Export an uncompressed version of level 0; compress other levels
        if l==0:
            balanced.rename(columns={iname:'iota',gname:'gamma'}).to_csv('./Data/derived/earnings_panel/panel_'+modelname+'_level_'+str(l)+'.csv', index=False, columns = export_columns)
        else:            
            balanced.rename(columns={iname:'iota',gname:'gamma'}).to_csv('./Data/derived/earnings_panel/panel_'+modelname+'_level_'+str(l)+'.csv.gz', index=False, compression='gzip', columns = export_columns)

